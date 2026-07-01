#!/usr/bin/env python3
"""
One-time backfill: Firestore -> Typesense.

Creates two collections (businesses, customer_businesses) and bulk-imports
existing documents into them. Run once before deploying the live sync
Cloud Functions. Safe to re-run: collections are recreated and re-imported.

Setup:
    pip install firebase-admin typesense tqdm
    # tqdm is optional (nicer progress bar); script falls back to plain text.
    # Download your service account JSON from Firebase Console:
    #   Project Settings -> Service accounts -> Generate new private key

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS=./serviceAccount.json
    export TYPESENSE_HOST=xxx.a1.typesense.net
    export TYPESENSE_API_KEY=<your ADMIN api key>     # admin key, not search-only

    # Full migration:
    python backfill_typesense.py

    # Small test run (default 25 docs per collection, into *_test collections):
    python backfill_typesense.py --test

    # Custom sample size:
    python backfill_typesense.py --test --limit 100

    # Test against the REAL collection names instead of *_test:
    python backfill_typesense.py --test --no-suffix
"""

import argparse
import os
import sys
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore
import typesense

# Optional progress bar -------------------------------------------------------
try:
    from tqdm import tqdm
    HAVE_TQDM = True
except ImportError:
    HAVE_TQDM = False

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
TYPESENSE_HOST = os.environ.get("TYPESENSE_HOST", "")
TYPESENSE_PORT = os.environ.get("TYPESENSE_PORT", "443")
TYPESENSE_PROTOCOL = os.environ.get("TYPESENSE_PROTOCOL", "https")
TYPESENSE_API_KEY = os.environ.get("TYPESENSE_API_KEY", "")
SERVICE_ACCOUNT = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "./serviceAccount.json")

BATCH_SIZE = 500          # docs per Typesense import call
TEST_DEFAULT_LIMIT = 25   # docs per collection in --test mode

# ----------------------------------------------------------------------------
# SCHEMAS  (the ".*" wildcard keeps any extra fields, indexing only declared ones)
# ----------------------------------------------------------------------------
def businesses_schema(name="businesses"):
    return {
        "name": name,
        "fields": [
            {"name": "business_name", "type": "string"},
            {"name": "address",       "type": "string", "optional": True},
            {"name": "street",        "type": "string", "optional": True},
            {"name": "city",          "type": "string", "facet": True, "optional": True},
            {"name": "postcode",      "type": "string", "facet": True, "optional": True},
            {"name": "ownerIds",      "type": "string[]", "facet": True},  # access filter
            {"name": "created_at",    "type": "int64"},
            {"name": ".*",            "type": "auto"},
        ],
        "default_sorting_field": "created_at",
    }


def customer_businesses_schema(name="customer_businesses"):
    return {
        "name": name,
        "fields": [
            {"name": "name",           "type": "string"},
            {"name": "email",          "type": "string", "optional": True},
            {"name": "phone",          "type": "string", "optional": True},
            {"name": "owner_id",       "type": "string", "facet": True},  # = customerId (uid)
            {"name": "business_id",    "type": "string", "facet": True, "optional": True},
            {"name": "campaign_count", "type": "int64",  "optional": True},
            {"name": "hit_count",      "type": "int64",  "optional": True},
            {"name": "updated_at",     "type": "int64"},
            {"name": ".*",             "type": "auto"},
        ],
        "default_sorting_field": "updated_at",
    }


# ----------------------------------------------------------------------------
# PROGRESS
# ----------------------------------------------------------------------------
class Progress:
    """Thin wrapper: tqdm bar if available, else periodic plain-text updates."""
    def __init__(self, label, total=None):
        self.label = label
        self.total = total
        self.n = 0
        # tqdm raises on bool(bar) when total is None; use plain text for unknown totals.
        if HAVE_TQDM and total is not None:
            self.bar = tqdm(total=total, desc=label, unit="doc")
        else:
            self.bar = None
            tail = f"/{total}" if total else ""
            print(f"  {label}: starting{(' (' + str(total) + ' docs)') if total else ''}")
            self._tail = tail

    def update(self, k=1):
        self.n += k
        if self.bar is not None:
            self.bar.update(k)
        elif self.n % 200 == 0 or (self.total and self.n == self.total):
            print(f"  {self.label}: {self.n}{self._tail}")

    def close(self):
        if self.bar is not None:
            self.bar.close()


# ----------------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------------
def to_epoch_ms(value):
    if value is None:
        return None
    if isinstance(value, datetime) or hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return None
    return None


def as_string(value) -> str:
    """Coerce Firestore null/missing/non-string values to a Typesense string."""
    return str(value) if value is not None else ""


def normalize_customer_business_strings(doc: dict, data: dict) -> None:
    """Ensure string schema fields are valid for Typesense import."""
    doc["name"] = as_string(data.get("name"))
    for field in ("email", "phone"):
        raw = data.get(field)
        if raw is None:
            doc.pop(field, None)
        else:
            doc[field] = str(raw)


def clean(doc: dict) -> dict:
    """Strip Firestore types Typesense can't store; refs -> <key>_id, ts -> epoch ms."""
    out = {}
    for k, v in doc.items():
        if hasattr(v, "id") and hasattr(v, "path") and not isinstance(v, (str, bytes)):
            out[f"{k}_id"] = v.id
            continue
        if isinstance(v, datetime) or hasattr(v, "timestamp"):
            ms = to_epoch_ms(v)
            if ms is not None:
                out[k] = ms
            continue
        out[k] = v
    return out


def import_batch(client, collection: str, docs: list):
    if not docs:
        return 0, 0
    results = client.collections[collection].documents.import_(docs, {"action": "upsert"})
    failures = [r for r in results if not r.get("success", False)]
    if failures:
        print(f"\n  ! {len(failures)} failures in {collection}. First: {failures[0]}")
    return len(docs) - len(failures), len(failures)


def recreate_collection(client, schema: dict):
    name = schema["name"]
    try:
        client.collections[name].delete()
        print(f"  dropped existing '{name}'")
    except typesense.exceptions.ObjectNotFound:
        pass
    client.collections.create(schema)
    print(f"  created '{name}'")


def count_collection(db, ref):
    """Best-effort count for the progress total. Uses Firestore aggregation count()."""
    try:
        return ref.count().get()[0][0].value
    except Exception:
        return None  # count() unsupported / failed -> bar runs without a total


# ----------------------------------------------------------------------------
# MIGRATIONS
# ----------------------------------------------------------------------------
def migrate_businesses(db, ts, target_name, limit=None):
    print(f"\nMigrating 'businesses' -> '{target_name}' ...")
    ref = db.collection("businesses")
    total = limit if limit else count_collection(db, ref)
    prog = Progress("businesses", total)

    query = ref.limit(limit) if limit else ref
    batch, ok, fail = [], 0, 0
    for snap in query.stream():
        data = snap.to_dict() or {}
        doc = clean(data)
        doc["id"] = data.get("business_id") or snap.id
        doc["business_name"] = as_string(data.get("business_name"))
        doc["ownerIds"] = data.get("ownerIds", [])
        doc["created_at"] = to_epoch_ms(data.get("created_at")) or 0
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            o, f = import_batch(ts, target_name, batch); ok += o; fail += f
            prog.update(len(batch)); batch = []
    if batch:
        o, f = import_batch(ts, target_name, batch); ok += o; fail += f
        prog.update(len(batch))
    prog.close()
    print(f"  done: {ok} imported, {fail} failed")


def migrate_customer_businesses(db, ts, target_name, limit=None):
    print(f"\nMigrating 'customer_businesses' -> '{target_name}' ...")
    prog = Progress("customer_businesses", limit)  # no cheap count for collection_group

    batch, ok, fail, seen = [], 0, 0, 0
    for snap in db.collection_group("businesses").stream():
        parent = snap.reference.parent.parent          # customers/{uid}
        if parent is None or parent.parent.id != "customers":
            continue
        data = snap.to_dict() or {}
        doc = clean(data)
        doc["id"] = f"{parent.id}__{snap.id}"
        doc["owner_id"] = parent.id
        normalize_customer_business_strings(doc, data)
        doc["updated_at"] = to_epoch_ms(data.get("updated_at")) or 0
        if "business_ref_id" in doc and "business_id" not in doc:
            doc["business_id"] = doc.pop("business_ref_id")
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            o, f = import_batch(ts, target_name, batch); ok += o; fail += f
            prog.update(len(batch)); batch = []
        seen += 1
        if limit and seen >= limit:
            break
    if batch:
        o, f = import_batch(ts, target_name, batch); ok += o; fail += f
        prog.update(len(batch))
    prog.close()
    print(f"  done: {ok} imported, {fail} failed")


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Backfill Firestore -> Typesense")
    ap.add_argument("--test", action="store_true",
                    help="small sample run (default 25 docs/collection)")
    ap.add_argument("--limit", type=int, default=None,
                    help="docs per collection (implies a capped run)")
    ap.add_argument("--no-suffix", action="store_true",
                    help="in test mode, use real collection names instead of *_test")
    args = ap.parse_args()

    limit = None
    suffix = ""
    if args.test:
        limit = args.limit if args.limit else TEST_DEFAULT_LIMIT
        suffix = "" if args.no_suffix else "_test"
    elif args.limit:
        limit = args.limit  # capped run against real collections

    if not TYPESENSE_HOST or not TYPESENSE_API_KEY:
        sys.exit("Set TYPESENSE_HOST and TYPESENSE_API_KEY (admin key) env vars.")
    if not os.path.exists(SERVICE_ACCOUNT):
        sys.exit(f"Service account JSON not found at {SERVICE_ACCOUNT}.")

    mode = f"TEST (limit {limit}/collection)" if limit else "FULL"
    print(f"Mode: {mode}")
    if suffix:
        print(f"Target collections: businesses{suffix}, customer_businesses{suffix}")

    firebase_admin.initialize_app(credentials.Certificate(SERVICE_ACCOUNT))
    db = firestore.client()

    ts = typesense.Client({
        "nodes": [{"host": TYPESENSE_HOST, "port": TYPESENSE_PORT,
                   "protocol": TYPESENSE_PROTOCOL}],
        "api_key": TYPESENSE_API_KEY,
        "connection_timeout_seconds": 10,
    })

    b_name = f"businesses{suffix}"
    c_name = f"customer_businesses{suffix}"

    print("\nCreating collections ...")
    recreate_collection(ts, businesses_schema(b_name))
    recreate_collection(ts, customer_businesses_schema(c_name))

    migrate_businesses(db, ts, b_name, limit=limit)
    migrate_customer_businesses(db, ts, c_name, limit=limit)

    print("\nBackfill complete.")
    if suffix:
        print("Test collections created with the '_test' suffix. "
              "Inspect them in the Typesense dashboard, then run without --test "
              "for the real migration.")


if __name__ == "__main__":
    main()