#!/usr/bin/env python3
"""
migrate_schema_v1.py

Backfill Firestore schema to the new ownership model and preview changes.

Does:
  1) links:      set owner_id from old 'customer' label (optionally delete 'customer')
  2) hits:       set owner_id from 'customer' OR (fallback) join links/{link_id}.owner_id
  3) businesses: ensure ownerIds (array of UIDs) derived from links {business_id, owner_id}
  4) customers:  ensure customers/{uid} skeleton docs (for any mapped/fallback UID)

Idempotent; dry-run by default (writes only when --commit is passed).

Examples:
  # Preview only (no writes), print 5 sample changes
  python migrate_schema_v1.py --project gb-qr-tracker --map data/migration_customers.json --preview 5

  # Commit writes (no legacy field deletion yet)
  python migrate_schema_v1.py --project gb-qr-tracker --map data/migration_customers.json --commit --preview 5

  # Commit + remove old 'customer' field
  python migrate_schema_v1.py --project gb-qr-tracker --map data/migration_customers.json --commit --delete-legacy

  # Single-customer shortcut (use this UID when mapping missing)
  python migrate_schema_v1.py --project gb-qr-tracker --fallback-uid 9xVVm8dhRhNmInut2bYcUfyQeTV2 --commit --preview 5
"""

import argparse
import json
import sys
from typing import Dict, Optional, Tuple, Iterable, Set, List

import firebase_admin
from firebase_admin import firestore, auth
from google.cloud.firestore import ArrayUnion, DELETE_FIELD


# ----------------------------
# Admin init and mapping
# ----------------------------

def init_admin(project: str):
    """Initialize Firebase Admin using ADC or GOOGLE_APPLICATION_CREDENTIALS."""
    if not firebase_admin._apps:
        firebase_admin.initialize_app(options={"projectId": project})
    return firestore.client()


def load_mapping(path: Optional[str]) -> Dict[str, str]:
    """Load JSON map: old_label -> uid_or_email"""
    if not path:
        print("[info] No --map file provided. If docs already have owner_id, mapping may be unnecessary.")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("--map must be a JSON object {label: uid_or_email}")
    return {str(k): str(v) for k, v in data.items()}


def resolve_uid(target: str) -> Optional[str]:
    """Resolve a target string as a UID (fast path) or fall back to email lookup."""
    try:
        u = auth.get_user(target)   # treat as uid
        return u.uid
    except Exception:
        pass
    try:
        u = auth.get_user_by_email(target)  # treat as email
        return u.uid
    except Exception:
        return None


def make_label_to_uid(map_json: Dict[str, str]) -> Tuple[Dict[str, str], Set[str]]:
    """Convert {label: uid_or_email} → {label: uid}."""
    resolved: Dict[str, str] = {}
    unresolved: Set[str] = set()
    for label, target in map_json.items():
        uid = resolve_uid(target)
        if uid:
            resolved[label] = uid
        else:
            unresolved.add(label)
    return resolved, unresolved


# ----------------------------
# Utilities
# ----------------------------

def batched(iterable: Iterable, size: int):
    """Yield lists of up to 'size' items (for batched writes)."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


# ----------------------------
# Counts (sanity checks)
# ----------------------------

def count_links_missing_owner(db) -> int:
    return sum(1 for d in db.collection("links").stream()
               if not (d.to_dict() or {}).get("owner_id"))


def count_hits_missing_owner(db) -> int:
    return sum(1 for d in db.collection("hits").stream()
               if not (d.to_dict() or {}).get("owner_id"))


def count_businesses_without_ownerIds(db) -> int:
    return sum(1 for d in db.collection("businesses").stream()
               if not isinstance((d.to_dict() or {}).get("ownerIds"), list))


# ----------------------------
# Previews (no writes)
# ----------------------------

def preview_links(db, label_to_uid: Dict[str, str], fallback_uid: Optional[str], limit: int = 5):
    print(f"\n[preview] links (up to {limit}) needing owner_id:")
    shown = 0
    for d in db.collection("links").stream():
        if shown >= limit:
            break
        data = d.to_dict() or {}
        if data.get("owner_id"):
            continue
        label = data.get("customer")
        uid = (label_to_uid.get(label) if label else None) or fallback_uid
        print(json.dumps({
            "id": d.id,
            "current": {
                "customer": label,
                "owner_id": data.get("owner_id"),
                "last_hit_at": str(data.get("last_hit_at")),
                "campaign": data.get("campaign")
            },
            "update":  {"owner_id": uid, "will_update": bool(uid)}
        }, ensure_ascii=False))
        shown += 1
    if shown == 0:
        print("  (none)")


def preview_hits(db, label_to_uid: Dict[str, str], fallback_uid: Optional[str], limit: int = 5):
    print(f"\n[preview] hits (up to {limit}) needing owner_id:")
    shown = 0
    for d in db.collection("hits").stream():
        if shown >= limit:
            break
        data = d.to_dict() or {}
        if data.get("owner_id"):
            continue
        label = data.get("customer")
        uid = None
        if label:
            uid = label_to_uid.get(label)
        if not uid:
            link_id = data.get("link_id")
            if link_id:
                link_snap = db.collection("links").document(link_id).get()
                link = link_snap.to_dict() or {}
                uid = link.get("owner_id")
        if not uid and fallback_uid:
            uid = fallback_uid
        print(json.dumps({
            "id": d.id,
            "current": {
                "customer": label,
                "owner_id": data.get("owner_id"),
                "ts": str(data.get("ts")),
                "link_id": data.get("link_id")
            },
            "update":  {"owner_id": uid, "will_update": bool(uid)}
        }, ensure_ascii=False))
        shown += 1
    if shown == 0:
        print("  (none)")


def preview_businesses_from_links(db, limit: int = 5):
    """
    Preview ownerIds that would be derived from links -> businesses.
    """
    print(f"\n[preview] businesses (up to {limit}) needing ownerIds from links:")
    # Build a quick map: biz -> set(owner_ids) from links
    biz_to_owners: Dict[str, Set[str]] = {}
    for ln in db.collection("links").stream():
        l = ln.to_dict() or {}
        biz = l.get("business_id")
        uid = l.get("owner_id")
        if biz and uid:
            biz_to_owners.setdefault(biz, set()).add(uid)

    shown = 0
    for biz_id, owners in biz_to_owners.items():
        if shown >= limit:
            break
        b_snap = db.collection("businesses").document(biz_id).get()
        b = b_snap.to_dict() or {}
        cur = b.get("ownerIds")
        if not isinstance(cur, list) or not owners.issubset(set(cur)):
            print(json.dumps({
                "id": biz_id,
                "name": b.get("business_name") or b.get("name"),
                "current": {"ownerIds": cur},
                "update":  {"add_ownerIds": sorted(list(owners))}
            }, ensure_ascii=False))
            shown += 1
    if shown == 0:
        print("  (none)")


# ----------------------------
# Backfill operations (writes)
# ----------------------------

def ensure_customer_doc(db, uid: str, dry_run: bool):
    ref = db.collection("customers").document(uid)
    if dry_run:
        return
    ref.set({
        "owner_id": uid,
        "updated_at": firestore.SERVER_TIMESTAMP,
        # Add backend-managed fields later if desired (plan, is_active, etc.)
    }, merge=True)


def ensure_customer_docs(db, uids: Iterable[str], dry_run: bool):
    for uid in set(uids):
        ensure_customer_doc(db, uid, dry_run)


def backfill_links(
    db,
    label_to_uid: Dict[str, str],
    dry_run: bool,
    delete_legacy: bool,
    fallback_uid: Optional[str]
) -> Tuple[int, int, int, List[str]]:
    """
    Returns: (updated, skipped_no_label, missing_mapping, sample_ids)
    """
    col = db.collection("links")
    docs = list(col.stream())

    updated = 0
    skipped_no_label = 0
    missing_mapping = 0
    samples: List[str] = []

    for chunk in batched(docs, 400):
        batch = db.batch() if not dry_run else None
        for d in chunk:
            data = d.to_dict() or {}
            if data.get("owner_id"):
                continue

            uid = None
            label = data.get("customer")
            if label:
                uid = label_to_uid.get(label)
            if not uid and fallback_uid:
                uid = fallback_uid

            if not uid:
                if not label:
                    skipped_no_label += 1
                else:
                    missing_mapping += 1
                continue

            if not dry_run:
                update = {"owner_id": uid}
                if delete_legacy and "customer" in data:
                    update["customer"] = DELETE_FIELD
                batch.update(d.reference, update)

            if len(samples) < 10:
                samples.append(d.id)
            updated += 1

        if not dry_run:
            batch.commit()

    return updated, skipped_no_label, missing_mapping, samples


def backfill_hits(
    db,
    label_to_uid: Dict[str, str],
    dry_run: bool,
    delete_legacy: bool,
    fallback_uid: Optional[str]
) -> Tuple[int, int, int, List[str]]:
    """
    Derive owner_id from:
      1) hits.customer via mapping
      2) links/{link_id}.owner_id
      3) --fallback-uid (optional)
    """
    col = db.collection("hits")
    docs = list(col.stream())

    updated = 0
    skipped_no_label = 0
    missing_mapping = 0
    samples: List[str] = []

    for chunk in batched(docs, 400):
        batch = db.batch() if not dry_run else None
        for d in chunk:
            data = d.to_dict() or {}
            if data.get("owner_id"):
                continue

            uid = None
            label = data.get("customer")
            if label:
                uid = label_to_uid.get(label)
            if not uid:
                link_id = data.get("link_id")
                if link_id:
                    link_snap = db.collection("links").document(link_id).get()
                    link = link_snap.to_dict() or {}
                    uid = link.get("owner_id")
            if not uid and fallback_uid:
                uid = fallback_uid

            if not uid:
                if not label:
                    skipped_no_label += 1
                else:
                    missing_mapping += 1
                continue

            if not dry_run:
                update = {"owner_id": uid}
                if delete_legacy and "customer" in data:
                    update["customer"] = DELETE_FIELD
                batch.update(d.reference, update)

            if len(samples) < 10:
                samples.append(d.id)
            updated += 1

        if not dry_run:
            batch.commit()

    return updated, skipped_no_label, missing_mapping, samples


def backfill_businesses_from_links(
    db,
    dry_run: bool
) -> Tuple[int, List[str]]:
    """
    Aggregate ownerIds onto businesses based on links {business_id, owner_id}.
    Returns (changed_count, sample_business_ids).
    """
    changed = 0
    samples: List[str] = []

    # Build map bizId -> set(owner_ids) from links
    biz_to_owners: Dict[str, Set[str]] = {}
    for d in db.collection("links").stream():
        data = d.to_dict() or {}
        biz = data.get("business_id")
        uid = data.get("owner_id")
        if biz and uid:
            biz_to_owners.setdefault(biz, set()).add(uid)

    # Apply to businesses using set(..., merge=True) so it works if doc is missing
    for chunk in batched(biz_to_owners.items(), 400):
        batch = db.batch() if not dry_run else None
        for biz_id, owners in chunk:
            if not owners:
                continue
            if not dry_run:
                batch.set(
                    db.collection("businesses").document(biz_id),
                    {
                        "ownerIds": ArrayUnion(list(owners)),
                        "updated_at": firestore.SERVER_TIMESTAMP
                    },
                    merge=True
                )
            if len(samples) < 10:
                samples.append(biz_id)
            changed += 1
        if not dry_run:
            batch.commit()

    return changed, samples


# ----------------------------
# Post-commit sample readouts
# ----------------------------

def print_docs(db, col: str, ids: List[str], fields: Optional[List[str]] = None, limit: int = 5):
    """Fetch and print a few updated docs for manual inspection."""
    print(f"\n[after-commit] sample {col} docs:")
    for doc_id in ids[:limit]:
        snap = db.collection(col).document(doc_id).get()
        data = snap.to_dict() or {}
        if fields:
            data = {k: data.get(k) for k in fields}
        print(json.dumps({"id": doc_id, **data}, ensure_ascii=False))


# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Firestore schema backfill to new ownership model")
    ap.add_argument("--project", required=True, help="GCP project ID, e.g. gb-qr-tracker")
    ap.add_argument("--map", help="JSON file mapping old customer label -> uid or email")
    ap.add_argument("--fallback-uid", help="UID to use when mapping/lookup missing (single-customer shortcut)")
    ap.add_argument("--commit", action="store_true", help="Perform writes (otherwise dry-run)")
    ap.add_argument("--delete-legacy", action="store_true", help="Delete old 'customer' field after setting owner_id")
    ap.add_argument("--skip-links", action="store_true", help="Skip links collection")
    ap.add_argument("--skip-hits", action="store_true", help="Skip hits collection")
    ap.add_argument("--skip-businesses", action="store_true", help="Skip businesses ownerIds backfill")
    ap.add_argument("--skip-customers", action="store_true", help="Skip creating customers/{uid} docs")
    ap.add_argument("--preview", type=int, default=5, help="Print up to N example changes per collection (0=none)")
    ap.add_argument("--verify-only", action="store_true", help="Do not backfill; only print counts and previews")
    args = ap.parse_args()

    DRY = (not args.commit)
    db = init_admin(args.project)

    mapping_raw = load_mapping(args.map)
    label_to_uid, unresolved = make_label_to_uid(mapping_raw)

    # Include fallback uid in the ensure step if provided
    ensure_uids = list(label_to_uid.values())
    if args.fallback_uid:
        ensure_uids.append(args.fallback_uid)

    if unresolved:
        print("\n[warn] Could not resolve these labels to UIDs (fix your --map or use --fallback-uid):")
        for l in unresolved:
            print("  -", l)
        if args.commit and not args.fallback_uid:
            print("[fatal] Commit mode requires all labels to resolve OR provide --fallback-uid. Exiting.")
            sys.exit(1)

    # Current counts
    print("\n[current counts]")
    print("  links missing owner_id:   ", count_links_missing_owner(db))
    print("  hits  missing owner_id:   ", count_hits_missing_owner(db))
    print("  businesses w/o ownerIds:  ", count_businesses_without_ownerIds(db))

    # Previews
    if args.preview > 0:
        preview_links(db, label_to_uid, args.fallback_uid, limit=args.preview)
        preview_hits(db, label_to_uid, args.fallback_uid, limit=args.preview)
        preview_businesses_from_links(db, limit=args.preview)

    if args.verify_only:
        print("\n[verify-only] No writes performed.")
        return

    # Backfills
    if not args.skip_customers and ensure_uids:
        print("\n[1/4] Ensuring customers/{uid} docs…")
        ensure_customer_docs(db, ensure_uids, dry_run=DRY)
        print("    done.")

    link_ids: List[str] = []
    if not args.skip_links:
        print("\n[2/4] Backfilling links.owner_id…")
        up, no_label, miss, link_ids = backfill_links(
            db, label_to_uid, dry_run=DRY, delete_legacy=args.delete_legacy, fallback_uid=args.fallback_uid
        )
        print(f"    updated={up} skipped_no_label={no_label} missing_mapping={miss}")

    hit_ids: List[str] = []
    if not args.skip_hits:
        print("\n[3/4] Backfilling hits.owner_id…")
        up, no_label, miss, hit_ids = backfill_hits(
            db, label_to_uid, dry_run=DRY, delete_legacy=args.delete_legacy, fallback_uid=args.fallback_uid
        )
        print(f"    updated={up} skipped_no_label={no_label} missing_mapping={miss}")

    biz_ids: List[str] = []
    if not args.skip_businesses:
        print("\n[4/4] Ensuring businesses.ownerIds from links…")
        changed, biz_ids = backfill_businesses_from_links(db, dry_run=DRY)
        print(f"    changed={changed}")

    # Post-run counts
    print("\n[post-run counts]")
    print("  links missing owner_id:   ", count_links_missing_owner(db))
    print("  hits  missing owner_id:   ", count_hits_missing_owner(db))
    print("  businesses w/o ownerIds:  ", count_businesses_without_ownerIds(db))

    # Show sample docs that were updated (only meaningful when committed)
    if not DRY and args.preview > 0:
        if link_ids:
            print_docs(db, "links", link_ids, fields=["owner_id", "campaign", "last_hit_at"])
        if hit_ids:
            print_docs(db, "hits", hit_ids, fields=["owner_id", "link_id", "ts"])
        if biz_ids:
            print_docs(db, "businesses", biz_ids, fields=["ownerIds", "business_name"])


if __name__ == "__main__":
    main()
