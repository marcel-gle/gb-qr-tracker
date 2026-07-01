#!/usr/bin/env python3
"""
Smoke-test Firestore -> Typesense live sync (dev *_test collections).

Writes test documents to Firestore (triggering the deployed Cloud Functions),
then polls Typesense until the expected create / update / delete outcomes appear.

SECURITY: Hardcoded to PROJECT_ID=gb-qr-tracker-dev.

Usage:
    export TYPESENSE_HOST=xxx.a1.typesense.net
    export TYPESENSE_API_KEY=<admin key>

    python scripts/general/test_typesense_sync.py
    python scripts/general/test_typesense_sync.py --owner-id Panugay5HYQ6WzyiBvUB5E3FSRB3
    python scripts/general/test_typesense_sync.py --only businesses
    python scripts/general/test_typesense_sync.py --cleanup-only

Environment:
    GOOGLE_APPLICATION_CREDENTIALS  Service account JSON (dev). Falls back to
                                      the same default path as create_demo_data.py.
    TYPESENSE_HOST                    Required for Typesense assertions.
    TYPESENSE_API_KEY                 Admin key (read access to *_test collections).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import firebase_admin
import typesense
from firebase_admin import credentials, firestore

# --- Dev-only guard (match other scripts) ---
PROJECT_ID = "gb-qr-tracker-dev"
DEFAULT_CREDENTIALS_PATH = (
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/"
    "gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
)
DEFAULT_OWNER_ID = "Panugay5HYQ6WzyiBvUB5E3FSRB3"

BIZ_DOC_ID = "_sync-test-001"
BIZ_LEAKY_ID = "_sync-test-no-owner"
CB_DOC_ID = "_sync-cb-001"

BUSINESSES_COLLECTION = "businesses_test"
CUSTOMER_BUSINESSES_COLLECTION = "customer_businesses_test"

_project_root = Path(__file__).resolve().parent.parent.parent

try:
    from dotenv import load_dotenv

    for env_file in (_project_root / ".env.dev", _project_root / ".env"):
        if env_file.exists():
            load_dotenv(env_file)
            break
except ImportError:
    pass


class TestFailure(Exception):
    pass


def init_firestore():
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        if os.path.exists(DEFAULT_CREDENTIALS_PATH):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = DEFAULT_CREDENTIALS_PATH
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path or not os.path.exists(cred_path):
        raise TestFailure(
            "Set GOOGLE_APPLICATION_CREDENTIALS to your dev service account JSON."
        )
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
    return firestore.client()


def init_typesense() -> typesense.Client:
    host = os.environ.get("TYPESENSE_HOST", "")
    key = os.environ.get("TYPESENSE_API_KEY", "")
    if not host or not key:
        raise TestFailure("Set TYPESENSE_HOST and TYPESENSE_API_KEY.")
    return typesense.Client(
        {
            "nodes": [
                {
                    "host": host,
                    "port": os.environ.get("TYPESENSE_PORT", "443"),
                    "protocol": os.environ.get("TYPESENSE_PROTOCOL", "https"),
                }
            ],
            "api_key": key,
            "connection_timeout_seconds": 10,
        }
    )


def ts_get(ts: typesense.Client, collection: str, doc_id: str) -> Optional[dict]:
    try:
        return ts.collections[collection].documents[doc_id].retrieve()
    except typesense.exceptions.ObjectNotFound:
        return None


def wait_for(
    fn: Callable[[], Optional[object]],
    *,
    timeout: float,
    interval: float,
    label: str,
) -> object:
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = fn()
        if result is not None:
            return result
        time.sleep(interval)
    raise TestFailure(f"Timed out after {timeout}s: {label}")


def wait_gone(
    ts: typesense.Client,
    collection: str,
    doc_id: str,
    *,
    timeout: float,
    interval: float,
    label: str,
) -> None:
    def _gone():
        if ts_get(ts, collection, doc_id) is None:
            return True
        return None

    wait_for(_gone, timeout=timeout, interval=interval, label=label)


def cleanup_firestore(db: firestore.Client, owner_id: str) -> None:
    db.collection("businesses").document(BIZ_DOC_ID).delete()
    db.collection("businesses").document(BIZ_LEAKY_ID).delete()
    (
        db.collection("customers")
        .document(owner_id)
        .collection("businesses")
        .document(CB_DOC_ID)
        .delete()
    )


def run_businesses_tests(
    db: firestore.Client,
    ts: typesense.Client,
    owner_id: str,
    *,
    poll_timeout: float,
    poll_interval: float,
) -> None:
    ref = db.collection("businesses").document(BIZ_DOC_ID)
    leaky_ref = db.collection("businesses").document(BIZ_LEAKY_ID)

    print("\n--- businesses: CREATE ---")
    ref.set(
        {
            "business_name": "Sync Test GmbH",
            "city": "Berlin",
            "street": "Teststraße 1",
            "postcode": "10115",
            "address": "Teststraße 1, 10115 Berlin",
            "business_id": BIZ_DOC_ID,
            "ownerIds": [owner_id],
            "created_at": firestore.SERVER_TIMESTAMP,
        }
    )
    doc = wait_for(
        lambda: ts_get(ts, BUSINESSES_COLLECTION, BIZ_DOC_ID),
        timeout=poll_timeout,
        interval=poll_interval,
        label=f"businesses create -> {BUSINESSES_COLLECTION}/{BIZ_DOC_ID}",
    )
    if doc.get("id") != BIZ_DOC_ID:
        raise TestFailure(f"businesses id mismatch: {doc.get('id')!r}")
    if doc.get("ownerIds") != [owner_id]:
        raise TestFailure(f"businesses ownerIds mismatch: {doc.get('ownerIds')!r}")
    created_at = doc.get("created_at")
    if not isinstance(created_at, int) or created_at <= 0:
        raise TestFailure(f"businesses created_at not epoch ms int: {created_at!r}")
    if doc.get("business_name") != "Sync Test GmbH":
        raise TestFailure("businesses business_name not copied through")
    print("  PASS")

    print("--- businesses: UPDATE ---")
    ref.update({"city": "Munich"})

    def _updated():
        d = ts_get(ts, BUSINESSES_COLLECTION, BIZ_DOC_ID)
        if d and d.get("city") == "Munich":
            return d
        return None

    wait_for(
        _updated,
        timeout=poll_timeout,
        interval=poll_interval,
        label="businesses update city=Munich",
    )
    print("  PASS")

    print("--- businesses: ACCESS GUARD (no ownerIds) ---")
    leaky_ref.set(
        {
            "business_name": "Leaky Business",
            "created_at": firestore.SERVER_TIMESTAMP,
        }
    )
    time.sleep(min(5.0, poll_timeout))
    if ts_get(ts, BUSINESSES_COLLECTION, BIZ_LEAKY_ID) is not None:
        raise TestFailure("leaky business without ownerIds was written to Typesense")
    leaky_ref.delete()
    print("  PASS")

    print("--- businesses: DELETE ---")
    ref.delete()
    wait_gone(
        ts,
        BUSINESSES_COLLECTION,
        BIZ_DOC_ID,
        timeout=poll_timeout,
        interval=poll_interval,
        label=f"businesses delete -> gone from {BUSINESSES_COLLECTION}",
    )
    print("  PASS")


def run_customer_businesses_tests(
    db: firestore.Client,
    ts: typesense.Client,
    owner_id: str,
    *,
    poll_timeout: float,
    poll_interval: float,
) -> None:
    ts_id = f"{owner_id}__{CB_DOC_ID}"
    biz_ref = db.collection("businesses").document(BIZ_DOC_ID)
    cb_ref = (
        db.collection("customers")
        .document(owner_id)
        .collection("businesses")
        .document(CB_DOC_ID)
    )

    print("\n--- customer_businesses: setup canonical business ---")
    biz_ref.set(
        {
            "business_name": "Sync Test GmbH",
            "business_id": BIZ_DOC_ID,
            "ownerIds": [owner_id],
            "created_at": firestore.SERVER_TIMESTAMP,
        }
    )
    wait_for(
        lambda: ts_get(ts, BUSINESSES_COLLECTION, BIZ_DOC_ID),
        timeout=poll_timeout,
        interval=poll_interval,
        label="canonical business for business_ref",
    )

    print("--- customer_businesses: CREATE ---")
    cb_ref.set(
        {
            "name": "Contact Sync Test",
            "email": "sync-test@example.com",
            "phone": "+49123456789",
            "business_ref": biz_ref,
            "updated_at": firestore.SERVER_TIMESTAMP,
            "campaign_count": 3,
            "hit_count": 7,
        }
    )
    doc = wait_for(
        lambda: ts_get(ts, CUSTOMER_BUSINESSES_COLLECTION, ts_id),
        timeout=poll_timeout,
        interval=poll_interval,
        label=f"customer_businesses create -> {CUSTOMER_BUSINESSES_COLLECTION}/{ts_id}",
    )
    if doc.get("id") != ts_id:
        raise TestFailure(f"customer_businesses id mismatch: {doc.get('id')!r}")
    if doc.get("owner_id") != owner_id:
        raise TestFailure(f"owner_id mismatch: {doc.get('owner_id')!r}")
    if doc.get("business_id") != BIZ_DOC_ID:
        raise TestFailure(f"business_id mismatch: {doc.get('business_id')!r}")
    if "business_ref" in doc:
        raise TestFailure("business_ref should not be stored in Typesense")
    if doc.get("updated_at") is None or not isinstance(doc.get("updated_at"), int):
        raise TestFailure(f"updated_at not epoch ms int: {doc.get('updated_at')!r}")
    if doc.get("campaign_count") != 3 or doc.get("hit_count") != 7:
        raise TestFailure("campaign_count / hit_count not synced as ints")
    print("  PASS")

    print("--- customer_businesses: UPDATE ---")
    cb_ref.update({"name": "Updated Contact"})

    def _updated():
        d = ts_get(ts, CUSTOMER_BUSINESSES_COLLECTION, ts_id)
        if d and d.get("name") == "Updated Contact":
            return d
        return None

    wait_for(
        _updated,
        timeout=poll_timeout,
        interval=poll_interval,
        label="customer_businesses update name",
    )
    print("  PASS")

    print("--- customer_businesses: DELETE ---")
    cb_ref.delete()
    wait_gone(
        ts,
        CUSTOMER_BUSINESSES_COLLECTION,
        ts_id,
        timeout=poll_timeout,
        interval=poll_interval,
        label=f"customer_businesses delete -> gone from {CUSTOMER_BUSINESSES_COLLECTION}",
    )
    biz_ref.delete()
    wait_gone(
        ts,
        BUSINESSES_COLLECTION,
        BIZ_DOC_ID,
        timeout=poll_timeout,
        interval=poll_interval,
        label="cleanup canonical business",
    )
    print("  PASS")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Smoke-test Typesense live sync (dev).")
    p.add_argument(
        "--owner-id",
        default=os.environ.get("OWNER_UID", DEFAULT_OWNER_ID),
        help="Firebase Auth uid for ownerIds / owner_id (default: demo owner)",
    )
    p.add_argument(
        "--poll-timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for each sync step (default: 30)",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between Typesense polls (default: 1)",
    )
    p.add_argument(
        "--only",
        choices=("businesses", "customer_businesses", "all"),
        default="all",
        help="Run a subset of tests (default: all)",
    )
    p.add_argument(
        "--cleanup-only",
        action="store_true",
        help="Delete test Firestore docs and exit (no assertions)",
    )
    p.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Leave test docs in Firestore on failure (default: always cleanup)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    print("=" * 60)
    print("TYPESENSE SYNC SMOKE TEST")
    print("=" * 60)
    print(f"Project:     {PROJECT_ID}")
    print(f"Owner uid:   {args.owner_id}")
    print(f"Collections: {BUSINESSES_COLLECTION}, {CUSTOMER_BUSINESSES_COLLECTION}")

    db = init_firestore()

    if args.cleanup_only:
        cleanup_firestore(db, args.owner_id)
        print("\nCleanup done.")
        return 0

    ts = init_typesense()
    try:
        if args.only in ("businesses", "all"):
            run_businesses_tests(
                db,
                ts,
                args.owner_id,
                poll_timeout=args.poll_timeout,
                poll_interval=args.poll_interval,
            )
        if args.only in ("customer_businesses", "all"):
            run_customer_businesses_tests(
                db,
                ts,
                args.owner_id,
                poll_timeout=args.poll_timeout,
                poll_interval=args.poll_interval,
            )
    except TestFailure as exc:
        print(f"\nFAIL: {exc}")
        if not args.no_cleanup:
            print("Cleaning up test Firestore docs...")
            cleanup_firestore(db, args.owner_id)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.")
        cleanup_firestore(db, args.owner_id)
        return 130
    else:
        cleanup_firestore(db, args.owner_id)
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED")
        print("=" * 60)
        return 0


if __name__ == "__main__":
    sys.exit(main())
