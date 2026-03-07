#!/usr/bin/env python3
"""
Scan all link documents and list unique (owner_id, template_id) combinations.

Usage:
    python list_templates_by_owner.py [--env dev|prod] [--limit N]
"""

import argparse
import sys
from collections import defaultdict
from typing import Dict, Tuple, Set, Optional

import firebase_admin
from firebase_admin import credentials, firestore

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm is optional
    def tqdm(x, **kwargs):
        return x


def initialize_firestore(env: str = "dev") -> firestore.Client:
    """Initialize Firestore client, mirroring audit_business_owners.py."""
    if not firebase_admin._apps:
        if env == "prod":
            service_account_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"
        else:
            service_account_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"

        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def scan_links(
    db: firestore.Client,
    limit: Optional[int] = None,
) -> Tuple[int, Dict[Tuple[str, str], int]]:
    """
    Scan the links collection and aggregate unique (owner_id, template_id) pairs.

    Returns:
        total_scanned: number of link documents scanned
        combo_counts: dict mapping (owner_id, template_id) -> link count
    """
    links_ref = db.collection("links")

    page_size = 1000
    last_doc = None
    total_scanned = 0
    combo_counts: Dict[Tuple[str, str], int] = defaultdict(int)

    while True:
        query = links_ref.limit(page_size)
        if last_doc is not None:
            query = query.start_after(last_doc)

        batch = list(query.stream())
        if not batch:
            break

        for doc in tqdm(batch, desc="Scanning links", unit="link", leave=False):
            if limit is not None and total_scanned >= limit:
                return total_scanned, combo_counts

            total_scanned += 1

            try:
                data = doc.to_dict() or {}
            except Exception as e:
                print(f"[warn] Failed to read document {doc.id}: {e}", file=sys.stderr)
                continue

            owner_id = data.get("owner_id")
            template_id = data.get("template_id")

            if not owner_id or not template_id:
                continue

            key = (str(owner_id), str(template_id))
            combo_counts[key] += 1

        if len(batch) < page_size:
            break

        last_doc = batch[-1]

    return total_scanned, combo_counts


def print_report(total_scanned: int, combo_counts: Dict[Tuple[str, str], int]) -> None:
    """Print a sorted stdout report of unique (owner_id, template_id) combinations."""
    unique_combos = len(combo_counts)
    print()
    print("=" * 80)
    print("TEMPLATE / OWNER REPORT")
    print("=" * 80)
    print(f"Total links scanned: {total_scanned}")
    print(f"Unique (owner_id, template_id) combinations: {unique_combos}")
    print()

    if not combo_counts:
        print("No links with both owner_id and template_id found.")
        return

    print("owner_id,template_id,count")

    for (owner_id, template_id), count in sorted(
        combo_counts.items(), key=lambda x: (x[0][0], x[0][1])
    ):
        print(f"{owner_id},{template_id},{count}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List unique (template_id, owner_id) combinations from links collection.",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment to use (default: dev)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of link documents to scan (for testing).",
    )

    args = parser.parse_args()

    try:
        db = initialize_firestore(args.env)
    except Exception as e:
        print(f"[error] Failed to initialize Firestore: {e}", file=sys.stderr)
        return 1

    total_scanned, combo_counts = scan_links(db, limit=args.limit)
    print_report(total_scanned, combo_counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())

