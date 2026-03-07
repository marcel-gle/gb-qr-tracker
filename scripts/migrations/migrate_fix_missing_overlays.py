#!/usr/bin/env python3
"""
Migration script to fix missing customer business overlay documents.

Detects businesses in /businesses/{businessId} whose ownerIds include a user
but the corresponding /customers/{uid}/businesses/{businessId} overlay document
does not exist, and creates the missing overlays.

Usage:
    # Dry-run on dev (preview only)
    python migrate_fix_missing_overlays.py --dry-run

    # Dry-run on prod
    python migrate_fix_missing_overlays.py --dry-run --project gb-qr-tracker

    # Run for real on dev
    python migrate_fix_missing_overlays.py

    # Run for a specific owner on dev
    python migrate_fix_missing_overlays.py --owner-id USER_UID --dry-run

    # Verify hit counts from the hits collection
    python migrate_fix_missing_overlays.py --verify-hits --dry-run

    # Limit to N businesses (for testing)
    python migrate_fix_missing_overlays.py --limit 10 --dry-run
"""

import sys
import argparse
from typing import Dict, List, Optional
from google.cloud import firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP
from tqdm import tqdm

# Default configuration
DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"

# Batch size for Firestore operations (max 500 per batch, use 450 for safety)
BATCH_SIZE = 450

# Fields to copy from canonical business into the overlay
OVERLAY_FIELDS_FROM_CANONICAL = {
    "phone",
    "email",
    "name",
    "salutation",
}


def get_project_for_env(env: str) -> str:
    """Map environment name to GCP project ID."""
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    raise ValueError(f"Unknown env: {env!r}")


def build_overlay_payload(
    db: firestore.Client,
    business_id: str,
    business_data: Dict,
    hit_count: int = 0,
    last_hit_at=None,
) -> Dict:
    """Build the overlay document payload from canonical business data."""
    canonical_ref = db.collection("businesses").document(business_id)

    payload = {
        "business_id": business_id,
        "business_ref": canonical_ref,
        "hit_count": hit_count,
        "last_hit_at": last_hit_at,
        "updated_at": SERVER_TIMESTAMP,
    }

    for field in OVERLAY_FIELDS_FROM_CANONICAL:
        payload[field] = business_data.get(field)

    return payload


def compute_hit_counts(
    db: firestore.Client,
    missing_pairs: List[Dict],
) -> Dict[str, Dict]:
    """
    Query the hits collection to compute actual hit_count and last_hit_at
    for each (owner_id, business_id) pair.

    Returns a dict keyed by "owner_id:business_id" with values
    {"hit_count": int, "last_hit_at": datetime|None}.
    """
    print("Verifying hit counts from the hits collection...")
    results = {}

    for pair in tqdm(missing_pairs, desc="Querying hits"):
        owner_id = pair["owner_id"]
        business_id = pair["business_id"]
        key = f"{owner_id}:{business_id}"

        biz_ref = db.collection("businesses").document(business_id)
        hits_query = (
            db.collection("hits")
            .where("owner_id", "==", owner_id)
            .where("business_ref", "==", biz_ref)
        )

        hits = list(hits_query.stream())
        count = len(hits)
        last_hit = None
        if hits:
            # Find the most recent hit timestamp
            for hit in hits:
                hit_data = hit.to_dict()
                ts = hit_data.get("ts")
                if ts and (last_hit is None or ts > last_hit):
                    last_hit = ts

        results[key] = {"hit_count": count, "last_hit_at": last_hit}

    return results


def scan_missing_overlays(
    db: firestore.Client,
    owner_id_filter: Optional[str] = None,
    limit: Optional[int] = None,
) -> tuple:
    """
    Scan all businesses and detect missing customer overlay documents.

    Returns:
        (missing_pairs, businesses_data, stats)
        - missing_pairs: list of {"owner_id": str, "business_id": str}
        - businesses_data: dict mapping business_id -> business document data
        - stats: scan statistics dict
    """
    stats = {
        "total_businesses": 0,
        "businesses_with_owners": 0,
        "total_owner_pairs": 0,
        "existing_overlays": 0,
        "missing_overlays": 0,
        "businesses_no_owners": 0,
        "errors": [],
    }

    # Stream all businesses
    query = db.collection("businesses")
    if limit:
        query = query.limit(limit)

    businesses = list(query.stream())
    stats["total_businesses"] = len(businesses)
    print(f"Found {len(businesses)} business documents")

    # Collect all overlay refs to batch-check and store business data
    overlay_refs_info = []  # list of (ref, owner_id, business_id)
    businesses_data = {}

    for biz_doc in tqdm(businesses, desc="Scanning businesses"):
        business_id = biz_doc.id
        business_data = biz_doc.to_dict() or {}
        businesses_data[business_id] = business_data

        owner_ids = business_data.get("ownerIds", [])
        if not owner_ids:
            stats["businesses_no_owners"] += 1
            continue

        stats["businesses_with_owners"] += 1

        for oid in owner_ids:
            if not oid:
                continue
            # If filtering by owner, skip non-matching owners
            if owner_id_filter and oid != owner_id_filter:
                continue

            stats["total_owner_pairs"] += 1
            ref = (
                db.collection("customers")
                .document(oid)
                .collection("businesses")
                .document(business_id)
            )
            overlay_refs_info.append((ref, oid, business_id))

    if not overlay_refs_info:
        print("No owner/business pairs to check.")
        return [], businesses_data, stats

    # Batch-check existence of overlay documents
    print(f"Checking {len(overlay_refs_info)} overlay documents...")
    overlay_existence = {}
    refs_only = [info[0] for info in overlay_refs_info]

    for i in range(0, len(refs_only), 500):
        batch_refs = refs_only[i : i + 500]
        snaps = db.get_all(batch_refs)
        for snap in snaps:
            overlay_existence[snap.reference.path] = snap.exists

    # Determine which are missing
    missing_pairs = []
    for ref, oid, biz_id in overlay_refs_info:
        exists = overlay_existence.get(ref.path, False)
        if exists:
            stats["existing_overlays"] += 1
        else:
            stats["missing_overlays"] += 1
            missing_pairs.append({"owner_id": oid, "business_id": biz_id})

    return missing_pairs, businesses_data, stats


def create_missing_overlays(
    db: firestore.Client,
    missing_pairs: List[Dict],
    businesses_data: Dict,
    hit_counts: Optional[Dict] = None,
    dry_run: bool = False,
) -> Dict:
    """
    Create the missing overlay documents.

    Returns write statistics.
    """
    write_stats = {
        "created": 0,
        "errors": [],
    }

    if not missing_pairs:
        return write_stats

    if dry_run:
        print(f"\n[DRY-RUN] Would create {len(missing_pairs)} overlay documents:")
        for pair in missing_pairs[:30]:
            oid = pair["owner_id"]
            biz_id = pair["business_id"]
            biz_data = businesses_data.get(biz_id, {})
            biz_name = biz_data.get("business_name") or biz_data.get("name") or "?"
            key = f"{oid}:{biz_id}"
            hc = hit_counts.get(key, {}).get("hit_count", 0) if hit_counts else 0
            print(
                f"  customers/{oid}/businesses/{biz_id}"
                f'  (business_name="{biz_name}", hit_count={hc})'
            )
        if len(missing_pairs) > 30:
            print(f"  ... and {len(missing_pairs) - 30} more")
        write_stats["created"] = len(missing_pairs)
        return write_stats

    # Batch-write missing overlays
    batch = db.batch()
    ops_count = 0

    for pair in tqdm(missing_pairs, desc="Creating overlays"):
        oid = pair["owner_id"]
        biz_id = pair["business_id"]
        biz_data = businesses_data.get(biz_id, {})

        # Determine hit_count and last_hit_at
        hc = 0
        lha = None
        if hit_counts:
            key = f"{oid}:{biz_id}"
            entry = hit_counts.get(key, {})
            hc = entry.get("hit_count", 0)
            lha = entry.get("last_hit_at")

        try:
            payload = build_overlay_payload(
                db, biz_id, biz_data, hit_count=hc, last_hit_at=lha
            )
            ref = (
                db.collection("customers")
                .document(oid)
                .collection("businesses")
                .document(biz_id)
            )
            batch.set(ref, payload)
            ops_count += 1
            write_stats["created"] += 1

            if ops_count >= BATCH_SIZE:
                try:
                    batch.commit()
                except Exception as e:
                    write_stats["errors"].append(f"Batch commit error: {e}")
                batch = db.batch()
                ops_count = 0

        except Exception as e:
            write_stats["errors"].append(
                f"Error preparing overlay for {oid}/{biz_id}: {e}"
            )

    # Commit remaining
    if ops_count > 0:
        try:
            batch.commit()
        except Exception as e:
            write_stats["errors"].append(f"Final batch commit error: {e}")

    return write_stats


def run_migration(
    env: str,
    project: Optional[str],
    database: str,
    dry_run: bool,
    owner_id: Optional[str] = None,
    limit: Optional[int] = None,
    verify_hits: bool = False,
):
    """Main migration orchestrator."""
    project_id = project or get_project_for_env(env)

    print("=" * 60)
    print("Fix Missing Business Overlays")
    print("=" * 60)
    print(f"Project:      {project_id}")
    print(f"Database:     {database}")
    print(f"Dry run:      {dry_run}")
    print(f"Verify hits:  {verify_hits}")
    if owner_id:
        print(f"Owner filter: {owner_id}")
    if limit:
        print(f"Limit:        {limit}")
    print()

    db = firestore.Client(project=project_id, database=database)

    # Phase 1: Scan and detect
    print("--- Phase 1: Scan & Detect ---")
    missing_pairs, businesses_data, scan_stats = scan_missing_overlays(
        db, owner_id_filter=owner_id, limit=limit
    )

    print(f"\nScan results:")
    print(f"  Total businesses:       {scan_stats['total_businesses']}")
    print(f"  With owners:            {scan_stats['businesses_with_owners']}")
    print(f"  Without owners:         {scan_stats['businesses_no_owners']}")
    print(f"  Owner/business pairs:   {scan_stats['total_owner_pairs']}")
    print(f"  Existing overlays:      {scan_stats['existing_overlays']}")
    print(f"  Missing overlays:       {scan_stats['missing_overlays']}")

    if not missing_pairs:
        print("\nNo missing overlays detected. Database is consistent.")
        return

    # Phase 2: Optionally verify hit counts
    hit_counts = None
    if verify_hits:
        print("\n--- Phase 2: Verify Hit Counts ---")
        hit_counts = compute_hit_counts(db, missing_pairs)
        non_zero = sum(1 for v in hit_counts.values() if v["hit_count"] > 0)
        print(f"  Pairs with hits > 0: {non_zero}")
        if non_zero > 0:
            print("  (These overlays should have been created by the redirector)")
            for key, val in hit_counts.items():
                if val["hit_count"] > 0:
                    print(
                        f"    {key}: hit_count={val['hit_count']}, "
                        f"last_hit_at={val['last_hit_at']}"
                    )

    # Phase 3: Create missing overlays
    print(f"\n--- Phase 3: Create Missing Overlays ---")
    write_stats = create_missing_overlays(
        db, missing_pairs, businesses_data, hit_counts=hit_counts, dry_run=dry_run
    )

    # Summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"Missing overlays detected: {scan_stats['missing_overlays']}")
    print(f"Overlays created:          {write_stats['created']}")

    all_errors = scan_stats["errors"] + write_stats["errors"]
    if all_errors:
        print(f"\nErrors ({len(all_errors)}):")
        for err in all_errors[:20]:
            print(f"  - {err}")
        if len(all_errors) > 20:
            print(f"  ... and {len(all_errors) - 20} more errors")

    if dry_run:
        print("\n[DRY-RUN] No changes were written to the database.")
    else:
        print("\nMigration completed!")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fix missing customer business overlay documents."
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="GCP Project ID (overrides --env)",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE_ID,
        help=f"Firestore Database ID (default: {DEFAULT_DATABASE_ID})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to the database",
    )
    parser.add_argument(
        "--owner-id",
        type=str,
        default=None,
        help="Only process businesses for a specific owner UID",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of businesses to scan (for testing)",
    )
    parser.add_argument(
        "--verify-hits",
        action="store_true",
        help="Cross-check hit counts from the hits collection (slower)",
    )

    args = parser.parse_args()

    try:
        run_migration(
            env=args.env,
            project=args.project,
            database=args.database,
            dry_run=args.dry_run,
            owner_id=args.owner_id,
            limit=args.limit,
            verify_hits=args.verify_hits,
        )
    except Exception as e:
        print(f"\n[FATAL] Migration failed: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
