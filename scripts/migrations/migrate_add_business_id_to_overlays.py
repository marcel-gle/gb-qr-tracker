#!/usr/bin/env python3
"""
Migration script to ensure every customer business overlay has a business_id field.

Target documents:
  /customers/{customerId}/businesses/{businessId}

For each overlay document where business_id is missing, this script sets:
  business_id = <document ID>

It deliberately ignores:
  - Top-level /businesses/{businessId} documents
  - Any other collections named "businesses" that do not match the overlay path pattern

Usage examples:

  # Dry-run on dev default database (just report stats)
  python migrate_add_business_id_to_overlays.py --dry-run --env dev

  # Dry-run on dev "test" database
  python migrate_add_business_id_to_overlays.py --dry-run --env dev --database test

  # Run for real on dev test database
  python migrate_add_business_id_to_overlays.py --env dev --database test

  # Only scan overlays for a single owner
  python migrate_add_business_id_to_overlays.py --dry-run --env dev --database test --owner-id xLRk37rnV7T4CbOXzW5N3saxVfy1
"""

import sys
import argparse
from typing import Dict, Optional

from google.cloud import firestore
from tqdm import tqdm

# Default configuration (keep in sync with other migration scripts)
DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"

# Batch size for Firestore operations (max 500 per batch, use 450 for safety)
BATCH_SIZE = 450


def get_project_for_env(env: str) -> str:
    """Map environment name to GCP project ID."""
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    raise ValueError(f"Unknown env: {env!r}")


def scan_and_fix_overlays(
    db: firestore.Client,
    dry_run: bool,
    owner_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict:
    """
    Scan all /customers/{uid}/businesses/{businessId} overlays and add missing
    business_id fields by setting them to the document ID.

    Returns stats dict.
    """
    stats: Dict[str, int] = {
        "total_businesses_collection_docs": 0,  # all docs seen in collection_group('businesses')
        "total_overlays_considered": 0,         # only /customers/{uid}/businesses/{businessId}
        "overlays_for_owner_filtered_out": 0,   # overlays skipped because of owner-id filter
        "overlays_with_business_id": 0,         # already have business_id (any value)
        "overlays_missing_business_id": 0,      # missing or None
        "overlays_updated": 0,                  # actually updated (non-dry-run)
        "overlays_would_update": 0,             # dry-run count
        "overlays_mismatched_business_id": 0,   # field present but != doc.id
        "errors": 0,
    }

    # collection_group('businesses') returns ALL collections named "businesses",
    # including the top-level /businesses collection. We filter to the overlay
    # path shape: customers/{uid}/businesses/{businessId}
    query = db.collection_group("businesses")
    if limit:
        query = query.limit(limit)

    docs_iter = query.stream()

    # Prepare batching
    batch = db.batch()
    ops_count = 0

    print("Scanning all collections named 'businesses' (collection group)...")

    for doc in tqdm(docs_iter, desc="Scanning overlays"):
        stats["total_businesses_collection_docs"] += 1

        ref = doc.reference
        path_segments = ref.path.split("/")

        # Only consider paths of the form: customers/{uid}/businesses/{businessId}
        if len(path_segments) != 4:
            # e.g. top-level /businesses/{id} or deeper nests we don't care about
            continue
        if path_segments[0] != "customers" or path_segments[2] != "businesses":
            continue

        owner_uid = path_segments[1]
        business_doc_id = path_segments[3]

        stats["total_overlays_considered"] += 1

        # Apply optional owner filter
        if owner_id and owner_uid != owner_id:
            stats["overlays_for_owner_filtered_out"] += 1
            continue

        try:
            data = doc.to_dict() or {}
            existing_business_id = data.get("business_id")

            if existing_business_id is not None:
                stats["overlays_with_business_id"] += 1
                if existing_business_id != business_doc_id:
                    # Mismatch – we don't change it automatically, just record it
                    stats["overlays_mismatched_business_id"] += 1
                continue

            # Missing business_id => we will set it to the document ID
            stats["overlays_missing_business_id"] += 1

            if dry_run:
                stats["overlays_would_update"] += 1
                continue

            # Real write
            batch.update(ref, {"business_id": business_doc_id})
            ops_count += 1
            stats["overlays_updated"] += 1

            if ops_count >= BATCH_SIZE:
                batch.commit()
                batch = db.batch()
                ops_count = 0

        except Exception:
            # We keep the error count but don't stop the whole migration
            stats["errors"] += 1

    if not dry_run and ops_count > 0:
        batch.commit()

    return stats


def run_migration(
    env: str,
    project: Optional[str],
    database: str,
    dry_run: bool,
    owner_id: Optional[str],
    limit: Optional[int],
) -> None:
    """Entry point to run the migration."""
    project_id = project or get_project_for_env(env)

    print("=" * 60)
    print("Add business_id to customer business overlays")
    print("=" * 60)
    print(f"Project:      {project_id}")
    print(f"Database:     {database}")
    print(f"Dry run:      {dry_run}")
    if owner_id:
        print(f"Owner filter: {owner_id}")
    if limit:
        print(f"Limit:        {limit}")
    print()

    db = firestore.Client(project=project_id, database=database)

    stats = scan_and_fix_overlays(
        db=db,
        dry_run=dry_run,
        owner_id=owner_id,
        limit=limit,
    )

    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"Total docs in collection_group('businesses'): {stats['total_businesses_collection_docs']}")
    print(f"Total overlays considered:                  {stats['total_overlays_considered']}")
    print(f"Overlays filtered out by owner-id:          {stats['overlays_for_owner_filtered_out']}")
    print(f"Overlays with existing business_id:         {stats['overlays_with_business_id']}")
    print(f"Overlays missing business_id:               {stats['overlays_missing_business_id']}")
    print(f"Overlays with mismatched business_id:       {stats['overlays_mismatched_business_id']}")
    if dry_run:
        print(f"Overlays that would be updated:             {stats['overlays_would_update']}")
    else:
        print(f"Overlays actually updated:                  {stats['overlays_updated']}")
    print(f"Errors encountered:                         {stats['errors']}")

    if dry_run:
        print("\n[DRY-RUN] No changes were written to the database.")
    else:
        print("\nMigration completed!")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ensure every customer business overlay has a business_id field."
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment (default: dev). Ignored if --project is set.",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="GCP Project ID (overrides --env defaults).",
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
        help="Preview changes without writing to the database.",
    )
    parser.add_argument(
        "--owner-id",
        type=str,
        default=None,
        help="Only process overlays for a specific customer UID.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit total documents scanned in collection_group('businesses') (for testing).",
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
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"\n[FATAL] Migration failed: {exc}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

