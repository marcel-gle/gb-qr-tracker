#!/usr/bin/env python3
"""
Simple script to sync business_id field with document ID in business documents.

This script ensures that the business_id field in business documents
matches the document ID (which should always be the case).

Usage:
    python sync_business_id_field.py [--dry-run] [--project PROJECT_ID] [--database DATABASE_ID]
"""

import os
import sys
import argparse
from typing import Dict
from google.cloud import firestore
from tqdm import tqdm

# Default configuration
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DEFAULT_DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")

# Batch sizes
BATCH_SIZE = 400  # Firestore batch limit is 500, leave some headroom


def sync_business_id_fields(
    db: firestore.Client,
    dry_run: bool = False
) -> Dict:
    """
    Sync business_id field with document ID for all business documents.
    Returns statistics.
    """
    print(f"Syncing business_id fields (dry_run={dry_run})...")
    
    businesses_ref = db.collection("businesses")
    businesses = list(businesses_ref.stream())
    
    stats = {
        "total": len(businesses),
        "updated": 0,
        "already_correct": 0,
        "missing_field": 0,
        "errors": []
    }
    
    batch = db.batch()
    ops = 0
    
    for business_doc in tqdm(businesses, desc="Processing businesses"):
        doc_id = business_doc.id
        data = business_doc.to_dict() or {}
        business_id_field = data.get("business_id")
        
        if business_id_field is None:
            # Field is missing, add it
            stats["missing_field"] += 1
            if not dry_run:
                batch.update(business_doc.reference, {"business_id": doc_id})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["updated"] += 1
        elif business_id_field != doc_id:
            # Field exists but doesn't match, update it
            if not dry_run:
                batch.update(business_doc.reference, {"business_id": doc_id})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["updated"] += 1
        else:
            # Field exists and matches, no update needed
            stats["already_correct"] += 1
    
    # Commit remaining batch operations
    if not dry_run and ops > 0:
        batch.commit()
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Sync business_id field with document ID in business documents"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to database"
    )
    parser.add_argument(
        "--project",
        type=str,
        default=DEFAULT_PROJECT_ID,
        help=f"GCP Project ID (default: {DEFAULT_PROJECT_ID})"
    )
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE_ID,
        help=f"Firestore Database ID (default: {DEFAULT_DATABASE_ID})"
    )

    args = parser.parse_args()

    print(f"Project: {args.project}")
    print(f"Database: {args.database}")
    print(f"Dry run: {args.dry_run}")

    # Initialize Firestore client
    db = firestore.Client(project=args.project, database=args.database)

    # Run sync
    stats = sync_business_id_fields(db, dry_run=args.dry_run)

    # Print summary
    print("\n" + "=" * 60)
    print("Sync Summary")
    print("=" * 60)
    print(f"Total businesses processed: {stats['total']}")
    print(f"Businesses updated: {stats['updated']}")
    print(f"Businesses already correct: {stats['already_correct']}")
    print(f"Businesses missing field: {stats['missing_field']}")
    
    if stats["errors"]:
        print(f"\nErrors ({len(stats['errors'])}):")
        for error in stats["errors"][:20]:  # Show first 20 errors
            print(f"  - {error}")
        if len(stats["errors"]) > 20:
            print(f"  ... and {len(stats['errors']) - 20} more errors")

    if args.dry_run:
        print("\n[DRY RUN] No changes were written to the database")
    else:
        print("\nSync completed!")

    return 0 if len(stats["errors"]) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

