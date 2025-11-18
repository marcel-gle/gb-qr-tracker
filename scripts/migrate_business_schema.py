#!/usr/bin/env python3
"""
Migration script to split business data into canonical and customer-specific overlays.

This script migrates existing business documents from:
  /businesses/{businessId} (all fields mixed)

To the new structure:
  /businesses/{businessId} (canonical fields only)
  /customers/{customerId}/businesses/{businessId} (customer-specific overlay)

Usage:
    python migrate_business_schema.py [--dry-run] [--project PROJECT_ID] [--database DATABASE_ID]
"""

import os
import sys
import argparse
from typing import Dict, List, Optional, Set
from datetime import datetime, timezone
from google.cloud import firestore
from google.cloud.firestore_v1 import ArrayUnion
from tqdm import tqdm

# Default configuration
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DEFAULT_DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")

# Canonical fields (shared across customers)
CANONICAL_FIELDS = {
    "business_name",
    "street",
    "house_number",
    "city",
    "postcode",
    "address",
    "coordinate",
    "created_at",
    "ownerIds",  # Keep for quick lookups
    "business_id",  # Keep for reference
}

# Customer-specific fields (per-customer overlay)
CUSTOMER_FIELDS = {
    "phone",
    "email",
    "name",
    "salutation",
    "hit_count",
    "last_hit_at",
    "updated_at",
}


def extract_canonical_payload(business_data: Dict) -> Dict:
    """Extract canonical fields from business document."""
    canonical = {}
    for field in CANONICAL_FIELDS:
        if field in business_data:
            canonical[field] = business_data[field]
    return canonical


def extract_customer_payload(business_data: Dict) -> Dict:
    """Extract customer-specific fields from business document."""
    customer = {}
    for field in CUSTOMER_FIELDS:
        if field in business_data:
            customer[field] = business_data[field]
    # Ensure hit_count defaults to 0 if missing
    if "hit_count" not in customer:
        customer["hit_count"] = 0
    # Ensure last_hit_at defaults to None if missing
    if "last_hit_at" not in customer:
        customer["last_hit_at"] = None
    return customer


def migrate_business(
    db: firestore.Client,
    business_id: str,
    business_data: Dict,
    dry_run: bool = False
) -> Dict:
    """
    Migrate a single business document.
    Returns statistics about the migration.
    """
    stats = {
        "canonical_created": False,
        "canonical_updated": False,
        "overlays_created": 0,
        "overlays_updated": 0,
        "errors": []
    }

    try:
        # Extract payloads
        canonical_payload = extract_canonical_payload(business_data)
        customer_payload = extract_customer_payload(business_data)

        # Get ownerIds (required for creating overlays)
        owner_ids = business_data.get("ownerIds", [])
        if not owner_ids:
            # If no ownerIds, we can't create overlays
            # This might be a data issue, but we'll still migrate canonical
            stats["errors"].append("No ownerIds found, skipping overlay creation")
            owner_ids = []
        
        # Note: hit_count and last_hit_at from the old document will be assigned
        # to the first owner only (since these are now per-customer fields)

        # Create/update canonical business document
        canonical_ref = db.collection("businesses").document(business_id)
        canonical_snap = canonical_ref.get()

        if not dry_run:
            if not canonical_snap.exists:
                # Create new canonical document (without customer fields)
                canonical_ref.set(canonical_payload)
                stats["canonical_created"] = True
            else:
                # Update existing canonical document (merge, removing customer fields)
                canonical_ref.set(canonical_payload, merge=True)
                stats["canonical_updated"] = True
        else:
            if not canonical_snap.exists:
                stats["canonical_created"] = True
            else:
                stats["canonical_updated"] = True

        # Create/update customer overlays for each owner
        for idx, owner_id in enumerate(owner_ids):
            if not owner_id:
                continue

            customer_business_ref = (
                db.collection("customers")
                .document(owner_id)
                .collection("businesses")
                .document(business_id)
            )

            # Prepare overlay payload with business_ref
            # For hit_count and last_hit_at: assign to first owner only
            overlay_payload = {
                "business_ref": canonical_ref,
                **customer_payload
            }
            
            # If this is not the first owner, reset hit_count and last_hit_at
            # (historical hits are assigned to the first owner)
            if idx > 0:
                overlay_payload["hit_count"] = 0
                overlay_payload["last_hit_at"] = None

            if not dry_run:
                customer_snap = customer_business_ref.get()
                if not customer_snap.exists:
                    customer_business_ref.set(overlay_payload)
                    stats["overlays_created"] += 1
                else:
                    customer_business_ref.set(overlay_payload, merge=True)
                    stats["overlays_updated"] += 1
            else:
                customer_snap = customer_business_ref.get()
                if not customer_snap.exists:
                    stats["overlays_created"] += 1
                else:
                    stats["overlays_updated"] += 1

    except Exception as e:
        stats["errors"].append(str(e))

    return stats


def migrate_all_businesses(
    db: firestore.Client,
    dry_run: bool = False,
    limit: Optional[int] = None
) -> Dict:
    """
    Migrate all business documents.
    Returns aggregate statistics.
    """
    print(f"Starting migration (dry_run={dry_run})...")
    
    businesses_ref = db.collection("businesses")
    
    # Get all business documents
    query = businesses_ref
    if limit:
        query = query.limit(limit)
    
    businesses = list(query.stream())
    total = len(businesses)
    
    print(f"Found {total} business documents to migrate")
    
    # Aggregate statistics
    aggregate_stats = {
        "total_businesses": total,
        "canonical_created": 0,
        "canonical_updated": 0,
        "overlays_created": 0,
        "overlays_updated": 0,
        "errors": [],
        "businesses_with_errors": 0,
    }

    # Process each business
    for business_doc in tqdm(businesses, desc="Migrating businesses"):
        business_id = business_doc.id
        business_data = business_doc.to_dict() or {}
        
        stats = migrate_business(db, business_id, business_data, dry_run)
        
        # Aggregate statistics
        aggregate_stats["canonical_created"] += 1 if stats["canonical_created"] else 0
        aggregate_stats["canonical_updated"] += 1 if stats["canonical_updated"] else 0
        aggregate_stats["overlays_created"] += stats["overlays_created"]
        aggregate_stats["overlays_updated"] += stats["overlays_updated"]
        
        if stats["errors"]:
            aggregate_stats["businesses_with_errors"] += 1
            aggregate_stats["errors"].extend([
                f"{business_id}: {err}" for err in stats["errors"]
            ])

    return aggregate_stats


def main():
    parser = argparse.ArgumentParser(
        description="Migrate business data to canonical + customer overlay structure"
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
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of businesses to migrate (for testing)"
    )

    args = parser.parse_args()

    print(f"Project: {args.project}")
    print(f"Database: {args.database}")
    print(f"Dry run: {args.dry_run}")
    if args.limit:
        print(f"Limit: {args.limit} businesses")

    # Initialize Firestore client
    db = firestore.Client(project=args.project, database=args.database)

    # Run migration
    stats = migrate_all_businesses(db, dry_run=args.dry_run, limit=args.limit)

    # Print summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"Total businesses processed: {stats['total_businesses']}")
    print(f"Canonical documents created: {stats['canonical_created']}")
    print(f"Canonical documents updated: {stats['canonical_updated']}")
    print(f"Customer overlays created: {stats['overlays_created']}")
    print(f"Customer overlays updated: {stats['overlays_updated']}")
    print(f"Businesses with errors: {stats['businesses_with_errors']}")
    
    if stats["errors"]:
        print(f"\nErrors ({len(stats['errors'])}):")
        for error in stats["errors"][:20]:  # Show first 20 errors
            print(f"  - {error}")
        if len(stats["errors"]) > 20:
            print(f"  ... and {len(stats['errors']) - 20} more errors")

    if args.dry_run:
        print("\n[DRY RUN] No changes were written to the database")
    else:
        print("\nMigration completed!")

    return 0 if stats["businesses_with_errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

