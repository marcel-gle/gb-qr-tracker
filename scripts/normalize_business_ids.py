#!/usr/bin/env python3
"""
Migration script to normalize business IDs to lowercase and update all references.

OPTIMIZED VERSION: Pre-loads all data and uses lookup maps for fast processing.

Usage:
    python normalize_business_ids.py [--dry-run] [--project PROJECT_ID] [--database DATABASE_ID]
"""

import os
import sys
import argparse
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from datetime import datetime, timezone
from google.cloud import firestore
from google.cloud.firestore_v1 import ArrayUnion
from tqdm import tqdm
import re

# Default configuration
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DEFAULT_DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")

# Batch sizes
BATCH_SIZE = 400  # Firestore batch limit is 500, leave some headroom


def sanitize_id(value: str) -> str:
    """Normalize ID to lowercase, matching the upload_processor logic."""
    if value is None:
        return ""
    v = str(value).strip()
    v = re.sub(r"[^A-Za-z0-9]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    v = v.lower()
    return v


def normalize_business_id(business_id: str) -> str:
    """Normalize a business ID to lowercase."""
    return sanitize_id(business_id)


def find_non_normalized_businesses(db: firestore.Client) -> List[Tuple[str, str]]:
    """
    Find all businesses with non-normalized IDs.
    Returns list of (old_id, normalized_id) tuples.
    """
    businesses_ref = db.collection("businesses")
    businesses = list(businesses_ref.stream())
    
    non_normalized = []
    for business_doc in businesses:
        old_id = business_doc.id
        normalized_id = normalize_business_id(old_id)
        
        if old_id != normalized_id:
            non_normalized.append((old_id, normalized_id))
    
    return non_normalized


def preload_all_references(db: firestore.Client) -> Dict:
    """
    Pre-load all documents that reference businesses.
    Returns lookup maps for fast access.
    """
    print("Pre-loading all references...")
    
    # Maps: business_id -> list of document references
    links_by_business = defaultdict(list)
    hits_by_business = defaultdict(list)
    targets_by_business = defaultdict(list)
    overlays_by_business = defaultdict(list)  # (customer_id, business_id) -> doc_ref
    blacklist_by_business = defaultdict(list)  # (customer_id, business_id) -> doc_ref
    
    # Documents with business_id field that needs normalization
    links_with_business_id = []  # (doc_ref, business_id_value)
    hits_with_business_id = []   # (doc_ref, business_id_value)
    targets_with_business_id = []  # (doc_ref, business_id_value)
    
    # Load links
    print("  Loading links...")
    for link_doc in tqdm(db.collection("links").stream(), desc="    Links", leave=False):
        link_data = link_doc.to_dict()
        business_ref = link_data.get("business_ref")
        if business_ref and hasattr(business_ref, "id"):
            links_by_business[business_ref.id].append(link_doc.reference)
        # Check for business_id field
        business_id_field = link_data.get("business_id")
        if business_id_field and isinstance(business_id_field, str):
            links_with_business_id.append((link_doc.reference, business_id_field))
    
    # Load hits
    print("  Loading hits...")
    for hit_doc in tqdm(db.collection("hits").stream(), desc="    Hits", leave=False):
        hit_data = hit_doc.to_dict()
        business_ref = hit_data.get("business_ref")
        if business_ref and hasattr(business_ref, "id"):
            hits_by_business[business_ref.id].append(hit_doc.reference)
        # Check for business_id field
        business_id_field = hit_data.get("business_id")
        if business_id_field and isinstance(business_id_field, str):
            hits_with_business_id.append((hit_doc.reference, business_id_field))
    
    # Load targets (across all campaigns)
    print("  Loading targets...")
    campaigns = list(db.collection("campaigns").stream())
    for campaign_doc in tqdm(campaigns, desc="    Campaigns", leave=False):
        targets_ref = campaign_doc.reference.collection("targets")
        for target_doc in targets_ref.stream():
            target_data = target_doc.to_dict()
            business_ref = target_data.get("business_ref")
            if business_ref and hasattr(business_ref, "id"):
                targets_by_business[business_ref.id].append(target_doc.reference)
            # Check for business_id field
            business_id_field = target_data.get("business_id")
            if business_id_field and isinstance(business_id_field, str):
                targets_with_business_id.append((target_doc.reference, business_id_field))
    
    # Load customer overlays
    print("  Loading customer overlays...")
    customers = list(db.collection("customers").stream())
    for customer_doc in tqdm(customers, desc="    Customers", leave=False):
        customer_id = customer_doc.id
        businesses_ref = customer_doc.reference.collection("businesses")
        for overlay_doc in businesses_ref.stream():
            business_id = overlay_doc.id
            overlays_by_business[business_id].append((customer_id, overlay_doc.reference))
    
    # Load blacklist entries
    print("  Loading blacklist entries...")
    for customer_doc in tqdm(customers, desc="    Blacklists", leave=False):
        customer_id = customer_doc.id
        blacklist_ref = customer_doc.reference.collection("blacklist")
        for blacklist_doc in blacklist_ref.stream():
            data = blacklist_doc.to_dict() or {}
            # Check business_id field
            business_id = data.get("business_id")
            if business_id:
                blacklist_by_business[business_id].append((customer_id, blacklist_doc.reference))
            # Check business_ref field
            business_ref = data.get("business")
            if business_ref:
                if hasattr(business_ref, "id"):
                    blacklist_by_business[business_ref.id].append((customer_id, blacklist_doc.reference))
    
    print(f"  Loaded: {len(links_by_business)} businesses in links, "
          f"{len(hits_by_business)} in hits, {len(targets_by_business)} in targets, "
          f"{len(overlays_by_business)} in overlays, {len(blacklist_by_business)} in blacklist")
    print(f"  Found business_id fields: {len(links_with_business_id)} in links, "
          f"{len(hits_with_business_id)} in hits, {len(targets_with_business_id)} in targets")
    
    return {
        "links": links_by_business,
        "hits": hits_by_business,
        "targets": targets_by_business,
        "overlays": overlays_by_business,
        "blacklist": blacklist_by_business,
        "links_with_business_id": links_with_business_id,
        "hits_with_business_id": hits_with_business_id,
        "targets_with_business_id": targets_with_business_id,
    }


def migrate_business_id_batch(
    db: firestore.Client,
    old_id: str,
    new_id: str,
    references: Dict,
    dry_run: bool = False
) -> Dict:
    """
    Migrate a single business ID using pre-loaded references.
    Returns statistics about the migration.
    """
    stats = {
        "business_created": False,
        "business_merged": False,
        "links_updated": 0,
        "targets_updated": 0,
        "overlays_updated": 0,
        "blacklist_updated": 0,
        "hits_updated": 0,
        "errors": []
    }
    
    try:
        old_business_ref = db.collection("businesses").document(old_id)
        new_business_ref = db.collection("businesses").document(new_id)
        
        old_business = old_business_ref.get()
        if not old_business.exists:
            stats["errors"].append(f"Old business {old_id} does not exist")
            return stats
        
        old_data = old_business.to_dict() or {}
        new_business = new_business_ref.get()
        
        # Create or merge canonical business
        if not dry_run:
            # Normalize business_id field in old_data if it exists
            if "business_id" in old_data:
                old_data["business_id"] = new_id
            
            if new_business.exists:
                # Merge data
                new_data = new_business.to_dict() or {}
                # Merge ownerIds
                old_owner_ids = set(old_data.get("ownerIds", []))
                new_owner_ids = set(new_data.get("ownerIds", []))
                merged_owner_ids = list(old_owner_ids | new_owner_ids)
                
                # Merge other fields (prefer old data for canonical fields)
                merged_data = {
                    **new_data,
                    **{k: v for k, v in old_data.items() 
                       if k in ["business_name", "street", "house_number", "city", "postcode", "address", "coordinate", "business_id"]},
                    "ownerIds": merged_owner_ids
                }
                
                # Ensure business_id is normalized in merged data
                if "business_id" in merged_data:
                    merged_data["business_id"] = new_id
                
                new_business_ref.set(merged_data, merge=True)
                stats["business_merged"] = True
            else:
                # Create new business with normalized business_id
                new_business_ref.set(old_data)
                stats["business_created"] = True
        
        # Update all references using pre-loaded data
        batch = db.batch()
        ops = 0
        
        # Update links
        for link_ref in references["links"].get(old_id, []):
            if not dry_run:
                batch.update(link_ref, {"business_ref": new_business_ref})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["links_updated"] += 1
        
        # Update hits
        for hit_ref in references["hits"].get(old_id, []):
            if not dry_run:
                batch.update(hit_ref, {"business_ref": new_business_ref})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["hits_updated"] += 1
        
        # Update targets
        for target_ref in references["targets"].get(old_id, []):
            if not dry_run:
                batch.update(target_ref, {"business_ref": new_business_ref})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["targets_updated"] += 1
        
        # Update customer overlays
        for customer_id, overlay_ref in references["overlays"].get(old_id, []):
            if not dry_run:
                overlay_doc = overlay_ref.get()
                if overlay_doc.exists:
                    old_data_overlay = overlay_doc.to_dict() or {}
                    
                    # Normalize business_id field in overlay if it exists
                    if "business_id" in old_data_overlay:
                        old_data_overlay["business_id"] = new_id
                    
                    # Check if new overlay exists
                    new_overlay_ref = (
                        db.collection("customers")
                        .document(customer_id)
                        .collection("businesses")
                        .document(new_id)
                    )
                    new_overlay = new_overlay_ref.get()
                    
                    if new_overlay.exists:
                        # Merge data
                        new_data_overlay = new_overlay.to_dict() or {}
                        merged_hit_count = max(
                            old_data_overlay.get("hit_count", 0),
                            new_data_overlay.get("hit_count", 0)
                        )
                        old_last_hit = old_data_overlay.get("last_hit_at")
                        new_last_hit = new_data_overlay.get("last_hit_at")
                        merged_last_hit = old_last_hit if old_last_hit else new_last_hit
                        if old_last_hit and new_last_hit:
                            merged_last_hit = max(old_last_hit, new_last_hit)
                        
                        overlay_merge_data = {
                            "business_ref": new_business_ref,
                            "hit_count": merged_hit_count,
                            "last_hit_at": merged_last_hit,
                            "updated_at": firestore.SERVER_TIMESTAMP,
                            **{k: v for k, v in old_data_overlay.items() 
                               if k not in ["business_ref", "hit_count", "last_hit_at", "updated_at"]}
                        }
                        # Ensure business_id is normalized
                        if "business_id" in overlay_merge_data:
                            overlay_merge_data["business_id"] = new_id
                        
                        new_overlay_ref.set(overlay_merge_data, merge=True)
                        overlay_ref.delete()
                    else:
                        # Copy to new ID with normalized business_id
                        new_overlay_ref.set({
                            "business_ref": new_business_ref,
                            **old_data_overlay
                        })
                        overlay_ref.delete()
            stats["overlays_updated"] += 1
        
        # Update blacklist
        for customer_id, blacklist_ref in references["blacklist"].get(old_id, []):
            if not dry_run:
                blacklist_doc = blacklist_ref.get()
                if blacklist_doc.exists:
                    data = blacklist_doc.to_dict() or {}
                    updates = {}
                    if data.get("business_id") == old_id:
                        updates["business_id"] = new_id
                    if data.get("business"):
                        business_ref_field = data.get("business")
                        if hasattr(business_ref_field, "id") and business_ref_field.id == old_id:
                            updates["business"] = new_business_ref
                        elif isinstance(business_ref_field, str) and f"/businesses/{old_id}" in business_ref_field:
                            updates["business"] = new_business_ref
                    
                    if updates:
                        batch.update(blacklist_ref, updates)
                        ops += 1
                        if ops >= BATCH_SIZE:
                            batch.commit()
                            batch = db.batch()
                            ops = 0
            stats["blacklist_updated"] += 1
        
        # Commit remaining batch operations
        if not dry_run and ops > 0:
            batch.commit()
        
        # Delete old business document (only if new one was created/merged successfully)
        if not dry_run and (stats["business_created"] or stats["business_merged"]):
            old_business_ref.delete()
        
    except Exception as e:
        stats["errors"].append(str(e))
    
    return stats


def normalize_business_id_fields(
    db: firestore.Client,
    references: Dict,
    dry_run: bool = False
) -> Dict:
    """
    Normalize business_id fields in links, hits, and targets documents.
    Returns statistics.
    """
    stats = {
        "links_updated": 0,
        "hits_updated": 0,
        "targets_updated": 0,
        "errors": []
    }
    
    batch = db.batch()
    ops = 0
    
    # Normalize business_id in links
    for link_ref, business_id_value in references.get("links_with_business_id", []):
        normalized_id = normalize_business_id(business_id_value)
        if business_id_value != normalized_id:
            if not dry_run:
                batch.update(link_ref, {"business_id": normalized_id})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["links_updated"] += 1
    
    # Normalize business_id in hits
    for hit_ref, business_id_value in references.get("hits_with_business_id", []):
        normalized_id = normalize_business_id(business_id_value)
        if business_id_value != normalized_id:
            if not dry_run:
                batch.update(hit_ref, {"business_id": normalized_id})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["hits_updated"] += 1
    
    # Normalize business_id in targets
    for target_ref, business_id_value in references.get("targets_with_business_id", []):
        normalized_id = normalize_business_id(business_id_value)
        if business_id_value != normalized_id:
            if not dry_run:
                batch.update(target_ref, {"business_id": normalized_id})
                ops += 1
                if ops >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops = 0
            stats["targets_updated"] += 1
    
    # Commit remaining batch operations
    if not dry_run and ops > 0:
        batch.commit()
    
    return stats


def migrate_all_business_ids(
    db: firestore.Client,
    dry_run: bool = False,
    limit: Optional[int] = None
) -> Dict:
    """
    Migrate all non-normalized business IDs using optimized bulk operations.
    Returns aggregate statistics.
    """
    print(f"Finding non-normalized business IDs (dry_run={dry_run})...")
    
    non_normalized = find_non_normalized_businesses(db)
    
    if limit:
        non_normalized = non_normalized[:limit]
    
    total = len(non_normalized)
    print(f"Found {total} non-normalized business IDs to migrate")
    
    if total == 0:
        print("All business IDs are already normalized!")
        # Still normalize business_id fields in documents
        references = preload_all_references(db)
        field_stats = normalize_business_id_fields(db, references, dry_run)
        return {
            "total": 0,
            "migrated": 0,
            "links_updated": 0,
            "targets_updated": 0,
            "overlays_updated": 0,
            "blacklist_updated": 0,
            "hits_updated": 0,
            "links_business_id_updated": field_stats["links_updated"],
            "hits_business_id_updated": field_stats["hits_updated"],
            "targets_business_id_updated": field_stats["targets_updated"],
            "errors": [],
            "businesses_with_errors": 0
        }
    
    # Pre-load all references
    references = preload_all_references(db)
    
    # Aggregate statistics
    aggregate_stats = {
        "total": total,
        "migrated": 0,
        "links_updated": 0,
        "targets_updated": 0,
        "overlays_updated": 0,
        "blacklist_updated": 0,
        "hits_updated": 0,
        "links_business_id_updated": 0,
        "hits_business_id_updated": 0,
        "targets_business_id_updated": 0,
        "errors": [],
        "businesses_with_errors": 0,
    }
    
    # Process each business
    print("\nMigrating business IDs...")
    for old_id, new_id in tqdm(non_normalized, desc="Processing"):
        stats = migrate_business_id_batch(db, old_id, new_id, references, dry_run)
        
        # Aggregate statistics
        if stats["business_created"] or stats["business_merged"]:
            aggregate_stats["migrated"] += 1
        
        aggregate_stats["links_updated"] += stats["links_updated"]
        aggregate_stats["targets_updated"] += stats["targets_updated"]
        aggregate_stats["overlays_updated"] += stats["overlays_updated"]
        aggregate_stats["blacklist_updated"] += stats["blacklist_updated"]
        aggregate_stats["hits_updated"] += stats["hits_updated"]
        
        if stats["errors"]:
            aggregate_stats["businesses_with_errors"] += 1
            aggregate_stats["errors"].extend([
                f"{old_id} -> {new_id}: {err}" for err in stats["errors"]
            ])
    
    # Normalize business_id fields in other documents
    print("\nNormalizing business_id fields in documents...")
    field_stats = normalize_business_id_fields(db, references, dry_run)
    aggregate_stats["links_business_id_updated"] = field_stats["links_updated"]
    aggregate_stats["hits_business_id_updated"] = field_stats["hits_updated"]
    aggregate_stats["targets_business_id_updated"] = field_stats["targets_updated"]
    
    return aggregate_stats


def main():
    parser = argparse.ArgumentParser(
        description="Normalize business IDs to lowercase and update all references"
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
    stats = migrate_all_business_ids(db, dry_run=args.dry_run, limit=args.limit)

    # Print summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"Total non-normalized businesses found: {stats['total']}")
    print(f"Businesses migrated: {stats['migrated']}")
    print(f"Links updated: {stats['links_updated']}")
    print(f"Targets updated: {stats['targets_updated']}")
    print(f"Customer overlays updated: {stats['overlays_updated']}")
    print(f"Blacklist entries updated: {stats['blacklist_updated']}")
    print(f"Hits updated: {stats['hits_updated']}")
    print(f"Links business_id fields normalized: {stats['links_business_id_updated']}")
    print(f"Hits business_id fields normalized: {stats['hits_business_id_updated']}")
    print(f"Targets business_id fields normalized: {stats['targets_business_id_updated']}")
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
