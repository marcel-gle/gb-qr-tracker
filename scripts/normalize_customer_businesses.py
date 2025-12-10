#!/usr/bin/env python3
"""
Fast script to normalize customer business documents.

For each customer in the customers collection:
  - Iterate through /businesses subcollection
  - Normalize document IDs to lowercase
  - Ensure business_id field exists (matches documentId)
  - Ensure business_ref field is correct (/businesses/{documentId})

Usage:
    python normalize_customer_businesses.py [--dry-run] [--project PROJECT_ID] [--database DATABASE_ID]
"""

import os
import sys
import argparse
from typing import Dict, List, Tuple
from google.cloud import firestore
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Default configuration
DEFAULT_PROJECT_ID = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"

# Batch size for Firestore operations (max 500 per batch, use 400 for safety)
BATCH_SIZE = 400
MAX_WORKERS = 10  # Number of parallel workers for processing customers


def normalize_document_id(doc_id: str) -> str:
    """Normalize document ID to lowercase."""
    return doc_id.lower()


def process_customer_businesses(
    db: firestore.Client,
    customer_id: str,
    dry_run: bool = False
) -> Dict:
    """
    Process all businesses for a single customer.
    Returns statistics about the processing.
    """
    stats = {
        "checked": 0,
        "renamed": 0,
        "merged": 0,
        "business_id_added": 0,
        "business_ref_fixed": 0,
        "errors": []
    }
    
    try:
        customer_ref = db.collection("customers").document(customer_id)
        businesses_ref = customer_ref.collection("businesses")
        
        # Get all business documents for this customer (with pagination for large collections)
        business_docs = []
        query = businesses_ref.limit(1000)
        last_doc = None
        
        while True:
            if last_doc:
                query = businesses_ref.limit(1000).start_after(last_doc)
            else:
                query = businesses_ref.limit(1000)
            
            batch = list(query.stream())
            if not batch:
                break
            
            business_docs.extend(batch)
            
            if len(batch) < 1000:
                break
            
            last_doc = batch[-1]
        
        stats["checked"] = len(business_docs)
        
        if not business_docs:
            return stats
        
        # Pre-load all business document data
        business_data_by_id = {}
        for business_doc in business_docs:
            business_data_by_id[business_doc.id] = business_doc.to_dict() or {}
        
        batch = db.batch()
        ops_count = 0
        
        for business_doc in business_docs:
            old_id = business_doc.id
            normalized_id = normalize_document_id(old_id)
            business_data = business_data_by_id[old_id]
            
            needs_rename = old_id != normalized_id
            needs_business_id = "business_id" not in business_data
            needs_business_ref_fix = False
            
            # Check business_ref
            business_ref = business_data.get("business_ref")
            expected_business_ref = db.collection("businesses").document(normalized_id)
            
            if not business_ref:
                needs_business_ref_fix = True
            elif hasattr(business_ref, "id"):
                # It's a DocumentReference
                if business_ref.id != normalized_id:
                    needs_business_ref_fix = True
            else:
                # It might be a string path
                expected_path = f"/businesses/{normalized_id}"
                if str(business_ref) != expected_path:
                    needs_business_ref_fix = True
            
            # Skip if nothing needs to be done
            if not needs_rename and not needs_business_id and not needs_business_ref_fix:
                continue
            
            if not dry_run:
                if needs_rename:
                    # Check if normalized document already exists (as a separate document)
                    new_business_ref = businesses_ref.document(normalized_id)
                    new_exists = normalized_id in business_data_by_id and normalized_id != old_id
                    
                    if new_exists:
                        # Merge data (use pre-loaded data)
                        new_data = business_data_by_id[normalized_id]
                        # Merge hit_count and last_hit_at (take max)
                        merged_hit_count = max(
                            business_data.get("hit_count", 0),
                            new_data.get("hit_count", 0)
                        )
                        old_last_hit = business_data.get("last_hit_at")
                        new_last_hit = new_data.get("last_hit_at")
                        merged_last_hit = old_last_hit if old_last_hit else new_last_hit
                        if old_last_hit and new_last_hit:
                            merged_last_hit = max(old_last_hit, new_last_hit)
                        
                        # Prepare merge data
                        merge_data = {
                            "business_id": normalized_id,
                            "business_ref": expected_business_ref,
                            "hit_count": merged_hit_count,
                            "last_hit_at": merged_last_hit,
                            **{k: v for k, v in business_data.items() 
                               if k not in ["business_id", "business_ref", "hit_count", "last_hit_at"]}
                        }
                        
                        batch.set(new_business_ref, merge_data, merge=True)
                        batch.delete(business_doc.reference)
                        ops_count += 2
                        stats["merged"] += 1
                    else:
                        # Just rename (copy and delete)
                        new_data = {
                            "business_id": normalized_id,
                            "business_ref": expected_business_ref,
                            **business_data
                        }
                        batch.set(new_business_ref, new_data)
                        batch.delete(business_doc.reference)
                        ops_count += 2
                        stats["renamed"] += 1
                else:
                    # No rename needed, just update fields
                    updates = {}
                    if needs_business_id:
                        updates["business_id"] = normalized_id
                        stats["business_id_added"] += 1
                    if needs_business_ref_fix:
                        updates["business_ref"] = expected_business_ref
                        stats["business_ref_fixed"] += 1
                    
                    if updates:
                        batch.update(business_doc.reference, updates)
                        ops_count += 1
                
                # Commit batch when approaching limit
                if ops_count >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    ops_count = 0
            else:
                # Dry run: just count what would happen
                if needs_rename:
                    new_exists = normalized_id in business_data_by_id and normalized_id != old_id
                    if new_exists:
                        stats["merged"] += 1
                    else:
                        stats["renamed"] += 1
                if needs_business_id:
                    stats["business_id_added"] += 1
                if needs_business_ref_fix:
                    stats["business_ref_fixed"] += 1
        
        # Commit remaining operations
        if not dry_run and ops_count > 0:
            batch.commit()
    
    except Exception as e:
        stats["errors"].append(f"Customer {customer_id}: {str(e)}")
    
    return stats


def normalize_all_customer_businesses(
    db: firestore.Client,
    dry_run: bool = False,
    max_workers: int = MAX_WORKERS
) -> Dict:
    """
    Normalize all customer business documents using parallel processing.
    Returns aggregate statistics.
    """
    print(f"Starting normalization (dry_run={dry_run}, workers={max_workers})...")
    
    # Pre-load all customer IDs (use pagination to avoid timeouts)
    print("Pre-loading customer IDs...")
    customers_ref = db.collection("customers")
    customer_ids = []
    
    query = customers_ref.limit(1000)
    last_doc = None
    
    while True:
        if last_doc:
            query = customers_ref.limit(1000).start_after(last_doc)
        else:
            query = customers_ref.limit(1000)
        
        batch = list(query.stream())
        if not batch:
            break
        
        for customer_doc in batch:
            customer_ids.append(customer_doc.id)
        
        if len(batch) < 1000:
            break
        
        last_doc = batch[-1]
    
    total_customers = len(customer_ids)
    print(f"Found {total_customers} customers to process")
    
    if total_customers == 0:
        print("No customers found!")
        return {
            "customers_processed": 0,
            "total_checked": 0,
            "total_renamed": 0,
            "total_merged": 0,
            "total_business_id_added": 0,
            "total_business_ref_fixed": 0,
            "errors": []
        }
    
    aggregate_stats = {
        "customers_processed": 0,
        "total_checked": 0,
        "total_renamed": 0,
        "total_merged": 0,
        "total_business_id_added": 0,
        "total_business_ref_fixed": 0,
        "errors": []
    }
    
    # Process customers in parallel
    def process_customer_wrapper(customer_id: str):
        """Wrapper for parallel processing."""
        return customer_id, process_customer_businesses(db, customer_id, dry_run)
    
    print("Processing customers in parallel...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_customer_wrapper, customer_id): customer_id
            for customer_id in customer_ids
        }
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            try:
                customer_id, stats = future.result()
                
                # Aggregate statistics
                aggregate_stats["customers_processed"] += 1
                aggregate_stats["total_checked"] += stats["checked"]
                aggregate_stats["total_renamed"] += stats["renamed"]
                aggregate_stats["total_merged"] += stats["merged"]
                aggregate_stats["total_business_id_added"] += stats["business_id_added"]
                aggregate_stats["total_business_ref_fixed"] += stats["business_ref_fixed"]
                
                if stats["errors"]:
                    aggregate_stats["errors"].extend(stats["errors"])
            except Exception as e:
                customer_id = futures[future]
                aggregate_stats["errors"].append(f"Customer {customer_id}: {str(e)}")
    
    return aggregate_stats


def main():
    parser = argparse.ArgumentParser(
        description="Normalize customer business documents (IDs, business_id, business_ref)"
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
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Number of parallel workers (default: {MAX_WORKERS})"
    )

    args = parser.parse_args()

    print(f"Project: {args.project}")
    print(f"Database: {args.database}")
    print(f"Dry run: {args.dry_run}")
    print(f"Workers: {args.workers}")

    # Initialize Firestore client
    db = firestore.Client(project=args.project, database=args.database)

    # Run normalization
    stats = normalize_all_customer_businesses(db, dry_run=args.dry_run, max_workers=args.workers)

    # Print summary
    print("\n" + "=" * 60)
    print("Normalization Summary")
    print("=" * 60)
    print(f"Customers processed: {stats['customers_processed']}")
    print(f"Business documents checked: {stats['total_checked']}")
    print(f"Documents renamed: {stats['total_renamed']}")
    print(f"Documents merged: {stats['total_merged']}")
    print(f"business_id fields added: {stats['total_business_id_added']}")
    print(f"business_ref fields fixed: {stats['total_business_ref_fixed']}")
    
    if stats["errors"]:
        print(f"\nErrors ({len(stats['errors'])}):")
        for error in stats["errors"][:20]:  # Show first 20 errors
            print(f"  - {error}")
        if len(stats["errors"]) > 20:
            print(f"  ... and {len(stats['errors']) - 20} more errors")

    if args.dry_run:
        print("\n[DRY RUN] No changes were written to the database")
    else:
        print("\nNormalization completed!")

    return 0 if len(stats["errors"]) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
