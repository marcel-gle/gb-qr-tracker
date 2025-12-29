#!/usr/bin/env python3
"""
Migration script to delete test hits from the hits collection.

After migrating to the new test data isolation approach, test hits are now written
to the test_hits collection. This script removes old test hits from the hits collection
that were created before the migration.

This script identifies test hits by:
- link_id starting with 'monitor-test'
- is_test_data == True
- user_agent starting with 'HealthMonitor/'

Usage:
    python migrate_delete_test_hits.py [--dry-run] [--project PROJECT_ID] [--database DATABASE_ID]
"""

import os
import sys
import argparse
from typing import List, Set
from datetime import datetime, timezone
from google.cloud import firestore
from tqdm import tqdm

# Default configuration
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DEFAULT_DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")

# Batch sizes
BATCH_SIZE = 400  # Firestore batch limit is 500, leave some headroom


def find_test_hits(db: firestore.Client) -> List[firestore.DocumentReference]:
    """
    Find all test hits in the hits collection.
    Returns a list of document references to delete.
    """
    hits_ref = db.collection('hits')
    hits_to_delete = []
    processed_hit_ids: Set[str] = set()
    
    print("Searching for test hits in hits collection...")
    print("This may take a while for large collections...")
    
    # Strategy: Query by link_id pattern first (most efficient), then scan for other patterns
    # Since Firestore doesn't support prefix queries, we'll need to scan
    
    # Method 1: Query hits with is_test_data == True (if indexed)
    print("  Querying hits with is_test_data == True...")
    try:
        query1 = hits_ref.where('is_test_data', '==', True).limit(1000)
        hits1 = list(query1.stream())
        print(f"    Found {len(hits1)} hits with is_test_data flag")
        for hit in hits1:
            if hit.id not in processed_hit_ids:
                hits_to_delete.append(hit.reference)
                processed_hit_ids.add(hit.id)
    except Exception as e:
        print(f"    Warning: Could not query by is_test_data (may not be indexed): {e}")
    
    # Method 2: Scan for link_id starting with 'monitor-test'
    # We'll need to scan all hits and filter in Python (Firestore doesn't support prefix queries)
    print("  Scanning hits for link_id starting with 'monitor-test'...")
    print("    (This requires scanning all hits - may take a while)")
    
    last_doc = None
    page_size = 1000
    total_checked = 0
    
    while True:
        query = hits_ref.limit(page_size)
        if last_doc:
            query = query.start_after(last_doc)
        
        page_hits = list(query.stream())
        if not page_hits:
            break
        
        for hit in page_hits:
            total_checked += 1
            if hit.id in processed_hit_ids:
                continue
            
            hit_data = hit.to_dict() or {}
            link_id = hit_data.get('link_id', '')
            
            # Check if link_id starts with 'monitor-test'
            if link_id.startswith('monitor-test'):
                hits_to_delete.append(hit.reference)
                processed_hit_ids.add(hit.id)
        
        if len(page_hits) < page_size:
            break
        
        last_doc = page_hits[-1]
        if total_checked % 5000 == 0:
            print(f"    Checked {total_checked} hits, found {len(hits_to_delete)} test hits so far...")
    
    print(f"    Checked {total_checked} hits total")
    
    # Method 3: Scan for user_agent starting with 'HealthMonitor/'
    print("  Scanning hits for HealthMonitor user agent...")
    print("    (This requires scanning all hits again - may take a while)")
    
    last_doc = None
    total_checked = 0
    
    while True:
        query = hits_ref.limit(page_size)
        if last_doc:
            query = query.start_after(last_doc)
        
        page_hits = list(query.stream())
        if not page_hits:
            break
        
        for hit in page_hits:
            total_checked += 1
            if hit.id in processed_hit_ids:
                continue
            
            hit_data = hit.to_dict() or {}
            user_agent = hit_data.get('user_agent', '') or ''
            
            # Check if user_agent starts with 'HealthMonitor/'
            if user_agent.startswith('HealthMonitor/'):
                hits_to_delete.append(hit.reference)
                processed_hit_ids.add(hit.id)
        
        if len(page_hits) < page_size:
            break
        
        last_doc = page_hits[-1]
        if total_checked % 5000 == 0:
            print(f"    Checked {total_checked} hits, found {len(hits_to_delete)} test hits so far...")
    
    print(f"    Checked {total_checked} hits total")
    
    # Remove duplicates (in case a hit matches multiple criteria)
    unique_hits_to_delete = []
    seen_ids = set()
    for hit_ref in hits_to_delete:
        if hit_ref.id not in seen_ids:
            unique_hits_to_delete.append(hit_ref)
            seen_ids.add(hit_ref.id)
    
    return unique_hits_to_delete


def delete_hits_in_batches(
    db: firestore.Client,
    hits_to_delete: List[firestore.DocumentReference],
    dry_run: bool = False
) -> int:
    """
    Delete hits in batches.
    Returns the number of hits deleted.
    """
    if not hits_to_delete:
        return 0
    
    if dry_run:
        print(f"\n[DRY-RUN] Would delete {len(hits_to_delete)} test hits from hits collection")
        return len(hits_to_delete)
    
    # Delete in batches
    deleted_count = 0
    batch = db.batch()
    
    for i, hit_ref in enumerate(tqdm(hits_to_delete, desc="Deleting test hits")):
        batch.delete(hit_ref)
        deleted_count += 1
        
        # Commit batch when it reaches BATCH_SIZE or at the end
        if (i + 1) % BATCH_SIZE == 0 or (i + 1) == len(hits_to_delete):
            try:
                batch.commit()
                batch = db.batch()
            except Exception as e:
                print(f"Error committing batch: {e}", file=sys.stderr)
                raise
    
    return deleted_count


def main():
    parser = argparse.ArgumentParser(
        description="Delete test hits from hits collection (migration to test_hits collection)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run: preview what would be deleted
  python migrate_delete_test_hits.py --dry-run
  
  # Delete all test hits from hits collection
  python migrate_delete_test_hits.py
  
  # Use specific project and database
  python migrate_delete_test_hits.py --project gb-qr-tracker-prod --database "(default)" --dry-run
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be deleted without actually deleting'
    )
    
    parser.add_argument(
        '--project',
        type=str,
        default=DEFAULT_PROJECT_ID,
        help=f'GCP Project ID (default: {DEFAULT_PROJECT_ID})'
    )
    
    parser.add_argument(
        '--database',
        type=str,
        default=DEFAULT_DATABASE_ID,
        help=f'Firestore Database ID (default: {DEFAULT_DATABASE_ID})'
    )
    
    args = parser.parse_args()
    
    # Initialize Firestore client
    try:
        db = firestore.Client(project=args.project, database=args.database)
    except Exception as e:
        print(f"Error initializing Firestore client: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Find test hits
    try:
        hits_to_delete = find_test_hits(db)
        print(f"\nFound {len(hits_to_delete)} test hits in hits collection")
    except Exception as e:
        print(f"\n❌ Error finding test hits: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    if not hits_to_delete:
        print("\n✅ No test hits found in hits collection. Nothing to delete.")
        return
    
    # Confirm before deleting (unless dry-run)
    if not args.dry_run:
        print(f"\n⚠️  WARNING: This will DELETE {len(hits_to_delete)} hits from Firestore!")
        print(f"   Project: {args.project}")
        print(f"   Database: {args.database}")
        print(f"   Collection: hits")
        print(f"   These are test hits that should now be in test_hits collection")
        print()
        response = input("Type 'DELETE' to confirm: ")
        if response != "DELETE":
            print("Cancelled.")
            sys.exit(0)
        print()
    
    # Delete hits
    try:
        deleted_count = delete_hits_in_batches(
            db=db,
            hits_to_delete=hits_to_delete,
            dry_run=args.dry_run
        )
        
        if args.dry_run:
            print(f"\n✅ [DRY-RUN] Would delete {deleted_count} test hits from hits collection")
        else:
            print(f"\n✅ Successfully deleted {deleted_count} test hits from hits collection")
            print("   Test hits are now isolated in test_hits collection")
            
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

