#!/usr/bin/env python3
"""
Cleanup script to delete health check hits that have been created by the health monitor.

This script finds and deletes hits created by the health monitor function. It can:
- Delete all hits for a specific test link ID
- Filter by age (delete hits older than X hours/days)
- Run in dry-run mode to preview what would be deleted

Usage:
    python cleanup_health_check_hits.py [--dry-run] [--project PROJECT_ID] [--database DATABASE_ID] 
                                        [--link-id LINK_ID] [--older-than-hours HOURS] [--older-than-days DAYS]
"""

import os
import sys
import argparse
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from google.cloud import firestore
from tqdm import tqdm

# Default configuration
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DEFAULT_DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")
DEFAULT_TEST_LINK_ID = "monitor-test-001"

# Batch sizes
BATCH_SIZE = 400  # Firestore batch limit is 500, leave some headroom


def delete_hits_in_batches(
    db: firestore.Client,
    link_id: str,
    older_than: Optional[datetime] = None,
    dry_run: bool = False
) -> int:
    """
    Delete hits for a specific link_id, optionally filtered by age.
    Returns the number of hits deleted.
    """
    hits_ref = db.collection('hits')
    
    # Build query
    query = hits_ref.where('link_id', '==', link_id)
    
    # If filtering by age, we need to fetch and filter in Python
    # (Firestore queries with timestamp comparisons can be tricky)
    all_hits = list(query.stream())
    
    # Filter by age if specified
    hits_to_delete = []
    for hit in all_hits:
        hit_data = hit.to_dict()
        hit_ts = hit_data.get('ts')
        
        if not hit_ts:
            # Skip hits without timestamp
            continue
        
        # Convert timestamp to datetime
        try:
            if hasattr(hit_ts, 'timestamp'):
                # Firestore Timestamp object
                hit_datetime = datetime.fromtimestamp(hit_ts.timestamp(), tz=timezone.utc)
            elif isinstance(hit_ts, datetime):
                # Python datetime object
                hit_datetime = hit_ts
            else:
                continue
            
            # Check if hit is older than threshold
            if older_than is None or hit_datetime < older_than:
                hits_to_delete.append(hit.reference)
        except Exception as e:
            print(f"Warning: Could not parse timestamp for hit {hit.id}: {e}", file=sys.stderr)
            continue
    
    if not hits_to_delete:
        return 0
    
    if dry_run:
        print(f"[DRY-RUN] Would delete {len(hits_to_delete)} hits for link_id={link_id}")
        if older_than:
            print(f"[DRY-RUN] Filtered by age: older than {older_than.isoformat()}")
        return len(hits_to_delete)
    
    # Delete in batches
    deleted_count = 0
    batch = db.batch()
    
    for i, hit_ref in enumerate(tqdm(hits_to_delete, desc="Deleting hits")):
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
        description="Cleanup health check hits created by the health monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run: preview what would be deleted
  python cleanup_health_check_hits.py --dry-run
  
  # Delete all health check hits
  python cleanup_health_check_hits.py --link-id monitor-test-001
  
  # Delete hits older than 24 hours
  python cleanup_health_check_hits.py --older-than-hours 24
  
  # Delete hits older than 7 days
  python cleanup_health_check_hits.py --older-than-days 7
  
  # Use specific project and database
  python cleanup_health_check_hits.py --project gb-qr-tracker-prod --database "(default)" --dry-run
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
    
    parser.add_argument(
        '--link-id',
        type=str,
        default=DEFAULT_TEST_LINK_ID,
        help=f'Test link ID to filter hits (default: {DEFAULT_TEST_LINK_ID})'
    )
    
    parser.add_argument(
        '--older-than-hours',
        type=int,
        help='Delete hits older than this many hours'
    )
    
    parser.add_argument(
        '--older-than-days',
        type=int,
        help='Delete hits older than this many days'
    )
    
    args = parser.parse_args()
    
    # Validate age arguments
    if args.older_than_hours and args.older_than_days:
        print("Error: Cannot specify both --older-than-hours and --older-than-days", file=sys.stderr)
        sys.exit(1)
    
    # Calculate age threshold
    older_than = None
    if args.older_than_hours:
        older_than = datetime.now(timezone.utc) - timedelta(hours=args.older_than_hours)
    elif args.older_than_days:
        older_than = datetime.now(timezone.utc) - timedelta(days=args.older_than_days)
    
    # Initialize Firestore client
    try:
        db = firestore.Client(project=args.project, database=args.database)
    except Exception as e:
        print(f"Error initializing Firestore client: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Confirm before deleting (unless dry-run)
    if not args.dry_run:
        print(f"⚠️  WARNING: This will DELETE hits from Firestore!")
        print(f"   Project: {args.project}")
        print(f"   Database: {args.database}")
        print(f"   Link ID: {args.link_id}")
        if older_than:
            print(f"   Age filter: older than {older_than.isoformat()}")
        else:
            print(f"   Age filter: none (all hits for this link_id)")
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
            link_id=args.link_id,
            older_than=older_than,
            dry_run=args.dry_run
        )
        
        if args.dry_run:
            print(f"\n✅ [DRY-RUN] Would delete {deleted_count} hits")
        else:
            print(f"\n✅ Successfully deleted {deleted_count} hits")
            
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

