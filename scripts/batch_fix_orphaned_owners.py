#!/usr/bin/env python3
"""
Batch fix script to remove owners with overlays but no links/targets.

This script fixes the data integrity issue where owners exist in the ownerIds array
and have customer overlays, but no links or targets. This can happen when:
- Migration scripts created overlays for all owners regardless of links
- Upload processor created overlays before checking if links would be created

Usage:
    python batch_fix_orphaned_owners.py [--env dev|prod] [--dry-run] [--limit N] [--owner-id OWNER_ID]

Example:
    # Dry run to see what would be fixed
    python batch_fix_orphaned_owners.py --env dev --dry-run --limit 10
    
    # Fix all issues for a specific owner
    python batch_fix_orphaned_owners.py --env dev --owner-id xLRk37rnV7T4CbOXzW5N3saxVfy1 --dry-run
    
    # Actually apply fixes (after reviewing dry run)
    python batch_fix_orphaned_owners.py --env dev --owner-id xLRk37rnV7T4CbOXzW5N3saxVfy1
"""

import argparse
import sys
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import ArrayRemove
from tqdm import tqdm


def initialize_firestore(env: str = "dev"):
    """Initialize Firestore client."""
    if not firebase_admin._apps:
        if env == "prod":
            service_account_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"
        else:
            service_account_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
        
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)
    
    return firestore.client()


def preload_references(db: firestore.Client, owner_id_filter: Optional[str] = None) -> Dict:
    """
    Pre-load all references for efficient analysis.
    Returns lookup maps grouped by business and owner.
    """
    print("=" * 80)
    print("PHASE 1: Pre-loading all references from database...")
    print("=" * 80)
    print()
    
    # Maps: business_id -> {owner_id -> [link_refs]}
    links_by_business_owner = defaultdict(lambda: defaultdict(list))
    
    # Maps: business_id -> {owner_id -> [target_refs]}
    targets_by_business_owner = defaultdict(lambda: defaultdict(list))
    
    # Load links
    print("Loading links...")
    links_ref = db.collection("links")
    query = links_ref.limit(1000)
    last_doc = None
    links_count = 0
    
    while True:
        if last_doc:
            query = links_ref.limit(1000).start_after(last_doc)
        else:
            query = links_ref.limit(1000)
        
        batch = list(query.stream())
        if not batch:
            break
        
        for link_doc in batch:
            link_data = link_doc.to_dict() or {}
            business_ref = link_data.get("business_ref")
            owner_id = link_data.get("owner_id")
            
            if business_ref and hasattr(business_ref, "id") and owner_id:
                business_id = business_ref.id
                if not owner_id_filter or owner_id == owner_id_filter:
                    links_by_business_owner[business_id][owner_id].append({
                        'id': link_doc.id,
                        'ref': link_doc.reference
                    })
                    links_count += 1
        
        if len(batch) < 1000:
            break
        
        last_doc = batch[-1]
    
    print(f"  ✅ Loaded {links_count} links")
    
    # Load targets from all campaigns
    print("Loading targets from all campaigns...")
    all_campaigns = list(db.collection('campaigns').stream())
    targets_count = 0
    
    for campaign_doc in tqdm(all_campaigns, desc="  Campaigns"):
        campaign_data = campaign_doc.to_dict() or {}
        campaign_owner = campaign_data.get('owner_id')
        campaign_id = campaign_doc.id
        
        if not campaign_owner:
            continue
        
        if owner_id_filter and campaign_owner != owner_id_filter:
            continue
        
        targets_ref = campaign_doc.reference.collection('targets')
        for target_doc in targets_ref.stream():
            target_data = target_doc.to_dict() or {}
            business_ref = target_data.get('business_ref')
            
            if business_ref and hasattr(business_ref, "id"):
                business_id = business_ref.id
                targets_by_business_owner[business_id][campaign_owner].append({
                    'id': target_doc.id,
                    'ref': target_doc.reference,
                    'campaign_id': campaign_id
                })
                targets_count += 1
    
    print(f"  ✅ Loaded {targets_count} targets from {len(all_campaigns)} campaigns")
    print()
    
    return {
        'links_by_business_owner': links_by_business_owner,
        'targets_by_business_owner': targets_by_business_owner
    }


def find_orphaned_owners(db: firestore.Client, references: Dict, owner_id_filter: Optional[str] = None, limit: Optional[int] = None) -> List[Dict]:
    """
    Find all businesses with owners that have overlays but no links/targets.
    Returns list of fixes to apply.
    """
    print("=" * 80)
    print("PHASE 2: Finding orphaned owners...")
    print("=" * 80)
    print()
    
    links_by_business_owner = references['links_by_business_owner']
    targets_by_business_owner = references['targets_by_business_owner']
    
    fixes = []
    
    # Load all businesses
    print("Loading businesses...")
    all_businesses = list(db.collection('businesses').stream())
    if limit:
        all_businesses = all_businesses[:limit]
        print(f"  (Limited to first {limit} businesses)")
    
    print(f"  ✅ Loaded {len(all_businesses)} businesses")
    print()
    print("Analyzing businesses...")
    print()
    
    for business_doc in tqdm(all_businesses, desc="  Businesses"):
        business_id = business_doc.id
        business_data = business_doc.to_dict() or {}
        business_name = business_data.get('business_name', 'N/A')
        owner_ids = business_data.get('ownerIds', [])
        
        if not owner_ids:
            continue
        
        # Filter by owner_id if specified
        if owner_id_filter:
            if owner_id_filter not in owner_ids:
                continue
            owner_ids_to_check = [owner_id_filter]
        else:
            owner_ids_to_check = owner_ids
        
        # Check each owner
        for owner_id in owner_ids_to_check:
            # Check if owner has links or targets
            has_links = len(links_by_business_owner.get(business_id, {}).get(owner_id, [])) > 0
            has_targets = len(targets_by_business_owner.get(business_id, {}).get(owner_id, [])) > 0
            
            if not has_links and not has_targets:
                # Owner has no links and no targets - check if overlay exists
                customer_business_ref = db.collection('customers').document(owner_id).collection('businesses').document(business_id)
                customer_business_snap = customer_business_ref.get()
                
                if customer_business_snap.exists:
                    # This is an orphaned owner - has overlay but no links/targets
                    fixes.append({
                        'business_id': business_id,
                        'business_name': business_name,
                        'owner_id': owner_id,
                        'has_overlay': True,
                        'link_count': 0,
                        'target_count': 0
                    })
    
    return fixes


def apply_fixes(db: firestore.Client, fixes: List[Dict], dry_run: bool = False) -> Dict:
    """
    Apply fixes to remove orphaned owners.
    Returns statistics about the fixes.
    """
    print()
    print("=" * 80)
    print(f"PHASE 3: {'DRY RUN - Previewing' if dry_run else 'Applying'} fixes...")
    print("=" * 80)
    print()
    
    stats = {
        'total_fixes': len(fixes),
        'businesses_fixed': 0,
        'owners_removed': 0,
        'overlays_deleted': 0,
        'errors': []
    }
    
    if not fixes:
        print("No fixes needed!")
        return stats
    
    print(f"Found {len(fixes)} orphaned owner(s) to fix")
    print()
    
    # Group fixes by business to batch operations
    fixes_by_business = defaultdict(list)
    for fix in fixes:
        fixes_by_business[fix['business_id']].append(fix)
    
    print(f"Affects {len(fixes_by_business)} unique business(es)")
    print()
    
    if dry_run:
        print("DRY RUN - Would perform the following fixes:")
        print()
        for i, fix in enumerate(fixes[:20], 1):
            print(f"{i}. Business: {fix['business_id']} ({fix['business_name']})")
            print(f"   Remove owner: {fix['owner_id']}")
            print(f"   Delete overlay: customers/{fix['owner_id']}/businesses/{fix['business_id']}")
            print()
        if len(fixes) > 20:
            print(f"... and {len(fixes) - 20} more fixes")
        print()
        print("Run without --dry-run to apply these fixes")
        return stats
    
    # Apply fixes in batches
    batch = db.batch()
    batch_count = 0
    BATCH_SIZE = 400  # Firestore limit is 500
    
    for business_id, business_fixes in tqdm(fixes_by_business.items(), desc="  Fixing businesses"):
        try:
            business_ref = db.collection('businesses').document(business_id)
            
            for fix in business_fixes:
                owner_id = fix['owner_id']
                
                # Remove ownerId from ownerIds array
                batch.update(business_ref, {"ownerIds": ArrayRemove([owner_id])})
                batch_count += 1
                stats['owners_removed'] += 1
                
                # Delete customer overlay
                customer_business_ref = db.collection('customers').document(owner_id).collection('businesses').document(business_id)
                batch.delete(customer_business_ref)
                batch_count += 1
                stats['overlays_deleted'] += 1
                
                # Commit batch if we're approaching the limit
                if batch_count >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
            
            stats['businesses_fixed'] += 1
            
        except Exception as e:
            error_msg = f"Error fixing business {business_id}: {e}"
            stats['errors'].append(error_msg)
            print(f"  ❌ {error_msg}")
    
    # Commit remaining batch
    if batch_count > 0:
        batch.commit()
    
    return stats


def print_summary(stats: Dict, fixes: List[Dict]):
    """Print summary of fixes."""
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print()
    print(f"Total fixes found: {stats['total_fixes']}")
    print(f"Businesses affected: {len(set(f['business_id'] for f in fixes))}")
    print(f"Unique owners to remove: {len(set(f['owner_id'] for f in fixes))}")
    print()
    
    if stats['errors']:
        print(f"Errors: {len(stats['errors'])}")
        for error in stats['errors'][:10]:
            print(f"  • {error}")
        if len(stats['errors']) > 10:
            print(f"  ... and {len(stats['errors']) - 10} more errors")
        print()
    
    if not stats.get('businesses_fixed', 0):
        return
    
    print("Fixes applied:")
    print(f"  Businesses fixed: {stats['businesses_fixed']}")
    print(f"  Owners removed from ownerIds: {stats['owners_removed']}")
    print(f"  Customer overlays deleted: {stats['overlays_deleted']}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Batch fix script to remove owners with overlays but no links/targets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run on first 10 businesses
  python batch_fix_orphaned_owners.py --env dev --dry-run --limit 10
  
  # Dry run for specific owner (the problematic one from audit)
  python batch_fix_orphaned_owners.py --env dev --owner-id xLRk37rnV7T4CbOXzW5N3saxVfy1 --dry-run
  
  # Actually apply fixes for specific owner
  python batch_fix_orphaned_owners.py --env dev --owner-id xLRk37rnV7T4CbOXzW5N3saxVfy1
  
  # Full fix (all orphaned owners, be careful!)
  python batch_fix_orphaned_owners.py --env dev --dry-run
        """
    )
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev',
                       help='Environment to use (default: dev)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be fixed without making changes (recommended first step)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of businesses to check (for testing)')
    parser.add_argument('--owner-id', type=str, default=None,
                       help='Only fix issues for a specific owner ID')
    
    args = parser.parse_args()
    
    db = initialize_firestore(args.env)
    
    # Preload references
    references = preload_references(db, owner_id_filter=args.owner_id)
    
    # Find orphaned owners
    fixes = find_orphaned_owners(db, references, owner_id_filter=args.owner_id, limit=args.limit)
    
    # Apply fixes
    stats = apply_fixes(db, fixes, dry_run=args.dry_run)
    
    # Print summary
    print_summary(stats, fixes)
    
    sys.exit(0 if len(stats.get('errors', [])) == 0 else 1)


if __name__ == "__main__":
    main()

