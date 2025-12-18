#!/usr/bin/env python3
"""
Migration script to mark existing test data documents with is_test_data: true.

This script identifies and marks test data documents across multiple collections:
- hits: Documents with test link IDs, demo data, or health monitor hits
- links: Test link documents (e.g., monitor-test-001)
- campaigns: Campaigns exclusively used by test links
- businesses: Businesses exclusively used by test links
- unique_ips: Unique IP documents in test campaigns (campaigns/{id}/unique_ips/{ip_hash})
- customer_businesses: Customer business overlays for test businesses (customers/{uid}/businesses/{id})

Usage:
    python mark_test_data.py [--dry-run] [--project PROJECT_ID] [--database DATABASE_ID] 
                            [--test-link-id LINK_ID] [--include-demo] [--collections COLLECTIONS]
"""

import os
import sys
import argparse
from typing import List, Optional, Set
from datetime import datetime, timezone
from google.cloud import firestore
from tqdm import tqdm

# Default configuration
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DEFAULT_DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")
DEFAULT_TEST_LINK_ID = "monitor-test-001"

# Batch size for Firestore operations (max 500 per batch, use 450 for safety)
BATCH_SIZE = 450

# Collections to process
ALL_COLLECTIONS = ["hits", "links", "campaigns", "businesses", "unique_ips", "customer_businesses"]


def mark_hits_test_data(
    db: firestore.Client,
    test_link_id: str,
    include_demo: bool,
    dry_run: bool = False
) -> int:
    """
    Mark hits as test data based on:
    - link_id matching test_link_id
    - is_demo == True (if include_demo is True)
    - user_agent starting with "HealthMonitor/"
    
    Returns the number of hits marked.
    """
    hits_ref = db.collection('hits')
    marked_count = 0
    batch = db.batch()
    batch_count = 0
    
    # Query 1: Hits with test link_id
    print(f"  Querying hits with link_id == '{test_link_id}'...")
    query1 = hits_ref.where('link_id', '==', test_link_id)
    hits1 = list(query1.stream())
    
    for hit in tqdm(hits1, desc=f"  Processing {len(hits1)} hits (link_id)"):
        hit_data = hit.to_dict()
        # Skip if already marked
        if hit_data.get('is_test_data') is True:
            continue
        
        if not dry_run:
            batch.update(hit.reference, {'is_test_data': True})
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        marked_count += 1
    
    # Query 2: Hits with is_demo == True
    if include_demo:
        print(f"  Querying hits with is_demo == True...")
        query2 = hits_ref.where('is_demo', '==', True)
        hits2 = list(query2.stream())
        
        for hit in tqdm(hits2, desc=f"  Processing {len(hits2)} hits (is_demo)"):
            hit_data = hit.to_dict()
            # Skip if already marked
            if hit_data.get('is_test_data') is True:
                continue
            
            if not dry_run:
                batch.update(hit.reference, {'is_test_data': True})
                batch_count += 1
                if batch_count >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
            marked_count += 1
    
    # Query 3: Hits with user_agent starting with "HealthMonitor/"
    # Note: Firestore doesn't support prefix queries, so we need to fetch and filter
    # Since health monitor just started, there should be relatively few of these
    print(f"  Querying hits with HealthMonitor user agent...")
    print(f"  Note: This may take a while for large collections (Firestore doesn't support prefix queries)")
    
    # Use pagination to handle large collections
    health_monitor_hits = []
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
            hit_data = hit.to_dict()
            user_agent = hit_data.get('user_agent', '') or ''
            if user_agent.startswith('HealthMonitor/'):
                # Skip if already marked
                if hit_data.get('is_test_data') is not True:
                    health_monitor_hits.append(hit)
        
        if len(page_hits) < page_size:
            break
        
        last_doc = page_hits[-1]
        print(f"    Checked {total_checked} hits, found {len(health_monitor_hits)} HealthMonitor hits so far...")
    
    print(f"  Found {len(health_monitor_hits)} hits with HealthMonitor user agent (checked {total_checked} total)")
    for hit in tqdm(health_monitor_hits, desc=f"  Processing {len(health_monitor_hits)} hits (HealthMonitor)"):
        if not dry_run:
            batch.update(hit.reference, {'is_test_data': True})
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        marked_count += 1
    
    # Commit remaining batch
    if not dry_run and batch_count > 0:
        batch.commit()
    
    return marked_count


def mark_links_test_data(
    db: firestore.Client,
    test_link_id: str,
    dry_run: bool = False
) -> int:
    """
    Mark links as test data based on link_id matching test_link_id.
    Returns the number of links marked.
    """
    links_ref = db.collection('links')
    marked_count = 0
    
    # Get the test link document
    link_ref = links_ref.document(test_link_id)
    link_snap = link_ref.get()
    
    if not link_snap.exists:
        print(f"  Test link '{test_link_id}' not found")
        return 0
    
    link_data = link_snap.to_dict()
    # Skip if already marked
    if link_data.get('is_test_data') is True:
        print(f"  Test link '{test_link_id}' already marked")
        return 0
    
    if not dry_run:
        link_ref.update({'is_test_data': True})
    marked_count = 1
    
    return marked_count


def mark_campaigns_test_data(
    db: firestore.Client,
    test_link_id: str,
    dry_run: bool = False
) -> int:
    """
    Mark campaigns as test data if they're associated with test links.
    This is a conservative approach - only mark campaigns that are exclusively
    used by test links (all links referencing the campaign are test links).
    
    Returns the number of campaigns marked.
    """
    links_ref = db.collection('links')
    campaigns_ref = db.collection('campaigns')
    marked_count = 0
    batch = db.batch()
    batch_count = 0
    
    # Find all test links (by link_id pattern or is_test_data flag)
    print("  Finding test links...")
    test_links = []
    
    # Get the specific test link
    test_link_ref = links_ref.document(test_link_id)
    test_link_snap = test_link_ref.get()
    if test_link_snap.exists:
        test_links.append(test_link_snap)
    
    # Also find links with is_test_data == True or link_id starting with 'monitor-test'
    all_links = links_ref.stream()
    for link in all_links:
        link_data = link.to_dict() or {}
        link_id = link.id
        if (link_data.get('is_test_data') is True or 
            link_id.startswith('monitor-test')):
            if link not in test_links:
                test_links.append(link)
    
    print(f"  Found {len(test_links)} test links")
    
    # Collect campaign_refs from test links
    test_campaign_refs = set()
    for link in test_links:
        link_data = link.to_dict() or {}
        campaign_ref = link_data.get('campaign_ref')
        if campaign_ref and hasattr(campaign_ref, 'id'):
            test_campaign_refs.add(campaign_ref.id)
    
    if not test_campaign_refs:
        print("  No campaigns found in test links")
        return 0
    
    print(f"  Found {len(test_campaign_refs)} unique campaigns in test links")
    
    # For each campaign, check if ALL links referencing it are test links
    campaigns_to_mark = []
    for campaign_id in tqdm(test_campaign_refs, desc="  Checking campaigns"):
        campaign_ref = campaigns_ref.document(campaign_id)
        campaign_snap = campaign_ref.get()
        
        if not campaign_snap.exists:
            continue
        
        campaign_data = campaign_snap.to_dict() or {}
        # Skip if already marked
        if campaign_data.get('is_test_data') is True:
            continue
        
        # Find all links that reference this campaign
        links_with_campaign = list(links_ref.where('campaign_ref', '==', campaign_ref).stream())
        
        # Check if ALL links are test links
        all_test = True
        for link in links_with_campaign:
            link_data = link.to_dict() or {}
            link_id = link.id
            is_test = (link_data.get('is_test_data') is True or 
                      link_id.startswith('monitor-test'))
            if not is_test:
                all_test = False
                break
        
        if all_test and len(links_with_campaign) > 0:
            campaigns_to_mark.append(campaign_ref)
    
    print(f"  Found {len(campaigns_to_mark)} campaigns exclusively used by test links")
    
    # Mark campaigns
    for campaign_ref in tqdm(campaigns_to_mark, desc="  Marking campaigns"):
        if not dry_run:
            batch.update(campaign_ref, {'is_test_data': True})
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        marked_count += 1
    
    # Commit remaining batch
    if not dry_run and batch_count > 0:
        batch.commit()
    
    return marked_count


def mark_businesses_test_data(
    db: firestore.Client,
    test_link_id: str,
    dry_run: bool = False
) -> int:
    """
    Mark businesses as test data if they're associated with test links.
    This is a conservative approach - only mark businesses that are exclusively
    used by test links (all links referencing the business are test links).
    
    Returns the number of businesses marked.
    """
    links_ref = db.collection('links')
    businesses_ref = db.collection('businesses')
    marked_count = 0
    batch = db.batch()
    batch_count = 0
    
    # Find all test links (by link_id pattern or is_test_data flag)
    print("  Finding test links...")
    test_links = []
    
    # Get the specific test link
    test_link_ref = links_ref.document(test_link_id)
    test_link_snap = test_link_ref.get()
    if test_link_snap.exists:
        test_links.append(test_link_snap)
    
    # Also find links with is_test_data == True or link_id starting with 'monitor-test'
    all_links = links_ref.stream()
    for link in all_links:
        link_data = link.to_dict() or {}
        link_id = link.id
        if (link_data.get('is_test_data') is True or 
            link_id.startswith('monitor-test')):
            if link not in test_links:
                test_links.append(link)
    
    print(f"  Found {len(test_links)} test links")
    
    # Collect business_refs from test links
    test_business_refs = set()
    for link in test_links:
        link_data = link.to_dict() or {}
        business_ref = link_data.get('business_ref')
        if business_ref and hasattr(business_ref, 'id'):
            test_business_refs.add(business_ref.id)
    
    if not test_business_refs:
        print("  No businesses found in test links")
        return 0
    
    print(f"  Found {len(test_business_refs)} unique businesses in test links")
    
    # For each business, check if ALL links referencing it are test links
    businesses_to_mark = []
    for business_id in tqdm(test_business_refs, desc="  Checking businesses"):
        business_ref = businesses_ref.document(business_id)
        business_snap = business_ref.get()
        
        if not business_snap.exists:
            continue
        
        business_data = business_snap.to_dict() or {}
        # Skip if already marked
        if business_data.get('is_test_data') is True:
            continue
        
        # Find all links that reference this business
        links_with_business = list(links_ref.where('business_ref', '==', business_ref).stream())
        
        # Check if ALL links are test links
        all_test = True
        for link in links_with_business:
            link_data = link.to_dict() or {}
            link_id = link.id
            is_test = (link_data.get('is_test_data') is True or 
                      link_id.startswith('monitor-test'))
            if not is_test:
                all_test = False
                break
        
        if all_test and len(links_with_business) > 0:
            businesses_to_mark.append(business_ref)
    
    print(f"  Found {len(businesses_to_mark)} businesses exclusively used by test links")
    
    # Mark businesses
    for business_ref in tqdm(businesses_to_mark, desc="  Marking businesses"):
        if not dry_run:
            batch.update(business_ref, {'is_test_data': True})
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        marked_count += 1
    
    # Commit remaining batch
    if not dry_run and batch_count > 0:
        batch.commit()
    
    return marked_count


def mark_unique_ips_test_data(
    db: firestore.Client,
    dry_run: bool = False
) -> int:
    """
    Mark unique_ips subcollection documents as test data if their parent campaign is test data.
    
    Returns the number of unique_ips documents marked.
    """
    campaigns_ref = db.collection('campaigns')
    marked_count = 0
    batch = db.batch()
    batch_count = 0
    
    # Find all test campaigns
    print("  Finding test campaigns...")
    test_campaigns = []
    all_campaigns = campaigns_ref.stream()
    for campaign in all_campaigns:
        campaign_data = campaign.to_dict() or {}
        if campaign_data.get('is_test_data') is True:
            test_campaigns.append(campaign)
    
    print(f"  Found {len(test_campaigns)} test campaigns")
    
    # For each test campaign, mark all unique_ips documents
    for campaign in tqdm(test_campaigns, desc="  Processing campaigns"):
        unique_ips_ref = campaign.reference.collection('unique_ips')
        unique_ips_docs = unique_ips_ref.stream()
        
        for unique_ip_doc in unique_ips_docs:
            unique_ip_data = unique_ip_doc.to_dict() or {}
            # Skip if already marked
            if unique_ip_data.get('is_test_data') is True:
                continue
            
            if not dry_run:
                batch.update(unique_ip_doc.reference, {'is_test_data': True})
                batch_count += 1
                if batch_count >= BATCH_SIZE:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
            marked_count += 1
    
    # Commit remaining batch
    if not dry_run and batch_count > 0:
        batch.commit()
    
    return marked_count


def mark_customer_businesses_test_data(
    db: firestore.Client,
    dry_run: bool = False
) -> int:
    """
    Mark customers/{uid}/businesses/{businessId} documents as test data 
    if their parent business is test data.
    
    Returns the number of customer business documents marked.
    """
    businesses_ref = db.collection('businesses')
    customers_ref = db.collection('customers')
    marked_count = 0
    batch = db.batch()
    batch_count = 0
    
    # Find all test businesses
    print("  Finding test businesses...")
    test_businesses = []
    all_businesses = businesses_ref.stream()
    for business in all_businesses:
        business_data = business.to_dict() or {}
        if business_data.get('is_test_data') is True:
            test_businesses.append(business)
    
    print(f"  Found {len(test_businesses)} test businesses")
    
    # For each test business, find all customer overlays and mark them
    for business in tqdm(test_businesses, desc="  Processing businesses"):
        business_id = business.id
        
        # Find all customers that have this business
        # We need to iterate through all customers (no direct query for subcollections)
        all_customers = customers_ref.stream()
        for customer in all_customers:
            customer_businesses_ref = customer.reference.collection('businesses')
            customer_business_ref = customer_businesses_ref.document(business_id)
            customer_business_snap = customer_business_ref.get()
            
            if customer_business_snap.exists:
                customer_business_data = customer_business_snap.to_dict() or {}
                # Skip if already marked
                if customer_business_data.get('is_test_data') is True:
                    continue
                
                if not dry_run:
                    batch.update(customer_business_ref, {'is_test_data': True})
                    batch_count += 1
                    if batch_count >= BATCH_SIZE:
                        batch.commit()
                        batch = db.batch()
                        batch_count = 0
                marked_count += 1
    
    # Commit remaining batch
    if not dry_run and batch_count > 0:
        batch.commit()
    
    return marked_count


def main():
    parser = argparse.ArgumentParser(
        description="Mark existing test data documents with is_test_data: true",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run: preview what would be marked
  python mark_test_data.py --dry-run
  
  # Mark test data in all collections
  python mark_test_data.py --test-link-id monitor-test-001 --include-demo
  
  # Mark only hits collection
  python mark_test_data.py --collections hits --include-demo
  
  # Use specific project and database
  python mark_test_data.py --project gb-qr-tracker-prod --database "(default)" --dry-run
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be marked without actually updating documents'
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
        '--test-link-id',
        type=str,
        default=DEFAULT_TEST_LINK_ID,
        help=f'Test link ID to identify test data (default: {DEFAULT_TEST_LINK_ID})'
    )
    
    parser.add_argument(
        '--include-demo',
        action='store_true',
        help='Also mark hits with is_demo == True as test data'
    )
    
    parser.add_argument(
        '--collections',
        type=str,
        nargs='+',
        choices=ALL_COLLECTIONS,
        default=ALL_COLLECTIONS,
        help=f'Collections to process (default: all: {", ".join(ALL_COLLECTIONS)})'
    )
    
    args = parser.parse_args()
    
    # Initialize Firestore client
    try:
        db = firestore.Client(project=args.project, database=args.database)
    except Exception as e:
        print(f"Error initializing Firestore client: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Confirm before updating (unless dry-run)
    if not args.dry_run:
        print(f"⚠️  WARNING: This will UPDATE documents in Firestore!")
        print(f"   Project: {args.project}")
        print(f"   Database: {args.database}")
        print(f"   Test Link ID: {args.test_link_id}")
        print(f"   Include Demo: {args.include_demo}")
        print(f"   Collections: {', '.join(args.collections)}")
        print()
        response = input("Type 'UPDATE' to confirm: ")
        if response != "UPDATE":
            print("Cancelled.")
            sys.exit(0)
        print()
    
    # Process each collection
    total_marked = 0
    results = {}
    
    try:
        if 'hits' in args.collections:
            print("\n📊 Processing 'hits' collection...")
            marked = mark_hits_test_data(
                db=db,
                test_link_id=args.test_link_id,
                include_demo=args.include_demo,
                dry_run=args.dry_run
            )
            results['hits'] = marked
            total_marked += marked
            print(f"  ✅ Marked {marked} hits")
        
        if 'links' in args.collections:
            print("\n🔗 Processing 'links' collection...")
            marked = mark_links_test_data(
                db=db,
                test_link_id=args.test_link_id,
                dry_run=args.dry_run
            )
            results['links'] = marked
            total_marked += marked
            print(f"  ✅ Marked {marked} links")
        
        if 'campaigns' in args.collections:
            print("\n📢 Processing 'campaigns' collection...")
            marked = mark_campaigns_test_data(
                db=db,
                test_link_id=args.test_link_id,
                dry_run=args.dry_run
            )
            results['campaigns'] = marked
            total_marked += marked
            print(f"  ✅ Marked {marked} campaigns")
        
        if 'businesses' in args.collections:
            print("\n🏢 Processing 'businesses' collection...")
            marked = mark_businesses_test_data(
                db=db,
                test_link_id=args.test_link_id,
                dry_run=args.dry_run
            )
            results['businesses'] = marked
            total_marked += marked
            print(f"  ✅ Marked {marked} businesses")
        
        if 'unique_ips' in args.collections:
            print("\n🔢 Processing 'unique_ips' subcollection...")
            marked = mark_unique_ips_test_data(
                db=db,
                dry_run=args.dry_run
            )
            results['unique_ips'] = marked
            total_marked += marked
            print(f"  ✅ Marked {marked} unique_ips documents")
        
        if 'customer_businesses' in args.collections:
            print("\n👥 Processing 'customers/{uid}/businesses' subcollection...")
            marked = mark_customer_businesses_test_data(
                db=db,
                dry_run=args.dry_run
            )
            results['customer_businesses'] = marked
            total_marked += marked
            print(f"  ✅ Marked {marked} customer business documents")
        
        # Summary
        print("\n" + "="*60)
        if args.dry_run:
            print(f"✅ [DRY-RUN] Would mark {total_marked} documents total")
        else:
            print(f"✅ Successfully marked {total_marked} documents total")
        print("\nBreakdown by collection:")
        for collection, count in results.items():
            print(f"  - {collection}: {count}")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
