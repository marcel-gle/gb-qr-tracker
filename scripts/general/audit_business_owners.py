#!/usr/bin/env python3
"""
Comprehensive database audit script to find business-owner relationship issues.

This script scans the ENTIRE database to find:
- Businesses with multiple owners where some owners have no links/targets
- Orphaned customer overlays (overlay exists but owner not in ownerIds)
- Owners in ownerIds array with no corresponding overlay
- Businesses with no links at all
- Inconsistent owner/overlay/link relationships

Usage:
    python audit_business_owners.py [--env dev|prod] [--limit N] [--output-format json|text]

Example:
    python audit_business_owners.py --env dev --limit 100
"""

import argparse
import sys
import json
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import firebase_admin
from firebase_admin import credentials, firestore
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


def preload_all_references(db: firestore.Client) -> Dict:
    """
    Pre-load all documents that reference businesses for efficient analysis.
    Returns lookup maps for fast access.
    """
    print("=" * 80)
    print("PHASE 1: Pre-loading all references from database...")
    print("=" * 80)
    print()
    
    # Maps: business_id -> {owner_id -> [link_refs]}
    links_by_business_owner = defaultdict(lambda: defaultdict(list))
    
    # Maps: business_id -> {owner_id -> [target_refs]}
    targets_by_business_owner = defaultdict(lambda: defaultdict(list))
    
    # Maps: (owner_id, business_id) -> overlay_ref
    overlays_by_owner_business = {}
    
    # Set of all business_ids that have overlays
    businesses_with_overlays = set()
    
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
                links_by_business_owner[business_id][owner_id].append({
                    'id': link_doc.id,
                    'ref': link_doc.reference,
                    'active': link_data.get('active', True),
                    'hit_count': link_data.get('hit_count', 0)
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
        
        targets_ref = campaign_doc.reference.collection('targets')
        for target_doc in targets_ref.stream():
            target_data = target_doc.to_dict() or {}
            business_ref = target_data.get('business_ref')
            
            if business_ref and hasattr(business_ref, "id"):
                business_id = business_ref.id
                targets_by_business_owner[business_id][campaign_owner].append({
                    'id': target_doc.id,
                    'ref': target_doc.reference,
                    'campaign_id': campaign_id,
                    'campaign_name': campaign_data.get('campaign_name', 'N/A'),
                    'status': target_data.get('status', 'N/A'),
                    'has_link_ref': target_data.get('link_ref') is not None
                })
                targets_count += 1
    
    print(f"  ✅ Loaded {targets_count} targets from {len(all_campaigns)} campaigns")
    
    # Load customer overlays
    print("Loading customer business overlays...")
    all_customers = list(db.collection('customers').stream())
    overlays_count = 0
    
    for customer_doc in tqdm(all_customers, desc="  Customers"):
        customer_id = customer_doc.id
        businesses_ref = customer_doc.reference.collection('businesses')
        
        for overlay_doc in businesses_ref.stream():
            business_id = overlay_doc.id
            overlays_by_owner_business[(customer_id, business_id)] = {
                'ref': overlay_doc.reference,
                'data': overlay_doc.to_dict() or {}
            }
            businesses_with_overlays.add(business_id)
            overlays_count += 1
    
    print(f"  ✅ Loaded {overlays_count} overlays from {len(all_customers)} customers")
    print()
    
    return {
        'links_by_business_owner': links_by_business_owner,
        'targets_by_business_owner': targets_by_business_owner,
        'overlays_by_owner_business': overlays_by_owner_business,
        'businesses_with_overlays': businesses_with_overlays,
        'stats': {
            'total_links': links_count,
            'total_targets': targets_count,
            'total_overlays': overlays_count,
            'total_campaigns': len(all_campaigns),
            'total_customers': len(all_customers)
        }
    }


def audit_businesses(db: firestore.Client, references: Dict, limit: Optional[int] = None) -> List[Dict]:
    """
    Audit all businesses for data integrity issues.
    Returns list of issues found.
    """
    print("=" * 80)
    print("PHASE 2: Auditing all businesses...")
    print("=" * 80)
    print()
    
    links_by_business_owner = references['links_by_business_owner']
    targets_by_business_owner = references['targets_by_business_owner']
    overlays_by_owner_business = references['overlays_by_owner_business']
    businesses_with_overlays = references['businesses_with_overlays']
    
    issues = []
    
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
            issues.append({
                'type': 'no_owners',
                'business_id': business_id,
                'business_name': business_name,
                'severity': 'high',
                'message': f"Business has no owners in ownerIds array"
            })
            continue
        
        # Get all links and targets for this business
        business_links = links_by_business_owner.get(business_id, {})
        business_targets = targets_by_business_owner.get(business_id, {})
        
        # Check each owner
        owners_without_links = []
        owners_without_targets = []
        owners_without_overlays = []
        owners_with_overlays_but_no_links = []
        
        for owner_id in owner_ids:
            has_links = len(business_links.get(owner_id, [])) > 0
            has_targets = len(business_targets.get(owner_id, [])) > 0
            has_overlay = (owner_id, business_id) in overlays_by_owner_business
            
            if not has_overlay:
                owners_without_overlays.append(owner_id)
            
            if not has_links and not has_targets:
                # Owner has no links AND no targets
                owners_without_links.append(owner_id)
                if has_overlay:
                    owners_with_overlays_but_no_links.append(owner_id)
            elif not has_links:
                owners_without_links.append(owner_id)
            elif not has_targets:
                owners_without_targets.append(owner_id)
        
        # Check for orphaned overlays (overlay exists but owner not in ownerIds)
        for (overlay_owner_id, overlay_business_id) in overlays_by_owner_business.keys():
            if overlay_business_id == business_id and overlay_owner_id not in owner_ids:
                issues.append({
                    'type': 'orphaned_overlay',
                    'business_id': business_id,
                    'business_name': business_name,
                    'owner_id': overlay_owner_id,
                    'severity': 'high',
                    'message': f"Customer overlay exists for owner {overlay_owner_id} but owner not in ownerIds array"
                })
        
        # Report issues
        if owners_with_overlays_but_no_links:
            for owner_id in owners_with_overlays_but_no_links:
                issues.append({
                    'type': 'owner_with_overlay_but_no_links',
                    'business_id': business_id,
                    'business_name': business_name,
                    'owner_id': owner_id,
                    'severity': 'medium',
                    'message': f"Owner {owner_id} has customer overlay but no links or targets",
                    'has_overlay': True,
                    'link_count': 0,
                    'target_count': 0
                })
        
        if owners_without_overlays and len(owner_ids) > 1:
            # Only report if there are multiple owners (single owner without overlay might be OK)
            for owner_id in owners_without_overlays:
                link_count = len(business_links.get(owner_id, []))
                target_count = len(business_targets.get(owner_id, []))
                if link_count > 0 or target_count > 0:
                    issues.append({
                        'type': 'owner_without_overlay',
                        'business_id': business_id,
                        'business_name': business_name,
                        'owner_id': owner_id,
                        'severity': 'low',
                        'message': f"Owner {owner_id} in ownerIds but no customer overlay (has {link_count} links, {target_count} targets)"
                    })
        
        # Check if business has no links at all
        total_links = sum(len(links) for links in business_links.values())
        total_targets = sum(len(targets) for targets in business_targets.values())
        
        if total_links == 0 and total_targets == 0:
            issues.append({
                'type': 'business_without_links',
                'business_id': business_id,
                'business_name': business_name,
                'severity': 'medium',
                'message': f"Business has {len(owner_ids)} owner(s) but no links or targets",
                'owner_count': len(owner_ids)
            })
        
        # Check for multiple owners where some have links and others don't
        if len(owner_ids) > 1:
            owners_with_links = [oid for oid in owner_ids if len(business_links.get(oid, [])) > 0 or len(business_targets.get(oid, [])) > 0]
            owners_without_links_list = [oid for oid in owner_ids if oid not in owners_with_links]
            
            if owners_with_links and owners_without_links_list:
                issues.append({
                    'type': 'inconsistent_owners',
                    'business_id': business_id,
                    'business_name': business_name,
                    'severity': 'high',
                    'message': f"Business has {len(owners_with_links)} owner(s) with links/targets and {len(owners_without_links_list)} owner(s) without",
                    'owners_with_links': owners_with_links,
                    'owners_without_links': owners_without_links_list
                })
    
    return issues


def print_report(issues: List[Dict], references: Dict, output_format: str = 'text'):
    """Print audit report."""
    print()
    print("=" * 80)
    print("AUDIT REPORT")
    print("=" * 80)
    print()
    
    stats = references['stats']
    print("Database Statistics:")
    print(f"  Total businesses: {len(issues) if issues else 'N/A'}")
    print(f"  Total links: {stats['total_links']}")
    print(f"  Total targets: {stats['total_targets']}")
    print(f"  Total overlays: {stats['total_overlays']}")
    print(f"  Total campaigns: {stats['total_campaigns']}")
    print(f"  Total customers: {stats['total_customers']}")
    print()
    
    # Group issues by type
    issues_by_type = defaultdict(list)
    issues_by_severity = defaultdict(list)
    
    for issue in issues:
        issues_by_type[issue['type']].append(issue)
        issues_by_severity[issue['severity']].append(issue)
    
    print("Issues Found:")
    print(f"  Total issues: {len(issues)}")
    print(f"  High severity: {len(issues_by_severity['high'])}")
    print(f"  Medium severity: {len(issues_by_severity['medium'])}")
    print(f"  Low severity: {len(issues_by_severity['low'])}")
    print()
    
    # Show counts for specific high-priority issue types
    print("High Priority Issue Types:")
    print(f"  no_owners: {len(issues_by_type.get('no_owners', []))}")
    print(f"  orphaned_overlay: {len(issues_by_type.get('orphaned_overlay', []))}")
    print(f"  inconsistent_owners: {len(issues_by_type.get('inconsistent_owners', []))}")
    print()
    
    if output_format == 'json':
        print(json.dumps({
            'stats': stats,
            'issues': issues,
            'issues_by_type': {k: len(v) for k, v in issues_by_type.items()},
            'issues_by_severity': {k: len(v) for k, v in issues_by_severity.items()}
        }, indent=2))
        return
    
    # Print issues by type
    print("Issues by Type:")
    print()
    
    # Define priority order for issue types
    priority_order = ['no_owners', 'orphaned_overlay', 'inconsistent_owners', 
                      'owner_with_overlay_but_no_links', 'business_without_links', 
                      'owner_without_overlay']
    
    # Sort: priority types first, then others
    sorted_types = sorted(issues_by_type.items(), 
                         key=lambda x: (priority_order.index(x[0]) if x[0] in priority_order else 999, x[0]))
    
    for issue_type, type_issues in sorted_types:
        severity = type_issues[0]['severity'] if type_issues else 'unknown'
        print(f"{issue_type}: {len(type_issues)} issue(s) [{severity} severity]")
        print("-" * 80)
        
        # Show first 10 examples
        for issue in type_issues[:10]:
            print(f"  • {issue['business_id']} ({issue['business_name']})")
            print(f"    {issue['message']}")
            if 'owner_id' in issue:
                print(f"    Owner: {issue['owner_id']}")
        if len(type_issues) > 10:
            print(f"  ... and {len(type_issues) - 10} more")
        print()
    
    # Print high severity issues in detail
    if issues_by_severity['high']:
        print("=" * 80)
        print("HIGH SEVERITY ISSUES (Detailed)")
        print("=" * 80)
        print()
        
        for issue in issues_by_severity['high'][:20]:
            print(f"Type: {issue['type']}")
            print(f"Business: {issue['business_id']} - {issue['business_name']}")
            print(f"Issue: {issue['message']}")
            if 'owner_id' in issue:
                print(f"Owner: {issue['owner_id']}")
            print()
        
        if len(issues_by_severity['high']) > 20:
            print(f"... and {len(issues_by_severity['high']) - 20} more high severity issues")
            print()


def main():
    parser = argparse.ArgumentParser(
        description='Comprehensive database audit for business-owner relationship issues',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full audit of dev database
  python audit_business_owners.py --env dev
  
  # Limited audit (first 100 businesses)
  python audit_business_owners.py --env dev --limit 100
  
  # JSON output for programmatic processing
  python audit_business_owners.py --env dev --output-format json > audit_results.json
  
  # Production audit (be careful!)
  python audit_business_owners.py --env prod --limit 50
        """
    )
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev',
                       help='Environment to use (default: dev)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of businesses to audit (for testing)')
    parser.add_argument('--output-format', choices=['text', 'json'], default='text',
                       help='Output format (default: text)')
    
    args = parser.parse_args()
    
    db = initialize_firestore(args.env)
    
    # Preload all references
    references = preload_all_references(db)
    
    # Audit businesses
    issues = audit_businesses(db, references, limit=args.limit)
    
    # Print report
    print_report(issues, references, output_format=args.output_format)
    
    sys.exit(0 if len(issues) == 0 else 1)


if __name__ == "__main__":
    main()

