#!/usr/bin/env python3
"""
Debug script to investigate a business that has no tracking links.

Usage:
    python debug_business_links.py <business_id> [--env dev|prod]

Example:
    python debug_business_links.py adlatus-gmbh-47798 --env prod
"""

import argparse
import sys
from typing import Optional
import firebase_admin
from firebase_admin import credentials, firestore


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


def debug_business_links(db: firestore.Client, business_id: str):
    """Debug a business to find why it has no links."""
    print(f"\n{'='*80}")
    print(f"Debugging business: {business_id}")
    print(f"{'='*80}\n")
    
    # 1. Check if business exists in canonical collection
    business_ref = db.collection('businesses').document(business_id)
    business_snap = business_ref.get()
    
    if not business_snap.exists:
        print(f"❌ ERROR: Business '{business_id}' does not exist in businesses/ collection")
        return
    
    business_data = business_snap.to_dict()
    print(f"✅ Business exists in businesses/ collection")
    print(f"   Business Name: {business_data.get('business_name', 'N/A')}")
    print(f"   Created At: {business_data.get('created_at', 'N/A')}")
    print(f"   Owner IDs: {business_data.get('ownerIds', [])}")
    print()
    
    # 2. Check for links with this business_ref
    print("🔍 Checking links collection...")
    links_query = db.collection('links').where('business_ref', '==', business_ref)
    links = list(links_query.stream())
    
    if links:
        print(f"✅ Found {len(links)} link(s) referencing this business:")
        for link in links:
            link_data = link.to_dict()
            print(f"   - Link ID: {link.id}")
            print(f"     Destination: {link_data.get('destination', 'N/A')}")
            print(f"     Active: {link_data.get('active', 'N/A')}")
            print(f"     Owner ID: {link_data.get('owner_id', 'N/A')}")
            print(f"     Campaign: {link_data.get('campaign_name', 'N/A')}")
            print(f"     Hit Count: {link_data.get('hit_count', 0)}")
            print()
    else:
        print(f"❌ No links found with business_ref pointing to this business")
        print()
    
    # 3. Check for targets (campaigns/{campaignId}/targets) that reference this business
    print("🔍 Checking targets in campaigns...")
    all_campaigns = db.collection('campaigns').stream()
    targets_found = []
    
    for campaign_doc in all_campaigns:
        campaign_id = campaign_doc.id
        targets_ref = campaign_doc.reference.collection('targets')
        targets_query = targets_ref.where('business_ref', '==', business_ref)
        targets = list(targets_query.stream())
        
        if targets:
            campaign_data = campaign_doc.to_dict()
            for target in targets:
                target_data = target.to_dict()
                link_ref = target_data.get('link_ref')
                link_ref_id = None
                if link_ref:
                    if hasattr(link_ref, 'id'):
                        link_ref_id = link_ref.id
                    elif isinstance(link_ref, str):
                        link_ref_id = link_ref.split('/')[-1]
                
                targets_found.append({
                    'campaign_id': campaign_id,
                    'campaign_name': campaign_data.get('campaign_name', 'N/A'),
                    'target_id': target.id,
                    'status': target_data.get('status', 'N/A'),
                    'reason_excluded': target_data.get('reason_excluded'),
                    'has_link_ref': link_ref is not None,
                    'link_ref_id': link_ref_id
                })
    
    if targets_found:
        print(f"✅ Found {len(targets_found)} target(s) referencing this business:")
        for target in targets_found:
            print(f"   - Campaign: {target['campaign_name']} ({target['campaign_id']})")
            print(f"     Target ID: {target['target_id']}")
            print(f"     Status: {target['status']}")
            if target['reason_excluded']:
                print(f"     ❌ Excluded Reason: {target['reason_excluded']}")
            if target['has_link_ref']:
                print(f"     ✅ Has link_ref: {target['link_ref_id']}")
                # Verify the link actually exists
                link_check = db.collection('links').document(target['link_ref_id']).get()
                if link_check.exists:
                    print(f"       (Link document exists)")
                else:
                    print(f"       ⚠️  WARNING: Link document does NOT exist!")
            else:
                print(f"     ❌ No link_ref (link was not created)")
            print()
    else:
        print(f"❌ No targets found referencing this business")
        print()
    
    # 4. Check customer overlays
    print("🔍 Checking customer business overlays...")
    owner_ids = business_data.get('ownerIds', [])
    if owner_ids:
        for owner_id in owner_ids:
            customer_business_ref = db.collection('customers').document(owner_id).collection('businesses').document(business_id)
            customer_business_snap = customer_business_ref.get()
            if customer_business_snap.exists:
                customer_business_data = customer_business_snap.to_dict()
                print(f"✅ Customer overlay exists for owner: {owner_id}")
                overlay_business_ref = customer_business_data.get('business_ref')
                if overlay_business_ref:
                    if hasattr(overlay_business_ref, 'id'):
                        print(f"   Business Ref ID: {overlay_business_ref.id}")
                    else:
                        print(f"   Business Ref: {overlay_business_ref}")
                print()
            else:
                print(f"⚠️  No customer overlay found for owner: {owner_id}")
                print()
    else:
        print(f"⚠️  Business has no ownerIds listed")
        print()
    
    # 5. Check for hits that might reference this business (via business_ref)
    print("🔍 Checking hits collection...")
    hits_query = db.collection('hits').where('business_ref', '==', business_ref)
    hits = list(hits_query.stream())
    
    if hits:
        print(f"✅ Found {len(hits)} hit(s) referencing this business")
        print(f"   (This means links existed at some point and were clicked)")
        print()
    else:
        print(f"ℹ️  No hits found for this business")
        print()
    
    # 6. Summary and diagnosis
    print(f"\n{'='*80}")
    print("DIAGNOSIS:")
    print(f"{'='*80}\n")
    
    if not links and not targets_found:
        print("❌ ISSUE: Business exists but has NO links AND NO targets")
        print("   This suggests the business was created outside the normal upload flow,")
        print("   or there was an error during upload processing.")
        print("   Possible causes:")
        print("   - Business was created manually or via API")
        print("   - Upload process failed partway through")
        print("   - Business was created but never included in a campaign")
    elif not links and targets_found:
        print("⚠️  ISSUE: Business has targets but NO links")
        print("   This is expected if:")
        print("   - The business was uploaded without a destination URL")
        print("   - The target status is 'excluded' with reason 'No destination'")
        print()
        excluded_targets = [t for t in targets_found if t['status'] == 'excluded']
        if excluded_targets:
            print(f"   ✅ Found {len(excluded_targets)} excluded target(s) - this confirms the issue.")
            print(f"   These targets were created but no links were generated because")
            print(f"   there was no destination URL provided.")
        else:
            print("   ⚠️  Targets exist but are not excluded - this is unexpected.")
            print("   Links should have been created for validated targets.")
    elif links:
        print("✅ Business has links - this should not be an issue")
        print(f"   Found {len(links)} link(s) associated with this business.")
    
    print()


def main():
    parser = argparse.ArgumentParser(description='Debug a business to find why it has no links')
    parser.add_argument('business_id', help='The business ID to debug (e.g., adlatus-gmbh-47798)')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', 
                       help='Environment to use (default: dev)')
    
    args = parser.parse_args()
    
    db = initialize_firestore(args.env)
    debug_business_links(db, args.business_id)


if __name__ == "__main__":
    main()

