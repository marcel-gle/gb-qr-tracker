#!/usr/bin/env python3
"""
Fix script to remove incorrect owner from a business.

This script removes an ownerId from a business's ownerIds array and deletes
the corresponding customer overlay when that owner should not have access.

Usage:
    python fix_business_owner.py <business_id> <incorrect_owner_id> [--env dev|prod] [--dry-run]

Example:
    python fix_business_owner.py adlatus-gmbh-47798 xLRk37rnV7T4CbOXzW5N3saxVfy1 --env dev --dry-run
"""

import argparse
import sys
from typing import Optional, Tuple, List
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import ArrayRemove


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


def verify_no_references(db: firestore.Client, business_id: str, owner_id: str) -> Tuple[bool, List[str]]:
    """Verify that the owner has no links, targets, hits, or other references to this business.
    
    This function comprehensively checks the ENTIRE database for any references to this business
    that are owned by or associated with the specified owner_id.
    """
    issues = []
    
    business_ref = db.collection('businesses').document(business_id)
    
    # 1. Check ALL links that reference this business (comprehensive check)
    print(f"   Checking ALL links that reference this business...")
    all_links_query = db.collection('links').where('business_ref', '==', business_ref)
    all_links = list(all_links_query.stream())
    
    owner_links = []
    for link in all_links:
        link_data = link.to_dict()
        link_owner = link_data.get('owner_id')
        if link_owner == owner_id:
            owner_links.append(link)
    
    if owner_links:
        link_ids = [l.id for l in owner_links]
        issues.append(f"Found {len(owner_links)} link(s) owned by {owner_id}: {link_ids}")
        print(f"   ❌ Found {len(owner_links)} link(s) owned by {owner_id}: {', '.join(link_ids)}")
        print(f"      Total links for this business: {len(all_links)}")
    else:
        print(f"   ✅ No links found owned by {owner_id}")
        if all_links:
            print(f"      (Found {len(all_links)} total link(s) for this business, but none owned by {owner_id})")
    
    # 2. Check ALL targets in ALL campaigns that reference this business (comprehensive check)
    print(f"   Checking ALL targets in ALL campaigns that reference this business...")
    all_campaigns = list(db.collection('campaigns').stream())
    print(f"      Scanning {len(all_campaigns)} campaign(s)...")
    
    owner_targets = []
    all_targets_count = 0
    
    for campaign_doc in all_campaigns:
        campaign_data = campaign_doc.to_dict()
        campaign_owner = campaign_data.get('owner_id')
        campaign_id = campaign_doc.id
        campaign_name = campaign_data.get('campaign_name', 'N/A')
        
        targets_ref = campaign_doc.reference.collection('targets')
        targets_query = targets_ref.where('business_ref', '==', business_ref)
        targets = list(targets_query.stream())
        
        if targets:
            all_targets_count += len(targets)
            # Check if this campaign is owned by the owner we're checking
            if campaign_owner == owner_id:
                owner_targets.extend([(campaign_id, campaign_name, t.id) for t in targets])
    
    if owner_targets:
        target_details = [f"campaign:{c} ({name})/target:{t}" for c, name, t in owner_targets]
        issues.append(f"Found {len(owner_targets)} target(s) in campaigns owned by {owner_id}: {target_details}")
        print(f"   ❌ Found {len(owner_targets)} target(s) in campaigns owned by {owner_id}")
        for campaign_id, campaign_name, target_id in owner_targets:
            print(f"      - Campaign: {campaign_name} ({campaign_id}), Target: {target_id}")
    else:
        print(f"   ✅ No targets found in campaigns owned by {owner_id}")
        if all_targets_count > 0:
            print(f"      (Found {all_targets_count} total target(s) for this business across all campaigns)")
    
    # 3. Check ALL hits that reference this business (comprehensive check)
    print(f"   Checking ALL hits that reference this business...")
    all_hits_query = db.collection('hits').where('business_ref', '==', business_ref)
    all_hits = list(all_hits_query.stream())
    
    owner_hits = []
    for hit in all_hits:
        hit_data = hit.to_dict()
        hit_owner = hit_data.get('owner_id')
        if hit_owner == owner_id:
            owner_hits.append(hit)
    
    if owner_hits:
        hit_ids = [h.id for h in owner_hits]
        issues.append(f"Found {len(owner_hits)} hit(s) owned by {owner_id}: {hit_ids[:10]}{'...' if len(hit_ids) > 10 else ''}")
        print(f"   ❌ Found {len(owner_hits)} hit(s) owned by {owner_id}")
        print(f"      Total hits for this business: {len(all_hits)}")
    else:
        print(f"   ✅ No hits found owned by {owner_id}")
        if all_hits:
            print(f"      (Found {len(all_hits)} total hit(s) for this business, but none owned by {owner_id})")
    
    # 4. Check blacklist entries (though these are less critical)
    print(f"   Checking blacklist entries...")
    customer_ref = db.collection('customers').document(owner_id)
    blacklist_ref = customer_ref.collection('blacklist')
    blacklist_entries = list(blacklist_ref.stream())
    
    business_in_blacklist = False
    for entry in blacklist_entries:
        entry_data = entry.to_dict()
        # Check if blacklist entry references this business
        entry_business_ref = entry_data.get('business') or entry_data.get('business_ref')
        entry_business_id = entry_data.get('business_id')
        
        if entry_business_ref:
            if hasattr(entry_business_ref, 'id') and entry_business_ref.id == business_id:
                business_in_blacklist = True
                break
        elif entry_business_id == business_id:
            business_in_blacklist = True
            break
    
    if business_in_blacklist:
        print(f"   ℹ️  Business is in blacklist for this owner (this is OK, can be removed)")
    else:
        print(f"   ✅ Business not in blacklist for this owner")
    
    print()
    print(f"   Summary: Checked {len(all_links)} link(s), {all_targets_count} target(s), {len(all_hits)} hit(s)")
    
    return len(issues) == 0, issues


def fix_business_owner(db: firestore.Client, business_id: str, incorrect_owner_id: str, env: str, dry_run: bool = False):
    """Remove incorrect owner from business and delete their overlay."""
    print(f"\n{'='*80}")
    print(f"FIX BUSINESS OWNER")
    print(f"{'='*80}")
    print(f"Business ID: {business_id}")
    print(f"Owner to remove: {incorrect_owner_id}")
    print(f"Environment: {env.upper()}")
    print(f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE (changes will be applied)'}")
    print(f"{'='*80}\n")
    
    # 1. Verify business exists
    print("STEP 1: Verifying business exists...")
    business_ref = db.collection('businesses').document(business_id)
    business_snap = business_ref.get()
    
    if not business_snap.exists:
        print(f"❌ ERROR: Business '{business_id}' does not exist in businesses/ collection")
        return False
    
    business_data = business_snap.to_dict()
    owner_ids = business_data.get('ownerIds', [])
    business_name = business_data.get('business_name', 'N/A')
    
    print(f"✅ Business found: {business_name}")
    print(f"   Document path: businesses/{business_id}")
    print(f"   Current ownerIds: {owner_ids}")
    print()
    
    if incorrect_owner_id not in owner_ids:
        print(f"⚠️  WARNING: Owner '{incorrect_owner_id}' is not in ownerIds array")
        print(f"   Current ownerIds: {owner_ids}")
        print(f"   Nothing to remove. Exiting.")
        return False
    
    print(f"✅ Owner '{incorrect_owner_id}' found in ownerIds array (position {owner_ids.index(incorrect_owner_id) + 1} of {len(owner_ids)})")
    print()
    
    # 2. Verify no references exist
    print("STEP 2: Verifying no links or targets exist for this owner...")
    safe_to_remove, issues = verify_no_references(db, business_id, incorrect_owner_id)
    print()
    
    if not safe_to_remove:
        print("❌ CANNOT PROCEED: Found references to this business owned by the incorrect owner:")
        print()
        for issue in issues:
            print(f"   • {issue}")
        print()
        print("   ⚠️  Removing this owner would create data inconsistency.")
        print("   Please investigate and resolve these references first.")
        print("   You may need to:")
        print("   - Delete or reassign the links to another owner")
        print("   - Delete or reassign the targets to another owner")
        return False
    
    print("✅ Verification passed: No links or targets found for this owner")
    print()
    
    # 3. Check customer overlay exists
    print("STEP 3: Checking customer business overlay...")
    customer_business_ref = db.collection('customers').document(incorrect_owner_id).collection('businesses').document(business_id)
    customer_business_snap = customer_business_ref.get()
    
    overlay_exists = customer_business_snap.exists
    if overlay_exists:
        overlay_data = customer_business_snap.to_dict()
        print(f"✅ Customer overlay exists")
        print(f"   Document path: customers/{incorrect_owner_id}/businesses/{business_id}")
        print(f"   Has business_ref: {overlay_data.get('business_ref') is not None}")
        print(f"   Hit count: {overlay_data.get('hit_count', 0)}")
        print(f"   Last hit: {overlay_data.get('last_hit_at', 'Never')}")
    else:
        print(f"ℹ️  Customer overlay does not exist")
        print(f"   Document path: customers/{incorrect_owner_id}/businesses/{business_id}")
        print(f"   (Nothing to delete)")
    print()
    
    # 4. Summary of what will be done
    print("STEP 4: Summary of changes...")
    print()
    print("The following changes will be made:")
    print()
    print(f"   1. Remove '{incorrect_owner_id}' from business ownerIds array")
    print(f"      Current: {owner_ids}")
    new_owner_ids = [oid for oid in owner_ids if oid != incorrect_owner_id]
    print(f"      After:   {new_owner_ids}")
    print()
    if overlay_exists:
        print(f"   2. Delete customer overlay document")
        print(f"      Path: customers/{incorrect_owner_id}/businesses/{business_id}")
    else:
        print(f"   2. No customer overlay to delete (does not exist)")
    print()
    
    # 5. Perform fixes
    if dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No changes have been made")
        print("=" * 80)
        print()
        print("To apply these changes, run the script again without --dry-run flag:")
        print()
        print(f"   python scripts/fix_business_owner.py {business_id} {incorrect_owner_id} --env {env}")
        print()
        return True
    
    print("STEP 5: Applying fixes...")
    print()
    
    try:
        # Remove ownerId from array
        print(f"   Removing '{incorrect_owner_id}' from ownerIds array...")
        business_ref.update({"ownerIds": ArrayRemove([incorrect_owner_id])})
        print(f"   ✅ Successfully removed from ownerIds array")
        
        # Delete customer overlay if it exists
        if overlay_exists:
            print(f"   Deleting customer overlay document...")
            customer_business_ref.delete()
            print(f"   ✅ Successfully deleted customer overlay")
        else:
            print(f"   ℹ️  Skipping overlay deletion (does not exist)")
        
        print()
        
        # Verify the fix
        print("STEP 6: Verifying changes...")
        updated_snap = business_ref.get()
        updated_data = updated_snap.to_dict()
        updated_owner_ids = updated_data.get('ownerIds', [])
        
        overlay_check = customer_business_ref.get()
        overlay_still_exists = overlay_check.exists
        
        print()
        if incorrect_owner_id not in updated_owner_ids and not overlay_still_exists:
            print("✅ Fix completed successfully!")
            print()
            print("Verification results:")
            print(f"   • Owner '{incorrect_owner_id}' removed from ownerIds: ✅")
            print(f"     Before: {owner_ids}")
            print(f"     After:  {updated_owner_ids}")
            if overlay_exists:
                print(f"   • Customer overlay deleted: ✅")
            else:
                print(f"   • Customer overlay (did not exist): ✅")
            print()
            print("The business now only belongs to the correct owner(s).")
        else:
            print("⚠️  WARNING: Verification found issues:")
            if incorrect_owner_id in updated_owner_ids:
                print(f"   • Owner '{incorrect_owner_id}' still in ownerIds array")
            if overlay_still_exists:
                print(f"   • Customer overlay still exists")
        
        return True
        
    except Exception as e:
        print()
        print("❌ ERROR: Failed to apply fixes")
        print(f"   Error: {e}")
        print()
        print("   The operation may have been partially completed.")
        print("   Please check the database state and retry if necessary.")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Remove incorrect owner from a business and delete their overlay',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be changed (recommended first step)
  python fix_business_owner.py adlatus-gmbh-47798 xLRk37rnV7T4CbOXzW5N3saxVfy1 --env dev --dry-run
  
  # Actually apply the fix (after reviewing dry run output)
  python fix_business_owner.py adlatus-gmbh-47798 xLRk37rnV7T4CbOXzW5N3saxVfy1 --env dev
  
  # For production environment
  python fix_business_owner.py adlatus-gmbh-47798 xLRk37rnV7T4CbOXzW5N3saxVfy1 --env prod --dry-run
        """
    )
    parser.add_argument('business_id', help='The business ID to fix (e.g., adlatus-gmbh-47798)')
    parser.add_argument('incorrect_owner_id', help='The owner ID to remove (e.g., xLRk37rnV7T4CbOXzW5N3saxVfy1)')
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev', 
                       help='Environment to use (default: dev)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be changed without making changes (recommended first step)')
    
    args = parser.parse_args()
    
    db = initialize_firestore(args.env)
    success = fix_business_owner(db, args.business_id, args.incorrect_owner_id, args.env, args.dry_run)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

