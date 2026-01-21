#!/usr/bin/env python3
"""
Query and print documents from the Firestore 'links' collection by business_ref.

Usage:
    python query_links_by_business.py [--env dev|prod] [--business-id BUSINESS_ID]

Examples:
    # Query links for a specific business
    python query_links_by_business.py --env dev --business-id abc123
    
    # List all links (without filtering by business)
    python query_links_by_business.py --env dev
"""

import argparse
import json
import sys
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import DocumentReference


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


def serialize_value(value):
    """Recursively serialize a Firestore value to a JSON-serializable format."""
    # Firestore DocumentReference
    if isinstance(value, DocumentReference):
        return f"DocumentReference({value.path})"
    
    # Firestore Timestamp - check by type name to avoid import issues
    if type(value).__name__ in ('DatetimeWithNanoseconds', 'Timestamp'):
        try:
            return value.isoformat()
        except (AttributeError, TypeError):
            return str(value)
    
    # Datetime-like objects (check for isoformat method, but exclude basic types)
    if hasattr(value, 'isoformat') and callable(getattr(value, 'isoformat', None)):
        if not isinstance(value, (str, int, float, bool)):
            try:
                return value.isoformat()
            except (AttributeError, TypeError):
                pass
    
    # Recursive handling for dict and list
    if isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [serialize_value(v) for v in value]
    
    # Default: return as-is (should be JSON-serializable)
    return value


def serialize_document(doc):
    """Convert Firestore document to a serializable dictionary."""
    data = doc.to_dict()
    if not data:
        return {"id": doc.id, "data": None}
    
    serialized = {"id": doc.id}
    
    for key, value in data.items():
        serialized[key] = serialize_value(value)
    
    return serialized


def query_links_by_business(db: firestore.Client, business_id: str = None):
    """Query links collection by business_ref and print results."""
    
    if business_id:
        # Query links for a specific business
        print(f"Querying links for business_id: {business_id}")
        print("=" * 80)
        
        business_ref = db.collection('businesses').document(business_id)
        
        # Verify business exists
        business_snap = business_ref.get()
        if not business_snap.exists:
            print(f"❌ ERROR: Business '{business_id}' does not exist in businesses/ collection")
            return []
        
        business_data = business_snap.to_dict()
        print(f"Business: {business_data.get('business_name', 'N/A')}")
        print()
        
        # Query links by business_ref
        links_query = db.collection('links').where('business_ref', '==', business_ref)
        links = list(links_query.stream())
        
    else:
        # Query all links
        print("Querying all links in collection...")
        print("=" * 80)
        print()
        
        links_query = db.collection('links')
        links = list(links_query.stream())
    
    print(f"Found {len(links)} link document(s)")
    print()
    
    if not links:
        print("No links found.")
        return []
    
    # Print each document
    for i, link_doc in enumerate(links, 1):
        print(f"--- Link {i}/{len(links)} ---")
        link_data = serialize_document(link_doc)
        print(json.dumps(link_data, indent=2, default=str))
        print()
    
    return links


def main():
    parser = argparse.ArgumentParser(
        description='Query and print documents from Firestore links collection by business_ref',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query links for a specific business
  python query_links_by_business.py --env dev --business-id abc123
  
  # List all links (without filtering by business)
  python query_links_by_business.py --env dev
        """
    )
    parser.add_argument('--env', choices=['dev', 'prod'], default='dev',
                       help='Environment to use (default: dev)')
    parser.add_argument('--business-id', type=str, default=None,
                       help='Business ID to filter links by business_ref')
    
    args = parser.parse_args()
    
    db = initialize_firestore(args.env)
    links = query_links_by_business(db, args.business_id)
    
    sys.exit(0)


if __name__ == "__main__":
    main()

