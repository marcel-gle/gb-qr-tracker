#!/usr/bin/env python3
"""
Local test script for prospect enrichment function.

This script allows you to test the enrichment function locally without deploying.
It directly calls the process_enrichment function with test data.

SECURITY: This script is hardcoded to use PROJECT_ID=gb-qr-tracker-dev to prevent
accidental execution on production data.

Usage:
    python scripts/test_prospect_enrichment.py <owner_id> <business_id>

Environment Variables Required:
    SNOVIO_CLIENT_ID: Snov.io API client ID (from .env)
    SNOVIO_CLIENT_SECRET: Snov.io API client secret (from .env)
    OPENAI_API_KEY: OpenAI API key (from .env)
    GOOGLE_APPLICATION_CREDENTIALS: Path to service account JSON file (optional, can use hardcoded path)

Example:
    python scripts/test_prospect_enrichment.py owner123 business456
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, Any

# Add functions directory to path to import from main.py
sys.path.insert(0, str(Path(__file__).parent.parent / "functions" / "prospect_enrichment"))

# Try to load from .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load .env from project root
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()  # Try current directory
except ImportError:
    pass

# SECURITY: Force dev project ID - prevent accidental prod usage
PROJECT_ID = "gb-qr-tracker-dev"
DATABASE_ID = "(default)"

# Set environment variables before importing main
os.environ["PROJECT_ID"] = PROJECT_ID
os.environ["DATABASE_ID"] = DATABASE_ID

# Hard-coded service account path (same pattern as other scripts)
# You can override with GOOGLE_APPLICATION_CREDENTIALS env var
SERVICE_ACCOUNT_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"

# Set service account if not already set
if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    if os.path.exists(SERVICE_ACCOUNT_PATH):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    else:
        print("⚠️  Warning: Service account not found at default path.")
        print(f"   Expected: {SERVICE_ACCOUNT_PATH}")
        print("   Set GOOGLE_APPLICATION_CREDENTIALS environment variable if using a different path.")

# Import after setting env vars
from main import process_enrichment, db, SERVER_TIMESTAMP


def print_results(results: Dict[str, Any]) -> None:
    """Pretty print enrichment results."""
    print("\n" + "=" * 60)
    print("ENRICHMENT RESULTS")
    print("=" * 60)
    
    print(f"\n✅ Personal Email: {results.get('personal_email', 'Not found')}")
    print(f"✅ LinkedIn URL: {results.get('linkedin_url', 'Not found')}")
    
    prospect_summary = results.get('prospect_summary')
    if prospect_summary:
        print(f"\n✅ Prospect Summary ({len(prospect_summary)} chars):")
        print(prospect_summary)
    else:
        print("\n❌ Prospect Summary: Not generated")
    
    business_summary = results.get('business_summary')
    if business_summary:
        print(f"\n✅ Business Summary ({len(business_summary)} chars):")
        print(business_summary)
    else:
        print("\n❌ Business Summary: Not generated")
    
    if results.get("errors"):
        print(f"\n⚠️  Errors ({len(results['errors'])}):")
        for error in results["errors"]:
            print(f"   - {error}")
    
    print("\n" + "=" * 60)
    print("API USAGE SUMMARY (local test)")
    print("=" * 60)
    # Get fresh api_usage from main module
    import main
    current_usage = main.api_usage
    
    print(f"\nSnov.io:")
    print(f"  Token requests: {current_usage['snovio']['token_requests']}")
    print(f"  Email searches: {current_usage['snovio']['email_searches']} ({current_usage['snovio']['credits_used']} credits)")
    print(f"  Profile requests: {current_usage['snovio']['profile_requests']}")
    print(f"\nOpenAI:")
    print(f"  Prospect summaries: {current_usage['openai']['prospect_summaries']}")
    print(f"  Business summaries: {current_usage['openai']['business_summaries']}")
    print(f"  Total tokens: {current_usage['openai']['total_tokens']}")
    print(f"    Prompt tokens: {current_usage['openai']['prompt_tokens']}")
    print(f"    Completion tokens: {current_usage['openai']['completion_tokens']}")
    
    # Calculate costs
    from main import OPENAI_INPUT_COST_PER_1K_TOKENS, OPENAI_OUTPUT_COST_PER_1K_TOKENS
    input_cost = (current_usage['openai']['prompt_tokens'] / 1000) * OPENAI_INPUT_COST_PER_1K_TOKENS
    output_cost = (current_usage['openai']['completion_tokens'] / 1000) * OPENAI_OUTPUT_COST_PER_1K_TOKENS
    openai_total_cost = input_cost + output_cost
    
    print(f"\nEstimated Costs:")
    print(f"  Snov.io: {current_usage['snovio']['credits_used']} credits")
    print(f"  OpenAI: ${openai_total_cost:.6f} USD")
    print(f"    Input: ${input_cost:.6f} USD ({current_usage['openai']['prompt_tokens']} tokens)")
    print(f"    Output: ${output_cost:.6f} USD ({current_usage['openai']['completion_tokens']} tokens)")
    print("=" * 60)


def check_environment() -> bool:
    """Check if required environment variables are set."""
    required_vars = [
        "SNOVIO_CLIENT_ID",
        "SNOVIO_CLIENT_SECRET",
        "OPENAI_API_KEY",
    ]
    
    missing = []
    for var in required_vars:
        if not os.environ.get(var):
            missing.append(var)
    
    if missing:
        print("❌ Missing required environment variables:")
        for var in missing:
            print(f"   - {var}")
        print("\nPlease set these in your .env file or export them.")
        print("The script will automatically load from .env if python-dotenv is installed.")
        return False
    
    # Verify we're using dev project
    if os.environ.get("PROJECT_ID") != PROJECT_ID:
        print(f"⚠️  Warning: PROJECT_ID is set to {os.environ.get('PROJECT_ID')}")
        print(f"   This script is hardcoded to use {PROJECT_ID} for security.")
        print(f"   Overriding to {PROJECT_ID}...")
        os.environ["PROJECT_ID"] = PROJECT_ID
    
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("⚠️  Warning: GOOGLE_APPLICATION_CREDENTIALS not set.")
        print("   Firestore operations may fail without service account credentials.")
        return False
    
    return True


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/test_prospect_enrichment.py <owner_id> <business_id>")
        print("\nExample:")
        print("  python scripts/test_prospect_enrichment.py owner123 business456")
        print("\nNote: This script is hardcoded to use PROJECT_ID=gb-qr-tracker-dev for security.")
        sys.exit(1)
    
    owner_id = sys.argv[1]
    business_id = sys.argv[2]
    
    print("=" * 60)
    print("PROSPECT ENRICHMENT - LOCAL TEST")
    print("=" * 60)
    print(f"\n🔒 SECURITY: Using PROJECT_ID={PROJECT_ID} (hardcoded for safety)")
    print(f"\nTesting enrichment for:")
    print(f"  Owner ID: {owner_id}")
    print(f"  Business ID: {business_id}")
    print(f"  Path: customers/{owner_id}/businesses/{business_id}")
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    print("\n" + "-" * 60)
    print("Starting enrichment process...")
    print("-" * 60)
    
    try:
        results = process_enrichment(owner_id, business_id)
        print_results(results)
        
        # Update Firestore document with results (same logic as CloudEvent function)
        print("\n" + "-" * 60)
        print("Updating Firestore document...")
        print("-" * 60)
        
        update_data = {
            "updated_at": SERVER_TIMESTAMP,
        }
        
        if results["personal_email"]:
            update_data["personal_email"] = results["personal_email"]
            print(f"  ✅ Adding personal_email: {results['personal_email']}")
        
        if results["linkedin_url"]:
            update_data["linkedin_url"] = results["linkedin_url"]
            print(f"  ✅ Adding linkedin_url: {results['linkedin_url']}")
        
        if results["prospect_summary"]:
            update_data["prospect_summary"] = results["prospect_summary"]
            print(f"  ✅ Adding prospect_summary ({len(results['prospect_summary'])} chars)")
        
        if results["business_summary"]:
            update_data["business_summary"] = results["business_summary"]
            print(f"  ✅ Adding business_summary ({len(results['business_summary'])} chars)")
        
        # Only set enriched_at if we got at least one result
        if any([results["personal_email"], results["prospect_summary"], results["business_summary"]]):
            update_data["enriched_at"] = SERVER_TIMESTAMP
            print(f"  ✅ Setting enriched_at timestamp")
        
        # Update Firestore document
        if update_data:
            business_ref = db.collection("customers").document(owner_id).collection("businesses").document(business_id)
            business_ref.set(update_data, merge=True)
            print(f"\n✅ Firestore document updated successfully!")
            print(f"   Path: customers/{owner_id}/businesses/{business_id}")
            print(f"   Fields updated: {list(update_data.keys())}")
        else:
            print("\n⚠️  No data to update in Firestore")
        
        # Exit with error code if there were errors
        if results.get("errors"):
            sys.exit(1)
        else:
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

