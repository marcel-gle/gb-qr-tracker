#!/usr/bin/env python3
"""
Find personalized emails using Snov.io emails-by-domain-by-name endpoint.

This script:
1. Reads a CSV file with first name, last name, and domain information
2. Uses Snov.io API to find personalized emails for each person
3. Writes the found emails to a new column in the CSV

Usage:
    python scripts/find_personalized_emails.py

Environment variables:
    SNOVIO_CLIENT_ID: Your Snov.io client ID
    SNOVIO_CLIENT_SECRET: Your Snov.io client secret
    SNOVIO_INPUT_CSV: Path to input CSV file
    CSV_ENCODING: CSV encoding (default: ISO-8859-15)
"""

import os
import sys
import time
import json
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from dotenv import load_dotenv

import pandas as pd
import requests


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Get OAuth access token for Snov.io.
    """
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    res = requests.post("https://api.snov.io/v1/oauth/access_token", data=params)
    res.raise_for_status()
    data = res.json()
    return data["access_token"]


def extract_domain_from_string(value: str) -> str:
    """
    Extract a bare domain from a URL, email address or raw domain string.
    """
    if not isinstance(value, str):
        return ""
    value = value.strip()
    if not value:
        return ""

    # If it looks like an email address, take part after @
    if "@" in value and " " not in value:
        domain = value.split("@", 1)[1]
    else:
        # Handle URL
        url = value if "://" in value else f"https://{value}"
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]

    if domain.startswith("www."):
        domain = domain[4:]
    return domain.lower()


def infer_domain_for_row(row, domain_col: Optional[str], website_col: Optional[str], email_col: Optional[str]) -> str:
    """
    Determine the best domain for a row using Domain, Website or E-Mail-Adresse.
    Priority: Domain > Website > E-Mail-Adresse.
    """
    for col in (domain_col, website_col, email_col):
        if col and col in row and isinstance(row[col], str) and row[col].strip():
            domain = extract_domain_from_string(row[col])
            if domain:
                return domain
    return ""


def start_email_search(
    rows: List[Dict[str, str]],
    token: str,
    webhook_url: Optional[str] = None
) -> str:
    """
    Start an email search task using Snov.io emails-by-domain-by-name endpoint.
    
    Args:
        rows: List of dicts with 'first_name', 'last_name', 'domain'
        token: Snov.io access token
        webhook_url: Optional webhook URL for instant results
    
    Returns:
        task_hash: Unique ID for the search task
    """
    url = "https://api.snov.io/v2/emails-by-domain-by-name/start"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "rows": rows
    }
    
    if webhook_url:
        payload["webhook_url"] = webhook_url
    
    res = requests.post(url, data=json.dumps(payload), headers=headers)
    res.raise_for_status()
    data = res.json()
    
    task_hash = data["data"]["task_hash"]
    return task_hash


def get_email_search_result(task_hash: str, token: str) -> Dict:
    """
    Get the result of an email search task.
    
    Args:
        task_hash: Unique ID for the search task
        token: Snov.io access token
    
    Returns:
        Response data with status and results
    """
    url = "https://api.snov.io/v2/emails-by-domain-by-name/result"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    params = {
        "task_hash": task_hash
    }
    
    res = requests.get(url, params=params, headers=headers)
    res.raise_for_status()
    return res.json()


def wait_for_results(task_hash: str, token: str, max_wait_time: int = 600, poll_interval: int = 1) -> Dict:
    """
    Poll the result endpoint until the task is completed.
    
    Args:
        task_hash: Unique ID for the search task
        token: Snov.io access token
        max_wait_time: Maximum time to wait in seconds (default: 10 minutes)
        poll_interval: Time between polls in seconds (default: 1 second)
    
    Returns:
        Final result data
    """
    start_time = time.time()
    last_status_print = 0
    
    while True:
        result = get_email_search_result(task_hash, token)
        status = result.get("status", "unknown")
        
        if status == "completed":
            return result
        elif status == "not_enough_credits":
            raise RuntimeError("Not enough credits in Snov.io account")
        elif status == "in_progress":
            elapsed = time.time() - start_time
            if elapsed > max_wait_time:
                raise TimeoutError(f"Task {task_hash} did not complete within {max_wait_time} seconds")
            
            # Only print status every 5 seconds to reduce verbosity
            if elapsed - last_status_print >= 5:
                print(f"  ⏳ Waiting... ({int(elapsed)}s elapsed)")
                last_status_print = elapsed
            
            time.sleep(poll_interval)
        else:
            raise RuntimeError(f"Unknown status: {status}")


def extract_email_from_result(result_data: Dict, first_name: str, last_name: str) -> Optional[str]:
    """
    Extract the best email from the API result for a specific person.
    
    Args:
        result_data: The 'data' array from the API response
        first_name: First name to match
        last_name: Last name to match
    
    Returns:
        Best email address found, or None
    """
    target_name = f"{first_name} {last_name}".strip()
    
    for item in result_data:
        people = item.get("people", "").strip()
        if people.lower() == target_name.lower():
            results = item.get("result", [])
            if results:
                # Prefer valid emails over unknown
                valid_emails = [r for r in results if r.get("smtp_status") == "valid"]
                if valid_emails:
                    return valid_emails[0].get("email")
                # Fall back to unknown status emails
                if results:
                    return results[0].get("email")
    
    return None


def process_csv_with_personalized_emails(
    csv_path: str,
    client_id: str,
    client_secret: str,
    encoding: str = "utf-8",
    delimiter: str = ";",
    domain_column: Optional[str] = "Domain",
    website_column: Optional[str] = "Website",
    email_column: Optional[str] = "E-Mail-Adresse",
    first_name_column: str = "Entscheider 1 Vorname",
    last_name_column: str = "Entscheider 1 Nachname",
    output_column: str = "personalized_email",
    skip_existing: bool = True,
    webhook_url: Optional[str] = None,
    save_interval: int = 10,
) -> None:
    """
    Process CSV file to find personalized emails using Snov.io API.
    Processes one row at a time with 1 second delay between API calls.
    
    Args:
        csv_path: Path to input CSV file
        client_id: Snov.io client ID
        client_secret: Snov.io client secret
        encoding: CSV encoding
        delimiter: CSV delimiter
        domain_column: Column name for domain (optional)
        website_column: Column name for website (optional)
        email_column: Column name for email (optional, used to extract domain)
        first_name_column: Column name for first name
        last_name_column: Column name for last name
        output_column: Name of the new column to write emails to
        skip_existing: Skip rows that already have a personalized_email
        webhook_url: Optional webhook URL for instant results
        save_interval: Save CSV every N rows (default: 10)
    """
    print(f"📖 Loading CSV from {csv_path}")
    df = pd.read_csv(csv_path, encoding=encoding, delimiter=delimiter)
    print(f"✅ Loaded {len(df)} rows")
    
    # Ensure output column exists
    if output_column not in df.columns:
        df[output_column] = ""
    
    # Find the last row with personalized_email and start from the next row
    start_index = 0
    if skip_existing:
        # Find rows that have a non-empty personalized_email
        has_email = df[output_column].notna() & (df[output_column] != "")
        # Convert to string and check if it's not just whitespace
        has_email = has_email & df[output_column].astype(str).str.strip().ne("")
        
        if has_email.any():
            # Find the last index that has an email
            last_index_with_email = df[has_email].index[-1]
            start_index = last_index_with_email + 1
            skipped_count = has_email.sum()
            print(f"⏭️  Found {skipped_count} rows with {output_column}")
            print(f"📍 Last row with email is at index {last_index_with_email}, starting from index {start_index}")
        else:
            print(f"📍 No existing {output_column} found, starting from the beginning")
    
    # Prepare rows for processing starting from start_index
    rows_to_process = []
    
    for i in range(start_index, len(df)):
        row = df.iloc[i]
        
        first_name = str(row.get(first_name_column, "")).strip()
        last_name = str(row.get(last_name_column, "")).strip()
        domain = infer_domain_for_row(row, domain_column, website_column, email_column)
        
        # Only process rows with all required fields
        if first_name and last_name and domain:
            rows_to_process.append({
                "index": i,
                "first_name": first_name,
                "last_name": last_name,
                "domain": domain,
            })
    
    if not rows_to_process:
        print("⚠️  No rows to process. All rows either have emails or are missing required fields.")
        return
    
    print(f"🚀 Processing {len(rows_to_process)} rows (1 per second)")
    
    # Get access token
    print("🔐 Getting access token...")
    token = get_access_token(client_id, client_secret)
    print("✅ Token obtained\n")
    
    # Process one row at a time
    processed = 0
    total_credits_used = 0
    
    for row_num, row_info in enumerate(rows_to_process, 1):
        first_name = row_info["first_name"]
        last_name = row_info["last_name"]
        domain = row_info["domain"]
        df_index = row_info["index"]
        
        print(f"📧 Row {row_num}/{len(rows_to_process)}: {first_name} {last_name} @ {domain}")
        
        # Prepare API request with single row
        api_rows = [{
            "first_name": first_name,
            "last_name": last_name,
            "domain": domain,
        }]
        
        # Track credits: 1 credit per email search
        total_credits_used += 1
        
        try:
            # Start the search
            task_hash = start_email_search(api_rows, token, webhook_url)
            
            # Wait for results
            result = wait_for_results(task_hash, token)
            
            # Extract email and update dataframe
            result_data = result.get("data", [])
            email = extract_email_from_result(result_data, first_name, last_name)
            
            if email:
                df.at[df_index, output_column] = email
                print(f"  ✅ Found: {email}")
            else:
                print(f"  ⚠️  No email found")
            
            processed += 1
            
            # Save progress periodically
            if row_num % save_interval == 0 or row_num == len(rows_to_process):
                df.to_csv(csv_path, index=False, encoding=encoding, sep=delimiter)
                print(f"  💾 Progress saved ({processed}/{len(rows_to_process)} rows processed, {total_credits_used} credits used)")
        
        except Exception as e:
            print(f"  ❌ Error: {e}")
            print(f"  ⚠️  Continuing with next row...")
            continue
        
        # Wait 1 second before next API call (except for the last row)
        if row_num < len(rows_to_process):
            time.sleep(1)
    
    # Final save
    df.to_csv(csv_path, index=False, encoding=encoding, sep=delimiter)
    
    # Summary
    updated_rows = df[output_column].notna() & (df[output_column] != "")
    print(f"\n✅ Processing complete!")
    print(f"   Total rows processed: {processed}")
    print(f"   Rows with personalized emails: {updated_rows.sum()}")
    print(f"   💳 Total credits used: {total_credits_used}")
    print(f"   Final CSV saved to {csv_path}")


if __name__ == "__main__":
    load_dotenv()
    
    # Configuration
    ENCODING = os.getenv("CSV_ENCODING", "ISO-8859-15")
    DELIMITER = os.getenv("CSV_DELIMITER", ";")
    
    client_id = os.getenv("SNOVIO_CLIENT_ID", "")
    client_secret = os.getenv("SNOVIO_CLIENT_SECRET", "")
    
    if not client_id or not client_secret:
        print("❌ Error: SNOVIO_CLIENT_ID and SNOVIO_CLIENT_SECRET must be set in environment or .env file")
        sys.exit(1)
    
    CSV_FILE = os.getenv("SNOVIO_INPUT_CSV", "")
    if not CSV_FILE:
        print("❌ Error: SNOVIO_INPUT_CSV must be set in environment or .env file")
        print("   Example: SNOVIO_INPUT_CSV=/path/to/your/file.csv")
        sys.exit(1)
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ Error: CSV file not found: {CSV_FILE}")
        sys.exit(1)
    
    # Optional webhook URL
    webhook_url = os.getenv("SNOVIO_WEBHOOK_URL", None)
    
    process_csv_with_personalized_emails(
        csv_path=CSV_FILE,
        client_id=client_id,
        client_secret=client_secret,
        encoding=ENCODING,
        delimiter=DELIMITER,
        domain_column="Domain",
        website_column="Website",
        email_column="E-Mail-Adresse",
        first_name_column="Entscheider 1 Vorname",
        last_name_column="Entscheider 1 Nachname",
        output_column="personalized_email",
        skip_existing=True,
        webhook_url=webhook_url,
    )

