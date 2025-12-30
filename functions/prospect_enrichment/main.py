# main.py
# Cloud Function (Gen 2) for prospect enrichment
# - Triggers on Firestore document creation in "hits" collection
# - Enriches business data with personalized emails, LinkedIn profiles, and summaries
# - Uses Snov.io API for email finding and profile enrichment
# - Uses ChatGPT API for generating prospect and business summaries

import os
import json
import re
import time
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

import functions_framework
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore
from openai import OpenAI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Firestore client
PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT")
DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")
logger.info(f"Initializing Firestore client: project={PROJECT_ID}, database={DATABASE_ID}")
db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)


# Constants
SNOVIO_RATE_LIMIT = 60  # requests per minute
MAX_WAIT_TIME = 600  # 10 minutes max wait for Snov.io results
POLL_INTERVAL = 1  # seconds between polls
CHATGPT_MODEL = "gpt-5-nano"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP

# Initialize OpenAI client
openai_api_key = os.environ.get("OPENAI_API_KEY")
if openai_api_key:
    logger.debug(f"OpenAI client initialized: API key length={len(openai_api_key)}")
    openai_client = OpenAI(api_key=openai_api_key)
else:
    logger.warning("OPENAI_API_KEY not set, ChatGPT features will fail")
    openai_client = None

# Snov.io credentials
SNOVIO_CLIENT_ID = os.environ.get("SNOVIO_CLIENT_ID")
SNOVIO_CLIENT_SECRET = os.environ.get("SNOVIO_CLIENT_SECRET")

if not SNOVIO_CLIENT_ID or not SNOVIO_CLIENT_SECRET:
    logger.warning("SNOVIO_CLIENT_ID or SNOVIO_CLIENT_SECRET not set, Snov.io features will fail")
else:
    logger.debug(f"Snov.io credentials loaded: client_id length={len(SNOVIO_CLIENT_ID)}")

logger.info(f"Function initialized: ChatGPT model={CHATGPT_MODEL}, Snov.io rate limit={SNOVIO_RATE_LIMIT}/min")


# API Usage Tracking
# Cost constants (update based on current pricing)
SNOVIO_EMAIL_SEARCH_COST = 1  # credits per email search
SNOVIO_PROFILE_COST = 1  # credits per profile retrieval (check Snov.io docs for actual cost)

# OpenAI gpt-5-nano pricing (per 1M tokens, converted to per 1K tokens)
# Source: https://platform.openai.com/pricing
OPENAI_INPUT_COST_PER_1K_TOKENS = 0.00005   # $0.05 per 1M tokens = $0.00005 per 1K tokens
OPENAI_OUTPUT_COST_PER_1K_TOKENS = 0.0004   # $0.40 per 1M tokens = $0.0004 per 1K tokens

# Global usage tracker (reset per function invocation)
api_usage = {
    "snovio": {
        "token_requests": 0,
        "email_searches": 0,
        "profile_requests": 0,
        "credits_used": 0,
    },
    "openai": {
        "prospect_summaries": 0,
        "business_summaries": 0,
        "total_tokens": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
    }
}


# ---------------------------
# Snov.io API Functions
# ---------------------------

def get_snovio_token() -> str:
    """Get OAuth access token for Snov.io."""
    logger.debug("Getting Snov.io OAuth token...")
    api_usage["snovio"]["token_requests"] += 1
    
    params = {
        "grant_type": "client_credentials",
        "client_id": SNOVIO_CLIENT_ID,
        "client_secret": SNOVIO_CLIENT_SECRET,
    }
    logger.debug(f"Snov.io token request: client_id={SNOVIO_CLIENT_ID[:8]}... (masked)")
    
    try:
        res = requests.post("https://api.snov.io/v1/oauth/access_token", data=params)
        res.raise_for_status()
        data = res.json()
        token = data["access_token"]
        logger.debug(f"Snov.io token obtained successfully (length: {len(token)})")
        return token
    except Exception as e:
        logger.error(f"Failed to get Snov.io token: {e}", exc_info=True)
        raise


def start_email_search(first_name: str, last_name: str, domain: str, token: str) -> str:
    """
    Start an email search task using Snov.io emails-by-domain-by-name endpoint.
    
    Returns:
        task_hash: Unique ID for the search task
    """
    logger.debug(f"Starting email search: first_name={first_name}, last_name={last_name}, domain={domain}")
    url = "https://api.snov.io/v2/emails-by-domain-by-name/start"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "rows": [{
            "first_name": first_name,
            "last_name": last_name,
            "domain": domain,
        }]
    }
    
    logger.debug(f"Snov.io email search request: {json.dumps(payload)}")
    
    try:
        api_usage["snovio"]["email_searches"] += 1
        api_usage["snovio"]["credits_used"] += SNOVIO_EMAIL_SEARCH_COST
        
        res = requests.post(url, data=json.dumps(payload), headers=headers)
        logger.debug(f"Snov.io response status: {res.status_code}")
        res.raise_for_status()
        data = res.json()
        task_hash = data["data"]["task_hash"]
        logger.debug(f"Email search task started: task_hash={task_hash} (cost: {SNOVIO_EMAIL_SEARCH_COST} credit)")
        return task_hash
    except Exception as e:
        logger.error(f"Failed to start email search: {e}", exc_info=True)
        logger.error(f"Response: {res.text if 'res' in locals() else 'No response'}")
        raise


def get_email_search_result(task_hash: str, token: str) -> Dict:
    """Get the result of an email search task."""
    logger.debug(f"Polling email search result: task_hash={task_hash}")
    url = "https://api.snov.io/v2/emails-by-domain-by-name/result"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    params = {
        "task_hash": task_hash
    }
    
    try:
        res = requests.get(url, params=params, headers=headers)
        logger.debug(f"Poll response status: {res.status_code}")
        res.raise_for_status()
        result = res.json()
        status = result.get("status", "unknown")
        logger.debug(f"Poll result status: {status}")
        logger.info("get_email_search_result raw data: ", result)
        return result
    except Exception as e:
        logger.error(f"Failed to get email search result: {e}", exc_info=True)
        raise


def wait_for_results(task_hash: str, token: str) -> Dict:
    """
    Poll the result endpoint until the task is completed.
    
    Returns:
        Final result data
    """
    start_time = time.time()
    last_status_print = 0
    poll_count = 0
    
    logger.debug(f"Starting to wait for results: task_hash={task_hash}, max_wait={MAX_WAIT_TIME}s")
    
    while True:
        poll_count += 1
        result = get_email_search_result(task_hash, token)
        status = result.get("status", "unknown")
        
        logger.debug(f"Poll #{poll_count}: status={status}")
        
        if status == "completed":
            elapsed = time.time() - start_time
            logger.info(f"Email search completed after {int(elapsed)}s ({poll_count} polls)")
            data_count = len(result.get("data", []))
            logger.debug(f"Result contains {data_count} data items")
            return result
        elif status == "not_enough_credits":
            logger.error("Snov.io account has insufficient credits")
            raise RuntimeError("Not enough credits in Snov.io account")
        elif status == "in_progress":
            elapsed = time.time() - start_time
            if elapsed > MAX_WAIT_TIME:
                logger.error(f"Timeout waiting for results: {elapsed}s > {MAX_WAIT_TIME}s")
                raise TimeoutError(f"Task {task_hash} did not complete within {MAX_WAIT_TIME} seconds")
            
            # Only print status every 5 seconds to reduce verbosity
            if elapsed - last_status_print >= 5:
                logger.info(f"  ⏳ Waiting for Snov.io results... ({int(elapsed)}s elapsed, {poll_count} polls)")
                last_status_print = elapsed
            
            time.sleep(POLL_INTERVAL)
        else:
            logger.error(f"Unknown status from Snov.io: {status}, full response: {json.dumps(result)}")
            raise RuntimeError(f"Unknown status: {status}")


def normalize_name_for_comparison(name: str) -> str:
    """
    Normalize name for comparison (handles umlauts, hyphens, spaces).
    
    Snov.io API normalizes names, so we need to normalize our target name too.
    Example: "Müller-Hartwich" -> "muller hartwich"
    """
    if not name:
        return ""
    
    # Convert to lowercase
    normalized = name.lower()
    
    # Replace umlauts and special characters
    replacements = {
        'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 'ss',
        'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a',
        'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
        'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
        'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o',
        'ù': 'u', 'ú': 'u', 'û': 'u',
        'ç': 'c', 'ñ': 'n',
    }
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    
    # Normalize hyphens and multiple spaces to single space
    normalized = normalized.replace('-', ' ').replace('_', ' ')
    # Collapse multiple spaces
    normalized = ' '.join(normalized.split())
    
    return normalized.strip()


def extract_email_from_result(result_data: Dict, first_name: str, last_name: str) -> Optional[str]:
    """
    Extract the best email from the API result for a specific person.
    
    Returns:
        Best email address found, or None
    """
    target_name = f"{first_name} {last_name}".strip()
    target_normalized = normalize_name_for_comparison(target_name)
    logger.debug(f"Extracting email for: {target_name} (normalized: '{target_normalized}'), from {len(result_data)} result items")
    
    for idx, item in enumerate(result_data):
        people = item.get("people", "").strip()
        people_normalized = normalize_name_for_comparison(people)
        logger.debug(f"Result item {idx}: people='{people}' (normalized: '{people_normalized}')")
        
        # Compare normalized names
        if people_normalized == target_normalized:
            results = item.get("result", [])
            logger.debug(f"Found matching name, {len(results)} email results")
            
            if results:
                # Prefer valid emails over unknown
                valid_emails = [r for r in results if r.get("smtp_status") == "valid"]
                if valid_emails:
                    email = valid_emails[0].get("email")
                    logger.debug(f"Selected valid email: {email}")
                    return email
                # Fall back to unknown status emails
                if results:
                    email = results[0].get("email")
                    smtp_status = results[0].get("smtp_status", "unknown")
                    logger.debug(f"Selected email with status '{smtp_status}': {email}")
                    return email
    
    logger.debug(f"No matching email found for {target_name} (normalized: '{target_normalized}')")
    logger.debug(f"Available names in results: {[item.get('people', '') for item in result_data]}")
    return None


def find_personalized_email(first_name: str, last_name: str, domain: str) -> Optional[str]:
    """
    Find personalized email using Snov.io API.
    
    Returns:
        Email address if found, None otherwise
    """
    logger.info(f"Finding personalized email for {first_name} {last_name} @ {domain}")
    start_time = time.time()
    
    try:
        token = get_snovio_token()
        task_hash = start_email_search(first_name, last_name, domain, token)
        result = wait_for_results(task_hash, token)
        logger.debug(f"Email search API result: {json.dumps(result, default=str)}")
        result_data = result.get("data", [])
        email = extract_email_from_result(result_data, first_name, last_name)
        
        elapsed = time.time() - start_time
        if email:
            logger.info(f"Email found in {elapsed:.1f}s: {email}")
        else:
            logger.warning(f"No email found after {elapsed:.1f}s")
        
        return email
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error finding personalized email after {elapsed:.1f}s: {e}", exc_info=True)
        return None


def get_profile_by_email(email: str) -> Optional[Dict]:
    """
    Get prospect profile from Snov.io using email address.
    
    Returns:
        Profile data with social links and job history, or None
    """
    logger.info(f"Getting profile for email: {email}")
    start_time = time.time()
    
    try:
        token = get_snovio_token()
        url = "https://api.snov.io/v1/get-profile-by-email"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "email": email
        }
        
        logger.debug(f"Snov.io profile request: email={email}")
        
        api_usage["snovio"]["profile_requests"] += 1
        api_usage["snovio"]["credits_used"] += SNOVIO_PROFILE_COST
        
        res = requests.post(url, data=json.dumps(payload), headers=headers)
        logger.debug(f"Profile response status: {res.status_code}")
        res.raise_for_status()
        data = res.json()

        logger.info("get_profile_by_email raw data: ", data)
        elapsed = time.time() - start_time
        
        # Check if we got valid profile data
        # Note: Snov.io API returns profile data at the top level, not nested under "data"
        success = data.get("success", False)
        logger.debug(f"Profile API raw response: {json.dumps(data, default=str)}")
        
        # The profile data is the response itself (excluding the "success" field)
        # Check if we have actual profile fields (name, currentJobs, etc.)
        has_profile_data = bool(data.get("name") or data.get("currentJobs") or data.get("previousJobs") or data.get("social"))
        
        logger.debug(f"Profile API response: success={success}, has_profile_data={has_profile_data} (cost: {SNOVIO_PROFILE_COST} credit)")
        
        if success and has_profile_data:
            # Log profile summary
            current_jobs = len(data.get("currentJobs", []))
            previous_jobs = len(data.get("previousJobs", []))
            social_links = len(data.get("social", []))
            name = data.get("name", "Unknown")
            logger.info(f"Profile retrieved in {elapsed:.1f}s: name={name}, {current_jobs} current jobs, {previous_jobs} previous jobs, {social_links} social links")
            # Return the entire response as profile data (it contains all the fields we need)
            return data
        else:
            logger.warning(f"No profile data returned after {elapsed:.1f}s: success={success}, has_profile_data={has_profile_data}")
            return None
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error getting profile by email after {elapsed:.1f}s: {e}", exc_info=True)
        if 'res' in locals():
            logger.error(f"Response status: {res.status_code}, body: {res.text[:500]}")
        return None


# ---------------------------
# ChatGPT API Functions
# ---------------------------

def generate_prospect_summary(current_jobs: List[Dict], previous_jobs: List[Dict]) -> Optional[str]:
    """
    Generate a brief career summary and timeline from job history.
    
    Returns:
        Summary text, or None if generation fails
    """
    if not openai_client:
        logger.error("OpenAI client not initialized, cannot generate prospect summary")
        return None
    
    logger.debug(f"Generating prospect summary: {len(current_jobs)} current jobs, {len(previous_jobs)} previous jobs")
    start_time = time.time()
    
    try:
        system_prompt = """Du bist ein Assistent, der kurze Karrierezusammenfassungen und Zeitpläne aus Berufshistorie-Daten erstellt.
Erstelle eine prägnante Zusammenfassung (2-5 Sätze) und einen einfachen Zeitplan des Karriereverlaufs der Person.
Konzentriere dich auf Schlüsselrollen, Unternehmen und Karriereentwicklung. Halte es professionell und sachlich.
Antworte ausschließlich auf Deutsch."""

        jobs_data = {
            "currentJobs": current_jobs,
            "previousJobs": previous_jobs
        }
        
        user_prompt = f"""Erstelle basierend auf der folgenden Berufshistorie eine kurze Karrierezusammenfassung und einen Zeitplan:

{json.dumps(jobs_data, indent=2)}

Bitte erstelle eine prägnante Zusammenfassung (2-3 Sätze) gefolgt von einem einfachen Zeitplan des Karriereverlaufs.
Antworte ausschließlich auf Deutsch."""

        logger.debug(f"ChatGPT request: model={CHATGPT_MODEL}, prompt_length={len(user_prompt)}")
        
        response = openai_client.chat.completions.create(
            model=CHATGPT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        
        elapsed = time.time() - start_time
        summary = response.choices[0].message.content
        summary_length = len(summary) if summary else 0
        
        # Track OpenAI usage
        api_usage["openai"]["prospect_summaries"] += 1
        if hasattr(response, 'usage') and response.usage:
            usage = response.usage
            api_usage["openai"]["total_tokens"] += usage.total_tokens
            api_usage["openai"]["prompt_tokens"] += usage.prompt_tokens
            api_usage["openai"]["completion_tokens"] += usage.completion_tokens
            logger.info(f"OpenAI usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = {usage.total_tokens} total tokens")
            logger.debug(f"ChatGPT response received in {elapsed:.1f}s: {summary_length} characters, tokens_used={usage.total_tokens}")
        else:
            logger.debug(f"ChatGPT response received in {elapsed:.1f}s: {summary_length} characters, tokens_used=N/A")
        
        if summary:
            logger.debug(f"Summary preview: {summary[:100]}...")
        
        return summary.strip() if summary else None
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error generating prospect summary after {elapsed:.1f}s: {e}", exc_info=True)
        return None


def fetch_company_homepage(url: str) -> Optional[str]:
    """
    Fetch and extract text content from company homepage.
    
    Returns:
        Extracted text content, or None if fetch fails
    """
    logger.info(f"Fetching company homepage: {url}")
    start_time = time.time()
    
    try:
        # Ensure URL has protocol
        original_url = url
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
            logger.debug(f"Added protocol to URL: {original_url} -> {url}")
        
        # Parse URL to get base domain
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        logger.debug(f"Fetching base URL: {base_url}")
        
        # Fetch homepage
        res = requests.get(base_url, headers={"User-Agent": USER_AGENT}, timeout=10)
        logger.debug(f"HTTP response: status={res.status_code}, content_length={len(res.content)}, content_type={res.headers.get('Content-Type', 'unknown')}")
        res.raise_for_status()
        
        # Parse HTML and extract text
        soup = BeautifulSoup(res.text, "html.parser")
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text content
        text = soup.get_text()
        original_length = len(text)
        logger.debug(f"Extracted text length: {original_length} characters")
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = " ".join(chunk for chunk in chunks if chunk)
        cleaned_length = len(text)
        logger.debug(f"Cleaned text length: {cleaned_length} characters")
        
        # Limit to first 5000 characters to avoid token limits
        final_text = text[:5000] if text else None
        if final_text and len(text) > 5000:
            logger.debug(f"Truncated text from {len(text)} to 5000 characters")
        
        elapsed = time.time() - start_time
        if final_text:
            logger.info(f"Homepage fetched in {elapsed:.1f}s: {len(final_text)} characters extracted")
        else:
            logger.warning(f"No text extracted after {elapsed:.1f}s")
        
        return final_text
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error fetching company homepage after {elapsed:.1f}s: {e}", exc_info=True)
        if 'res' in locals():
            logger.error(f"Response status: {res.status_code if hasattr(res, 'status_code') else 'N/A'}")
        return None


def generate_business_summary(company_url: str, homepage_content: Optional[str]) -> Optional[str]:
    """
    Generate a brief company summary from homepage content.
    
    Returns:
        Summary text, or None if generation fails
    """
    if not openai_client:
        logger.error("OpenAI client not initialized, cannot generate business summary")
        return None
    
    logger.debug(f"Generating business summary for: {company_url}")
    start_time = time.time()
    
    try:
        if not homepage_content:
            logger.warning("No homepage content provided for business summary")
            return None
        
        system_prompt = """Du bist ein Assistent, der kurze Unternehmenszusammenfassungen aus Website-Inhalten erstellt.
Erstelle eine prägnante Zusammenfassung (2-5 Sätze) darüber, was das Unternehmen macht, in welcher Branche es tätig ist und welche Schwerpunkte es hat.
Halte es professionell und sachlich.
Antworte ausschließlich auf Deutsch."""

        content_preview = homepage_content[:3000]
        logger.debug(f"Using {len(content_preview)} characters of homepage content for summary")
        
        user_prompt = f"""Erstelle basierend auf dem folgenden Inhalt der Unternehmens-Startseite eine kurze Unternehmenszusammenfassung:

Unternehmens-URL: {company_url}

Startseiten-Inhalt:
{content_preview}

Bitte erstelle eine prägnante Zusammenfassung (2-3 Sätze) darüber, was dieses Unternehmen macht.
Antworte ausschließlich auf Deutsch."""

        logger.debug(f"ChatGPT request: model={CHATGPT_MODEL}, prompt_length={len(user_prompt)}")
        
        response = openai_client.chat.completions.create(
            model=CHATGPT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        
        elapsed = time.time() - start_time
        summary = response.choices[0].message.content
        summary_length = len(summary) if summary else 0
        
        # Track OpenAI usage
        api_usage["openai"]["business_summaries"] += 1
        if hasattr(response, 'usage') and response.usage:
            usage = response.usage
            api_usage["openai"]["total_tokens"] += usage.total_tokens
            api_usage["openai"]["prompt_tokens"] += usage.prompt_tokens
            api_usage["openai"]["completion_tokens"] += usage.completion_tokens
            logger.info(f"OpenAI usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = {usage.total_tokens} total tokens")
            logger.debug(f"ChatGPT response received in {elapsed:.1f}s: {summary_length} characters, tokens_used={usage.total_tokens}")
        else:
            logger.debug(f"ChatGPT response received in {elapsed:.1f}s: {summary_length} characters, tokens_used=N/A")
        
        if summary:
            logger.debug(f"Summary preview: {summary[:100]}...")
        
        return summary.strip() if summary else None
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error generating business summary after {elapsed:.1f}s: {e}", exc_info=True)
        return None


# ---------------------------
# Helper Functions
# ---------------------------

def extract_domain_from_email(email: str) -> str:
    """Extract domain from email address."""
    if "@" in email:
        return email.split("@", 1)[1].lower()
    return ""


def split_name(name: str) -> tuple:
    """
    Split a full name into first_name and last_name.
    
    Returns:
        (first_name, last_name) tuple
    """
    if not name:
        return "", ""
    
    parts = name.strip().split()
    if len(parts) == 0:
        return "", ""
    elif len(parts) == 1:
        return parts[0], ""
    else:
        # First part is first name, rest is last name
        return parts[0], " ".join(parts[1:])


def extract_linkedin_url(profile_data: Dict) -> Optional[str]:
    """Extract LinkedIn URL from profile social links."""
    social = profile_data.get("social", [])
    for item in social:
        if item.get("type", "").lower() == "linkedin":
            return item.get("link")
    return None


def get_company_url(profile_data: Dict) -> Optional[str]:
    """Extract company URL from current job."""
    current_jobs = profile_data.get("currentJobs", [])
    if current_jobs and len(current_jobs) > 0:
        job = current_jobs[0]
        # Try site first, then socialLink
        site = job.get("site")
        if site:
            return site
        social_link = job.get("socialLink")
        if social_link:
            return social_link
    return None


def get_company_url_from_domain(domain: str) -> str:
    """Construct company URL from domain."""
    # Construct URL from domain (e.g., example.com -> https://example.com)
    if not domain:
        return ""
    # Remove www. if present (already handled in extract_domain_from_email)
    return f"https://{domain}"


# ---------------------------
# Main Processing Function
# ---------------------------

def process_enrichment(owner_id: str, business_id: str) -> Dict[str, Any]:
    """
    Main enrichment processing logic.
    
    Returns:
        Dictionary with enrichment results
    """
    # Reset API usage tracker for this invocation
    global api_usage
    api_usage = {
        "snovio": {
            "token_requests": 0,
            "email_searches": 0,
            "profile_requests": 0,
            "credits_used": 0,
        },
        "openai": {
            "prospect_summaries": 0,
            "business_summaries": 0,
            "total_tokens": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
        }
    }
    
    logger.info(f"=== Starting enrichment process ===")
    logger.info(f"Owner ID: {owner_id}, Business ID: {business_id}")
    process_start_time = time.time()
    
    results = {
        "personal_email": None,
        "linkedin_url": None,
        "prospect_summary": None,
        "business_summary": None,
        "errors": []
    }
    
    try:
        # Read business document
        business_path = f"customers/{owner_id}/businesses/{business_id}"
        logger.debug(f"Reading business document: {business_path}")
        business_ref = db.collection("customers").document(owner_id).collection("businesses").document(business_id)
        business_doc = business_ref.get()
        
        if not business_doc.exists:
            logger.warning(f"Business document not found: {business_path}")
            results["errors"].append("Business document not found")
            return results
        
        business_data = business_doc.to_dict()
        logger.debug(f"Business document fields: {list(business_data.keys())}")
        
        # Extract name and email
        name = business_data.get("name", "").strip()
        email = business_data.get("email", "").strip()
        
        logger.debug(f"Extracted from business doc: name='{name}', email='{email}'")
        
        if not name or not email:
            logger.warning(f"Missing name or email in business document: name={name}, email={email}")
            results["errors"].append("Missing name or email in business document")
            return results
        
        # Split name and extract domain
        first_name, last_name = split_name(name)
        domain = extract_domain_from_email(email)
        
        logger.debug(f"Parsed data: first_name='{first_name}', last_name='{last_name}', domain='{domain}'")
        
        if not first_name or not last_name or not domain:
            logger.warning(f"Insufficient data: first_name={first_name}, last_name={last_name}, domain={domain}")
            results["errors"].append("Insufficient data to process")
            return results
        
        logger.info(f"Processing enrichment for {first_name} {last_name} @ {domain}")
        
        # Step 1: Find personalized email
        logger.info("Step 1: Finding personalized email...")
        personal_email = find_personalized_email(first_name, last_name, domain)
        if personal_email:
            results["personal_email"] = personal_email
            logger.info(f"  ✅ Found email: {personal_email}")
        else:
            logger.warning("  ⚠️  No personalized email found")
            results["errors"].append("No personalized email found")
        
        # Step 2: Get LinkedIn profile and generate prospect summary (only if we have personal email)
        profile_data = None
        if personal_email:
            logger.info("Step 2: Getting LinkedIn profile...")
            profile_data = get_profile_by_email(personal_email)
            
            if profile_data:
                # Extract LinkedIn URL
                linkedin_url = extract_linkedin_url(profile_data)
                if linkedin_url:
                    results["linkedin_url"] = linkedin_url
                    logger.info(f"  ✅ Found LinkedIn: {linkedin_url}")
                
                # Step 3: Generate prospect summary (only if we have profile with job history)
                current_jobs = profile_data.get("currentJobs", [])
                previous_jobs = profile_data.get("previousJobs", [])
                
                if current_jobs or previous_jobs:
                    logger.info("Step 3: Generating prospect summary...")
                    prospect_summary = generate_prospect_summary(current_jobs, previous_jobs)
                    if prospect_summary:
                        results["prospect_summary"] = prospect_summary
                        logger.info("  ✅ Generated prospect summary")
                    else:
                        logger.warning("  ⚠️  Failed to generate prospect summary")
                        results["errors"].append("Failed to generate prospect summary")
                else:
                    logger.warning("  ⚠️  No job history found in profile - skipping prospect summary")
                    results["errors"].append("No job history found")
            else:
                logger.warning("  ⚠️  No profile data found - skipping prospect summary")
                results["errors"].append("No profile data found")
        
        # Step 4: Generate business summary (always attempt, even without personal email)
        logger.info("Step 4: Generating business summary...")
        company_url = None
        
        # Try to get company URL from profile first (if available)
        if profile_data:
            company_url = get_company_url(profile_data)
            if company_url:
                logger.debug(f"Using company URL from profile: {company_url}")
        
        # Fallback: construct URL from domain if no profile URL
        if not company_url:
            company_url = get_company_url_from_domain(domain)
            logger.debug(f"Using company URL from domain: {company_url}")
        
        if company_url:
            homepage_content = fetch_company_homepage(company_url)
            logger.debug(f"Company URL: {company_url}")
            if homepage_content:
                business_summary = generate_business_summary(company_url, homepage_content)
                if business_summary:
                    results["business_summary"] = business_summary
                    logger.info("  ✅ Generated business summary")
                else:
                    logger.warning("  ⚠️  Failed to generate business summary")
                    results["errors"].append("Failed to generate business summary")
            else:
                logger.warning(f"  ⚠️  Failed to fetch company homepage: {company_url}")
                results["errors"].append("Failed to fetch company homepage")
        else:
            logger.warning("  ⚠️  No company URL available (neither from profile nor domain)")
            results["errors"].append("No company URL available")
        
        elapsed = time.time() - process_start_time
        
        # Calculate estimated costs
        # OpenAI costs: input tokens + output tokens (different rates)
        input_cost = (api_usage["openai"]["prompt_tokens"] / 1000) * OPENAI_INPUT_COST_PER_1K_TOKENS
        output_cost = (api_usage["openai"]["completion_tokens"] / 1000) * OPENAI_OUTPUT_COST_PER_1K_TOKENS
        openai_total_cost = input_cost + output_cost
        
        estimated_cost = {
            "snovio_credits": api_usage["snovio"]["credits_used"],
            "openai_cost_usd": openai_total_cost,
            "openai_input_cost_usd": input_cost,
            "openai_output_cost_usd": output_cost,
        }
        
        logger.info(f"=== Enrichment process completed in {elapsed:.1f}s ===")
        logger.info(f"Final results: email={bool(results['personal_email'])}, linkedin={bool(results['linkedin_url'])}, "
                   f"prospect_summary={bool(results['prospect_summary'])}, business_summary={bool(results['business_summary'])}, "
                   f"errors={len(results['errors'])}")
        
        # Log API usage summary
        logger.info("=" * 60)
        logger.info("API USAGE SUMMARY cloud function")
        logger.info("=" * 60)
        logger.info(f"Snov.io:")
        logger.info(f"  Token requests: {api_usage['snovio']['token_requests']}")
        logger.info(f"  Email searches: {api_usage['snovio']['email_searches']} ({api_usage['snovio']['credits_used']} credits)")
        logger.info(f"  Profile requests: {api_usage['snovio']['profile_requests']}")
        logger.info(f"OpenAI:")
        logger.info(f"  Prospect summaries: {api_usage['openai']['prospect_summaries']}")
        logger.info(f"  Business summaries: {api_usage['openai']['business_summaries']}")
        logger.info(f"  Total tokens: {api_usage['openai']['total_tokens']}")
        logger.info(f"    Prompt tokens: {api_usage['openai']['prompt_tokens']}")
        logger.info(f"    Completion tokens: {api_usage['openai']['completion_tokens']}")
        logger.info(f"Estimated costs:")
        logger.info(f"  Snov.io: {estimated_cost['snovio_credits']} credits")
        logger.info(f"  OpenAI: ${estimated_cost['openai_cost_usd']:.6f} USD")
        logger.info(f"    Input: ${estimated_cost['openai_input_cost_usd']:.6f} USD ({api_usage['openai']['prompt_tokens']} tokens)")
        logger.info(f"    Output: ${estimated_cost['openai_output_cost_usd']:.6f} USD ({api_usage['openai']['completion_tokens']} tokens)")
        logger.info("=" * 60)
        
        return results
        
    except Exception as e:
        elapsed = time.time() - process_start_time
        logger.error(f"Error in process_enrichment after {elapsed:.1f}s: {e}", exc_info=True)
        results["errors"].append(f"Processing error: {str(e)}")
        return results


# ---------------------------
# CloudEvent Entry Point
# ---------------------------

@functions_framework.cloud_event
def enrich_prospect(cloud_event):
    """
    Triggered by: google.cloud.firestore.document.v1.created
    Event data shape: https://cloud.google.com/eventarc/docs/cloudevents#firestore
    """
    # Check if function is disabled via environment variable
    if os.environ.get("PROSPECT_ENRICHMENT_DISABLED", "").lower() in ("true", "1", "yes"):
        logger.info("⏭️  Prospect enrichment is disabled via PROSPECT_ENRICHMENT_DISABLED environment variable")
        return
    
    function_start_time = time.time()
    logger.info("=" * 60)
    logger.info("PROSPECT ENRICHMENT FUNCTION TRIGGERED")
    logger.info("=" * 60)
    
    try:
        # Parse Firestore event data
        # For Firestore events via Eventarc, data might be bytes (protobuf) or dict
        event_data = cloud_event.data
        
        # Log event metadata
        event_type = getattr(cloud_event, 'type', 'N/A')
        event_source = getattr(cloud_event, 'source', 'N/A')
        event_subject = getattr(cloud_event, 'subject', 'N/A')
        event_id = getattr(cloud_event, 'id', 'N/A')
        
        logger.info(f"Event type: {event_type}")
        logger.info(f"Event source: {event_source}")
        logger.info(f"Event subject: {event_subject}")
        logger.info(f"Event ID: {event_id}")
        logger.debug(f"Event data type: {type(event_data)}")
        
        # Extract document path from the event
        # For Firestore events via Eventarc, the subject should contain the document path
        hit_id = None
        
        # Method 1: Try to get from subject (format: documents/hits/{hit_id})
        # This is the most reliable method for Firestore events
        if event_subject and event_subject != 'N/A':
            logger.debug(f"Trying to extract from subject: {event_subject}")
            if "/" in event_subject:
                parts = event_subject.split("/")
                if len(parts) >= 3 and parts[0] == "documents" and parts[1] == "hits":
                    hit_id = parts[2]
                    logger.info(f"✅ Extracted hit_id from subject: {hit_id}")
        
        # Method 2: If event_data is bytes (protobuf), try to decode and extract
        if not hit_id:
            if isinstance(event_data, bytes):
                try:
                    # Try to decode as UTF-8 and extract the path
                    # Firestore protobuf data often contains readable document paths
                    decoded = event_data.decode('utf-8', errors='ignore')
                    logger.debug(f"Decoded event data (first 500 chars): {decoded[:500]}")
                    # Look for the document path pattern: documents/hits/{hit_id}
                    match = re.search(r'documents/hits/([a-zA-Z0-9_-]+)', decoded)
                    if match:
                        hit_id = match.group(1)
                        logger.info(f"✅ Extracted hit_id from decoded bytes: {hit_id}")
                except Exception as e:
                    logger.warning(f"Failed to decode event data as UTF-8: {e}")
            
            # Method 3: Try to parse as dict (if it's already JSON)
            if not hit_id and isinstance(event_data, dict):
                logger.debug("Trying to extract from event_data dict")
                # Check for resource path in event data
                resource = event_data.get("resource", "")
                if resource and "hits/" in resource:
                    parts = resource.split("/")
                    if "hits" in parts:
                        idx = parts.index("hits")
                        if idx + 1 < len(parts):
                            hit_id = parts[idx + 1]
                            logger.info(f"✅ Extracted hit_id from resource: {hit_id}")
                
                # Alternative: try from value.name (Firestore document name)
                if not hit_id:
                    value = event_data.get("value", {})
                    if isinstance(value, dict):
                        name = value.get("name", "")
                        if name and "hits/" in name:
                            parts = name.split("/")
                            if "hits" in parts:
                                idx = parts.index("hits")
                                if idx + 1 < len(parts):
                                    hit_id = parts[idx + 1]
                                    logger.info(f"✅ Extracted hit_id from value.name: {hit_id}")
        
        # Method 4: Try to extract from the raw string representation (fallback)
        if not hit_id:
            try:
                event_str = str(event_data)
                logger.debug(f"Trying to extract from string representation (first 500 chars): {event_str[:500]}")
                # Look for the document path in the string
                # Pattern: documents/hits/{document_id}
                match = re.search(r'documents/hits/([a-zA-Z0-9_-]+)', event_str)
                if match:
                    hit_id = match.group(1)
                    logger.info(f"✅ Extracted hit_id from string representation: {hit_id}")
            except Exception as e:
                logger.warning(f"Failed to extract from string representation: {e}")
        
        if not hit_id:
            logger.error("❌ Could not extract hit document ID from event")
            logger.error(f"Event subject: {event_subject}")
            logger.error(f"Event data type: {type(event_data)}")
            if isinstance(event_data, bytes):
                logger.error(f"Event data (first 500 bytes as hex): {event_data[:500].hex()}")
                try:
                    decoded = event_data.decode('utf-8', errors='ignore')
                    logger.error(f"Event data (decoded, first 1000 chars): {decoded[:1000]}")
                except:
                    pass
            else:
                try:
                    logger.error(f"Event data: {json.dumps(event_data, default=str)}")
                except:
                    logger.error(f"Event data (string): {str(event_data)[:1000]}")
            return
        
        logger.info(f"Processing hit document: {hit_id}")
        
        # Read hit document
        hit_ref = db.collection("hits").document(hit_id)
        hit_doc = hit_ref.get()
        
        if not hit_doc.exists:
            logger.warning(f"Hit document not found: {hit_id}")
            return
        
        hit_data = hit_doc.to_dict()
        logger.debug(f"Hit document fields: {list(hit_data.keys())}")
        
        # Skip health check hits
        link_id = hit_data.get("link_id", "")
        user_agent = hit_data.get("user_agent", "")
        is_health_check = (
            link_id.startswith('monitor-test') or
            (user_agent and user_agent.startswith('HealthMonitor/'))
        )
        
        if is_health_check:
            logger.info(f"⏭️  Skipping health check hit: {hit_id} (link_id={link_id}, user_agent={user_agent[:50] if user_agent else 'N/A'})")
            return
        
        # Extract owner_id and business_ref
        owner_id = hit_data.get("owner_id")
        business_ref = hit_data.get("business_ref")
        
        logger.debug(f"Extracted from hit: owner_id={owner_id}, business_ref type={type(business_ref).__name__}")
        
        if not owner_id:
            logger.warning(f"No owner_id in hit document: {hit_id}")
            logger.debug(f"Hit document data: {json.dumps(hit_data, default=str)}")
            return
        
        if not business_ref:
            logger.warning(f"No business_ref in hit document: {hit_id}")
            logger.debug(f"Hit document data: {json.dumps(hit_data, default=str)}")
            return
        
        # Get business_id from business_ref
        # business_ref can be a DocumentReference object or a path string
        business_id = None
        if isinstance(business_ref, firestore.DocumentReference):
            business_id = business_ref.id
        elif hasattr(business_ref, "id"):
            # Handle other reference-like objects
            business_id = business_ref.id
        elif isinstance(business_ref, str):
            # If it's a path string, extract the ID
            # Format could be: "businesses/{id}" or full path
            parts = business_ref.split("/")
            if "businesses" in parts:
                idx = parts.index("businesses")
                if idx + 1 < len(parts):
                    business_id = parts[idx + 1]
            else:
                # Assume last part is the ID
                business_id = parts[-1]
        
        if not business_id:
            logger.warning(f"Could not extract business_id from business_ref: {business_ref} (type: {type(business_ref).__name__})")
            return
        
        logger.info(f"Enriching business: customers/{owner_id}/businesses/{business_id}")
        logger.debug(f"Business reference resolved: business_id={business_id}")
        
        # Check idempotency - skip if already enriched
        logger.debug("Checking idempotency...")
        business_ref_check = db.collection("customers").document(owner_id).collection("businesses").document(business_id)
        business_doc_check = business_ref_check.get()
        
        if business_doc_check.exists:
            business_data_check = business_doc_check.to_dict()
            has_personal_email = bool(business_data_check.get("personal_email"))
            has_prospect_summary = bool(business_data_check.get("prospect_summary"))
            has_business_summary = bool(business_data_check.get("business_summary"))
            
            logger.debug(f"Idempotency check: email={has_personal_email}, prospect_summary={has_prospect_summary}, business_summary={has_business_summary}")
            
            if has_personal_email and has_prospect_summary and has_business_summary:
                logger.info(f"⏭️  Business already enriched, skipping: {business_id}")
                logger.info(f"   Existing data: personal_email={business_data_check.get('personal_email', 'N/A')[:30]}..., "
                           f"linkedin_url={bool(business_data_check.get('linkedin_url'))}, "
                           f"enriched_at={business_data_check.get('enriched_at', 'N/A')}")
                return
            else:
                logger.info(f"Business partially enriched, continuing: email={has_personal_email}, "
                           f"prospect_summary={has_prospect_summary}, business_summary={has_business_summary}")
        
        # Process enrichment
        results = process_enrichment(owner_id, business_id)
        
        # Update business document with results
        update_data = {
            "updated_at": SERVER_TIMESTAMP,
        }
        
        if results["personal_email"]:
            update_data["personal_email"] = results["personal_email"]
        
        if results["linkedin_url"]:
            update_data["linkedin_url"] = results["linkedin_url"]
        
        if results["prospect_summary"]:
            update_data["prospect_summary"] = results["prospect_summary"]
        
        if results["business_summary"]:
            update_data["business_summary"] = results["business_summary"]
        
        # Only set enriched_at if we got at least one result
        if any([results["personal_email"], results["prospect_summary"], results["business_summary"]]):
            update_data["enriched_at"] = SERVER_TIMESTAMP
        
        # Update Firestore document
        logger.info(f"Updating business document with {len(update_data)} fields: {list(update_data.keys())}")
        business_ref_update = db.collection("customers").document(owner_id).collection("businesses").document(business_id)
        business_ref_update.set(update_data, merge=True)
        logger.info(f"✅ Business document updated successfully: customers/{owner_id}/businesses/{business_id}")
        
        # Log what was actually updated
        if results["personal_email"]:
            logger.info(f"   - personal_email: {results['personal_email']}")
        if results["linkedin_url"]:
            logger.info(f"   - linkedin_url: {results['linkedin_url']}")
        if results["prospect_summary"]:
            logger.info(f"   - prospect_summary: {len(results['prospect_summary'])} chars")
        if results["business_summary"]:
            logger.info(f"   - business_summary: {len(results['business_summary'])} chars")
        if "enriched_at" in update_data:
            logger.info(f"   - enriched_at: {update_data.get('enriched_at')}")
        
        function_elapsed = time.time() - function_start_time
        logger.info("=" * 60)
        logger.info(f"✅ ENRICHMENT COMPLETE in {function_elapsed:.1f}s")
        logger.info(f"   Business: {business_id}")
        logger.info(f"   Results: personal_email={bool(results['personal_email'])}, "
                   f"linkedin_url={bool(results['linkedin_url'])}, "
                   f"prospect_summary={bool(results['prospect_summary'])}, "
                   f"business_summary={bool(results['business_summary'])}")
        
        if results["errors"]:
            logger.warning(f"   Errors encountered ({len(results['errors'])}): {results['errors']}")
        
        logger.info("=" * 60)
        
    except Exception as e:
        function_elapsed = time.time() - function_start_time
        logger.error("=" * 60)
        logger.error(f"❌ FUNCTION FAILED after {function_elapsed:.1f}s")
        logger.error(f"Error in enrich_prospect: {e}", exc_info=True)
        logger.error("=" * 60)
        raise

