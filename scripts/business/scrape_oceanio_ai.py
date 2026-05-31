"""
This script is used to scrape the imprint data from the website and enrich the CSV file from oceanio.com with the data.
It uses the OpenAI API to extract the data.
"""

import os
import csv
import time
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore  # <-- uses Semaphore

import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from tqdm import tqdm

from imprint_md_extract import (
    extract_managing_director_from_imprint_plaintext,
    gpt_managing_directors_is_empty,
)

# ---------------- Load environment variables from .env file ----------------

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

# ---------------- OpenAI client ----------------

# Loads OPENAI_API_KEY from environment or .env file
client = OpenAI()

OPENAI_MODEL = "gpt-5-mini"  # use the model name you want to call

# ---------------- Local ML Studio client for parsing ----------------

# ML Studio typically runs on localhost:1234/v1
# Can be overridden via ML_STUDIO_BASE_URL environment variable
ML_STUDIO_BASE_URL = os.environ.get("ML_STUDIO_BASE_URL", "http://localhost:1234/v1")
LOCAL_MODEL = "openai/gpt-oss-20b"  # ML Studio model name

# Initialize local model client (no API key needed for local models)
local_client = OpenAI(
    base_url=ML_STUDIO_BASE_URL,
    api_key="not-needed"  # ML Studio doesn't require a real API key
)


# ---------------- HTTP / scraping helpers ----------------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10
DELAY_BETWEEN_REQUESTS = 1.0  # seconds between domains (only used if not parallelizing)
MAX_WORKERS_HTTP = 10  # concurrent HTTP requests
MAX_WORKERS_GPT = 5  # concurrent GPT API calls (be mindful of rate limits)

# Enforce GPT concurrency limit
GPT_SEMAPHORE = Semaphore(MAX_WORKERS_GPT)


def fetch_url(url: str) -> Optional[requests.Response]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            return resp
    except requests.RequestException:
        return None
    return None


def best_base_url(domain: str) -> Optional[str]:
    """
    Normalize a domain (which may already include scheme/path) and
    return the first reachable base URL (https or http).
    """
    domain = domain.strip()
    if not domain:
        return None

    # If domain already includes a scheme, strip it
    if domain.startswith("http://"):
        domain = domain[len("http://") :]
    elif domain.startswith("https://"):
        domain = domain[len("https://") :]

    # Strip everything after the host (paths, query, etc.)
    # e.g. "www.example.com/impressum/" -> "www.example.com"
    domain = domain.split("/")[0].rstrip("/")

    # Try https first, then http
    for scheme in ("https://", "http://"):
        url = scheme + domain
        resp = fetch_url(url)
        if resp:
            return resp.url
    return None


def find_imprint_url(base_html: str, base_url: str) -> Optional[str]:
    """
    Find an Impressum/Imprint/Kontakt link.
    """
    soup = BeautifulSoup(base_html, "html.parser")

    # 1) Look for links whose text or href suggests "impressum" or "imprint"
    candidates = []
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"].lower()
        if any(key in text for key in ["impressum", "imprint"]) or any(
            key in href for key in ["impressum", "imprint"]
        ):
            candidates.append(a["href"])

    for href in candidates:
        url = urljoin(base_url, href)
        resp = fetch_url(url)
        if resp:
            return resp.url

    # 2) Try common fallback paths
    common_paths = [
        "/impressum",
        "/impressum/",
        "/impressum.html",
        "/imprint",
        "/imprint/",
        "/imprint.html",
        "/kontakt",
        "/kontakt/",
        "/kontakt/impressum/",
        "/kontakt/impressum"
    ]
    for path in common_paths:
        url = urljoin(base_url, path)
        resp = fetch_url(url)
        if resp:
            return resp.url

    return None


def extract_text_from_url(url: str) -> Optional[str]:
    resp = fetch_url(url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(separator="\n")
    # Minimize token usage: trim very long texts
    max_chars = 15000
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def get_imprint_text_for_domain(domain: str) -> Optional[str]:
    base_url = best_base_url(domain)
    if not base_url:
        print(f"  ⚠ Could not reach base URL for domain: {domain}")
        return None

    print(f"  Base URL: {base_url}")
    home_resp = fetch_url(base_url)
    if not home_resp:
        print(f"  ⚠ Could not fetch homepage for: {domain}")
        return None

    imprint_url = find_imprint_url(home_resp.text, base_url)
    if imprint_url:
        print(f"  Imprint URL: {imprint_url}")
        return extract_text_from_url(imprint_url)
    else:
        print("  ⚠ No imprint URL found, using homepage text as fallback.")
        soup = BeautifulSoup(home_resp.text, "html.parser")
        text = soup.get_text(separator="\n")
        max_chars = 15000
        if len(text) > max_chars:
            text = text[:max_chars]
        return text


# ---------------- GPT call ----------------

SYSTEM_PROMPT = """
You are an assistant that extracts structured company data from German "Impressum" (imprint) pages.

Your job:
- Read the given text from a German company's website (usually the Impressum / Kontakt page).
- Identify and extract:
  - The company's full postal address, split into structured components (street, house number, postal code, city).
  - The managing director(s) / legal representatives. CRITICAL: For each managing director, you MUST extract:
    * First name (Vorname) - the person's given name
    * Last name (Nachname) - the person's family name/surname
    * Gender/Salutation - determine if it's "Herr" (male) or "Frau" (female) based on the first name or explicit salutation in the text
    * Full name - the complete name as it appears in the text
    These fields will be used to populate separate CSV columns (Vorname, Nachname, Salutation).
  - The full legal company name as written in the imprint (including GmbH, UG, AG, KG, etc.).
  - Generic company phone numbers (main switchboard, office numbers; ignore obviously private mobiles if clearly marked as personal).
  - Generic company email addresses (like info@, kontakt@, office@; also include named emails if they are clearly business emails in the imprint).

Output rules:
- Always respond with a single valid JSON object only, no explanation text.
- Use this exact JSON structure and keys:

{
  "full_address": "string or null",
  "address_street": "string or null",
  "address_house_number": "string or null",
  "address_postcode": "string or null",
  "address_city": "string or null",
  "managing_directors": [
    {
      "first_name": "string or null",
      "last_name": "string or null",
      "gender": "Herr" or "Frau" or null,
      "full_name": "string or null"
    }
  ],
  "company_legal_name": "string or null",
  "generic_company_phones": ["+49 ...", "..."],
  "generic_company_emails": ["info@example.com", "..."],
  "confidence": 0.0
}

Notes:
- "full_address" should be one line, including street, house number, postal code, city, country if available (for backward compatibility).
- "address_street" should contain only the street name (e.g., "Beethovenstr.", "Lange Gasse").
- "address_house_number" should contain only the house number (e.g., "4", "19", "13a").
- "address_postcode" should contain only the postal code (e.g., "86368", "85139").
- "address_city" should contain only the city name (e.g., "Gersthofen", "Wettstetten").
- For managing_directors: This is CRITICAL for populating separate CSV columns (Vorname, Nachname, Salutation).
  * "first_name" (Vorname): Extract the person's given/first name. This is essential and should be extracted whenever possible.
  * "last_name" (Nachname): Extract the person's family name/surname. This is essential and should be extracted whenever possible.
  * "gender" (Salutation): Determine gender based on the first name (use "Herr" for male, "Frau" for female). If the text already contains "Herr" or "Frau", use that explicitly. If gender cannot be determined with reasonable confidence, set to null.
  * "full_name": Should contain the complete name as it appears in the text (for reference and fallback).
  * Always try to split names into first_name and last_name. Common German name patterns: "Max Mustermann" (first: "Max", last: "Mustermann"), "Herr Thomas Herrmann" (first: "Thomas", last: "Herrmann", gender: "Herr").
  * If there are multiple managing directors, include all of them in the array. The first one in the array will be used for the separate CSV columns.
  * Headings often split label and name across lines (e.g. a line or heading "Geschäftsführer:" with the person's name on the following line or in the next paragraph). Treat the following line as the name when it clearly looks like a person and not a new section (Kontakt, Telefon, Register, etc.).
- If you are not sure about a field, set it to null or an empty list.
- Only include information that appears in the text; do not invent data.
- If there are multiple possible addresses, choose the one that most likely represents the head office / main business location.
""".strip()


def call_gpt_for_imprint(
    domain: str, company_name: str, imprint_text: str
) -> Dict[str, Any]:
    user_prompt = f"""
Extract company data from this website content.

Domain: {domain}
CRM company name: {company_name}

Text from Impressum / Kontakt page:
\"\"\" 
{imprint_text}
\"\"\"

Remember: respond with a single JSON object only, using the exact schema described in the system prompt.
""".strip()

    # Enforce GPT concurrency limit via semaphore
    with GPT_SEMAPHORE:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            temperature=1,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )

    content = response.choices[0].message.content
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # Fallback: wrap in minimal structure so we don't crash
        print("  ⚠ GPT response was not valid JSON, raw content:")
        print(content)
        data = {
            "full_address": None,
            "address_street": None,
            "address_house_number": None,
            "address_postcode": None,
            "address_city": None,
            "managing_directors": [],
            "company_legal_name": None,
            "generic_company_phones": [],
            "generic_company_emails": [],
            "confidence": 0.0,
        }
    return data


# ---------------- GPT parsing for existing data ----------------

PARSE_ADDRESS_SYSTEM_PROMPT = """
You are an assistant that parses German address strings into structured components.

Your job:
- Take a German address string (which may be incomplete or in various formats)
- Extract and return structured components: street name, house number, postal code, and city
- Handle common German address formats and variations

Output rules:
- Always respond with a single valid JSON object only, no explanation text.
- Use this exact JSON structure:

{
  "street": "string or null",
  "house_number": "string or null",
  "postcode": "string or null",
  "city": "string or null"
}

Notes:
- "street" should contain only the street name (e.g., "Beethovenstr.", "Lange Gasse", "Maaßenstraße")
- "house_number" should contain only the house number (e.g., "4", "19", "13a", "19-21")
- "postcode" should contain only the 5-digit postal code (e.g., "86368", "85139", "10777")
- "city" should contain only the city name (e.g., "Gersthofen", "Wettstetten", "Berlin")
- If a component cannot be determined, set it to null
- Do not include country names in the city field
- Handle addresses with or without commas, with various separators
""".strip()


PARSE_NAME_SYSTEM_PROMPT = """
You are an assistant that parses German person names into structured components.

Your job:
- Take a German person name string (which may include titles, salutations, or be in various formats)
- Extract and return: first name (Vorname), last name (Nachname), and salutation (Herr/Frau)
- Handle common German name formats and variations

Output rules:
- Always respond with a single valid JSON object only, no explanation text.
- Use this exact JSON structure:

{
  "vorname": "string or null",
  "nachname": "string or null",
  "salutation": "Herr" or "Frau" or null
}

Notes:
- "vorname" (first name): Extract the person's given/first name
- "nachname" (last name): Extract the person's family name/surname
- "salutation": Determine if it's "Herr" (male) or "Frau" (female) based on:
  * Explicit salutation in the text ("Herr", "Frau")
  * First name gender patterns (if no explicit salutation)
  * Set to null if gender cannot be determined
- Handle formats like:
  * "Herr Max Mustermann" → vorname: "Max", nachname: "Mustermann", salutation: "Herr"
  * "Frau Anna Schmidt" → vorname: "Anna", nachname: "Schmidt", salutation: "Frau"
  * "Dr. Marcus Bysikiewicz" → vorname: "Marcus", nachname: "Bysikiewicz", salutation: null
  * "Antje Seidel" → vorname: "Antje", nachname: "Seidel", salutation: null (or "Frau" if name suggests female)
  * "Haselhorst, Kathola" → vorname: "Kathola", nachname: "Haselhorst", salutation: null
- Remove titles like "Dr.", "Prof.", "Prof. Dr." before parsing
- If only one name part is available, prefer it as the last name
""".strip()


def call_local_model_for_address_parsing(full_address: str) -> Dict[str, Optional[str]]:
    """
    Use local ML Studio model to parse a full address string into structured components.
    This is a lightweight call that only parses the provided string.
    """
    user_prompt = f"""
Parse this German address string into structured components:

Address: "{full_address}"

Remember: respond with a single JSON object only, using the exact schema described in the system prompt.
""".strip()

    # Enforce GPT concurrency limit via semaphore
    content = None
    with GPT_SEMAPHORE:
        try:
            response = local_client.chat.completions.create(
                model=LOCAL_MODEL,
                temperature=0.3,  # Lower temperature for more consistent parsing
                messages=[
                    {"role": "system", "content": PARSE_ADDRESS_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )

            content = response.choices[0].message.content
            print(f"  [DEBUG] Local model address parsing response: {content[:200]}...")  # Print first 200 chars
            
            # Try to extract JSON from the response (might be wrapped in markdown code blocks)
            content_clean = content.strip()
            
            # Remove markdown code blocks if present
            if content_clean.startswith("```json"):
                content_clean = content_clean[7:]  # Remove ```json
            elif content_clean.startswith("```"):
                content_clean = content_clean[3:]  # Remove ```
            
            if content_clean.endswith("```"):
                content_clean = content_clean[:-3]  # Remove closing ```
            
            content_clean = content_clean.strip()
            
            # Try to find JSON object in the response (handle nested objects)
            # Look for opening brace followed by content including "street" field
            brace_count = 0
            start_idx = content_clean.find('{')
            if start_idx != -1:
                for i in range(start_idx, len(content_clean)):
                    if content_clean[i] == '{':
                        brace_count += 1
                    elif content_clean[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            # Found complete JSON object
                            content_clean = content_clean[start_idx:i+1]
                            break
            
            data = json.loads(content_clean)
            return {
                "street": data.get("street"),
                "house_number": data.get("house_number"),
                "postcode": data.get("postcode"),
                "city": data.get("city")
            }
        except json.JSONDecodeError as e:
            print(f"  ⚠ Local model address parsing failed (JSON decode error): {e}")
            if content:
                print(f"  [DEBUG] Raw response was: {content[:500]}")
            return {
                "street": None,
                "house_number": None,
                "postcode": None,
                "city": None
            }
        except Exception as e:
            print(f"  ⚠ Local model address parsing failed: {e}")
            print(f"  [DEBUG] Error type: {type(e).__name__}")
            if content:
                print(f"  [DEBUG] Response content: {content[:500]}")
            if hasattr(e, 'response'):
                print(f"  [DEBUG] Error response: {e.response}")
            return {
                "street": None,
                "house_number": None,
                "postcode": None,
                "city": None
            }


def call_gpt_for_address_parsing(full_address: str) -> Dict[str, Optional[str]]:
    """
    Legacy function name - now uses local model.
    Use local ML Studio model to parse a full address string into structured components.
    """
    return call_local_model_for_address_parsing(full_address)


def call_local_model_for_name_parsing(md_string: str) -> Dict[str, Optional[str]]:
    """
    Use local ML Studio model to parse a managing director name string into structured components.
    This is a lightweight call that only parses the provided string.
    """
    user_prompt = f"""
Parse this German person name into structured components:

Name: "{md_string}"

Remember: respond with a single JSON object only, using the exact schema described in the system prompt.
""".strip()

    # Enforce GPT concurrency limit via semaphore
    content = None
    with GPT_SEMAPHORE:
        try:
            response = local_client.chat.completions.create(
                model=LOCAL_MODEL,
                temperature=0.3,  # Lower temperature for more consistent parsing
                messages=[
                    {"role": "system", "content": PARSE_NAME_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )

            content = response.choices[0].message.content
            print(f"  [DEBUG] Local model name parsing response: {content[:200]}...")  # Print first 200 chars
            
            # Try to extract JSON from the response (might be wrapped in markdown code blocks)
            content_clean = content.strip()
            
            # Remove markdown code blocks if present
            if content_clean.startswith("```json"):
                content_clean = content_clean[7:]  # Remove ```json
            elif content_clean.startswith("```"):
                content_clean = content_clean[3:]  # Remove ```
            
            if content_clean.endswith("```"):
                content_clean = content_clean[:-3]  # Remove closing ```
            
            content_clean = content_clean.strip()
            
            # Try to find JSON object in the response (handle nested objects)
            # Look for opening brace followed by content including "vorname" field
            brace_count = 0
            start_idx = content_clean.find('{')
            if start_idx != -1:
                for i in range(start_idx, len(content_clean)):
                    if content_clean[i] == '{':
                        brace_count += 1
                    elif content_clean[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            # Found complete JSON object
                            content_clean = content_clean[start_idx:i+1]
                            break
            
            data = json.loads(content_clean)
            return {
                "vorname": data.get("vorname"),
                "nachname": data.get("nachname"),
                "salutation": data.get("salutation")
            }
        except json.JSONDecodeError as e:
            print(f"  ⚠ Local model name parsing failed (JSON decode error): {e}")
            if content:
                print(f"  [DEBUG] Raw response was: {content[:500]}")
            return {
                "vorname": None,
                "nachname": None,
                "salutation": None
            }
        except Exception as e:
            print(f"  ⚠ Local model name parsing failed: {e}")
            print(f"  [DEBUG] Error type: {type(e).__name__}")
            if content:
                print(f"  [DEBUG] Response content: {content[:500]}")
            if hasattr(e, 'response'):
                print(f"  [DEBUG] Error response: {e.response}")
            return {
                "vorname": None,
                "nachname": None,
                "salutation": None
            }


def call_gpt_for_name_parsing(md_string: str) -> Dict[str, Optional[str]]:
    """
    Legacy function name - now uses local model.
    Use local ML Studio model to parse a managing director name string into structured components.
    """
    return call_local_model_for_name_parsing(md_string)


# ---------------- CSV enrichment logic ----------------

COL_COMPANY = "Company"
COL_DOMAIN = "Domain"
COL_PHONE = "Generic Company Phones"
COL_EMAIL = "Generic Company Emails"
COL_ADDRESS = "Headquarter Raw Address"
COL_ADDRESS_STREET = "Street"
COL_ADDRESS_HOUSE_NUMBER = "House Number"
COL_ADDRESS_POSTCODE = "Postcode"
COL_ADDRESS_CITY = "City"
COL_MD = "Imprint: Managing director"
COL_MD_VORNAME = "Vorname"
COL_MD_NACHNAME = "Nachname"
COL_MD_SALUTATION = "Salutation"
COL_LEGAL_NAME = "Imprint: Company legal name"


def _normalize_header_name(value: Optional[str]) -> str:
    """Normalize CSV header names for resilient matching."""
    if value is None:
        return ""
    # Handle BOM and accidental surrounding whitespace.
    return str(value).replace("\ufeff", "").strip().lower()


def _extract_domain_from_email_value(value: Optional[str]) -> str:
    """
    Extract a usable domain from an email field.

    Supports plain email addresses, mailto: links, and comma/semicolon-separated
    values by using the first valid email-like token it finds.
    """
    raw = (value or "").strip()
    if not raw:
        return ""

    candidate = re.split(r"[;,]", raw, maxsplit=1)[0].strip()
    if candidate.lower().startswith("mailto:"):
        candidate = candidate[7:].strip()

    match = re.search(r"@([A-Z0-9.\-]+\.[A-Z]{2,})", candidate, flags=re.IGNORECASE)
    if not match:
        return ""

    domain = match.group(1).strip().strip(" >)\"'").lower().rstrip(".")
    return domain


def address_incomplete(addr: str) -> bool:
    """
    Very simple heuristic: treat short or non-specific addresses as incomplete.
    """
    if not addr:
        return True
    addr = addr.strip()
    if len(addr) < 15:
        return True
    # if it has no digits, probably missing house number or ZIP
    if not any(ch.isdigit() for ch in addr):
        return True
    return False


def parse_address_into_components(full_address: str) -> Dict[str, Optional[str]]:
    """
    Parse a full address string into structured components using local ML Studio model.
    Returns dict with keys: street, house_number, postcode, city
    """
    if not full_address or len(full_address.strip()) < 10:
        return {
            "street": None,
            "house_number": None,
            "postcode": None,
            "city": None
        }
    
    # Use local model for parsing
    return call_local_model_for_address_parsing(full_address)


def parse_managing_director_name(md_string: str) -> Dict[str, Optional[str]]:
    """
    Parse managing director name string into Vorname, Nachname, Salutation using local ML Studio model.
    Returns dict with keys: vorname, nachname, salutation
    """
    if not md_string or not md_string.strip():
        return {
            "vorname": None,
            "nachname": None,
            "salutation": None
        }
    
    # Use local model for parsing
    return call_local_model_for_name_parsing(md_string)


def process_row(
    row: Dict[str, str],
    row_idx: int,
    total_rows: int,
    imprint_text_cache: Dict[str, Optional[str]],
    domain_cache: Dict[str, Dict[str, Any]],
    imprint_fetching: set,
    domain_fetching: set,
    imprint_lock: Lock,
    domain_lock: Lock,
) -> Tuple[int, Dict[str, str], bool]:
    """
    Process a single row. Returns (row_idx, updated_row, was_updated).
    Thread-safe caching is handled via locks.
    """
    domain = (row.get(COL_DOMAIN) or "").strip()
    company = (row.get(COL_COMPANY) or "").strip()
    email = (row.get(COL_EMAIL) or "").strip()

    if not domain and email:
        domain = _extract_domain_from_email_value(email)
        if domain:
            row = row.copy()
            row[COL_DOMAIN] = domain

    print(f"\n[{row_idx}/{total_rows}] {company} — {domain}")

    if not domain:
        print("  ⚠ No domain → skipping row.")
        return (row_idx, row, False)

    # existing values
    addr = (row.get(COL_ADDRESS) or "").strip()
    addr_street = (row.get(COL_ADDRESS_STREET) or "").strip()
    addr_house = (row.get(COL_ADDRESS_HOUSE_NUMBER) or "").strip()
    addr_postcode = (row.get(COL_ADDRESS_POSTCODE) or "").strip()
    addr_city = (row.get(COL_ADDRESS_CITY) or "").strip()
    md = (row.get(COL_MD) or "").strip()
    md_vorname = (row.get(COL_MD_VORNAME) or "").strip()
    md_nachname = (row.get(COL_MD_NACHNAME) or "").strip()
    md_salutation = (row.get(COL_MD_SALUTATION) or "").strip()
    legal = (row.get(COL_LEGAL_NAME) or "").strip()
    phone = (row.get(COL_PHONE) or "").strip()
    email = (row.get(COL_EMAIL) or "").strip()

    # NEW: Try to parse existing full address into structured components
    row = row.copy()  # Work on a copy
    updated = False
    
    if addr and (not addr_street or not addr_house or not addr_postcode or not addr_city):
        print(f"  [{row_idx}] Parsing existing full address using local ML Studio model...")
        # Use local model for parsing
        parsed_addr = parse_address_into_components(addr)
        
        # Apply parsed results
        if parsed_addr["street"] and not addr_street:
            row[COL_ADDRESS_STREET] = parsed_addr["street"]
            updated = True
            print(f"  [{row_idx}] ✔ Extracted street → {parsed_addr['street']}")
        if parsed_addr["house_number"] and not addr_house:
            row[COL_ADDRESS_HOUSE_NUMBER] = parsed_addr["house_number"]
            updated = True
            print(f"  [{row_idx}] ✔ Extracted house number → {parsed_addr['house_number']}")
        if parsed_addr["postcode"] and not addr_postcode:
            row[COL_ADDRESS_POSTCODE] = parsed_addr["postcode"]
            updated = True
            print(f"  [{row_idx}] ✔ Extracted postcode → {parsed_addr['postcode']}")
        if parsed_addr["city"] and not addr_city:
            row[COL_ADDRESS_CITY] = parsed_addr["city"]
            updated = True
            print(f"  [{row_idx}] ✔ Extracted city → {parsed_addr['city']}")
        
        # Update local variables after parsing
        addr_street = row.get(COL_ADDRESS_STREET, "").strip()
        addr_house = row.get(COL_ADDRESS_HOUSE_NUMBER, "").strip()
        addr_postcode = row.get(COL_ADDRESS_POSTCODE, "").strip()
        addr_city = row.get(COL_ADDRESS_CITY, "").strip()
    
    # NEW: Try to parse existing managing director name into structured fields
    if md and (not md_vorname or not md_nachname or not md_salutation):
        print(f"  [{row_idx}] Parsing existing managing director name using local ML Studio model...")
        # Use local model for parsing
        parsed_name = parse_managing_director_name(md)
        
        # Apply parsed results
        if parsed_name["vorname"] and not md_vorname:
            row[COL_MD_VORNAME] = parsed_name["vorname"]
            updated = True
            print(f"  [{row_idx}] ✔ Extracted Vorname → {parsed_name['vorname']}")
        if parsed_name["nachname"] and not md_nachname:
            row[COL_MD_NACHNAME] = parsed_name["nachname"]
            updated = True
            print(f"  [{row_idx}] ✔ Extracted Nachname → {parsed_name['nachname']}")
        if parsed_name["salutation"] and not md_salutation:
            row[COL_MD_SALUTATION] = parsed_name["salutation"]
            updated = True
            print(f"  [{row_idx}] ✔ Extracted Salutation → {parsed_name['salutation']}")
        
        # Update local variables after parsing
        md_vorname = row.get(COL_MD_VORNAME, "").strip()
        md_nachname = row.get(COL_MD_NACHNAME, "").strip()
        md_salutation = row.get(COL_MD_SALUTATION, "").strip()

    # Continue with existing logic...
    need_addr = address_incomplete(addr)
    need_addr_street = not addr_street
    need_addr_house = not addr_house
    need_addr_postcode = not addr_postcode
    need_addr_city = not addr_city
    need_md = not md
    need_md_vorname = not md_vorname
    need_md_nachname = not md_nachname
    need_md_salutation = not md_salutation
    need_legal = not legal
    need_phone = not phone
    need_email = not email

    if not any([need_addr, need_addr_street, need_addr_house, need_addr_postcode, need_addr_city, need_md, need_md_vorname, need_md_nachname, need_md_salutation, need_legal, need_phone, need_email]):
        if updated:
            print(f"  [{row_idx}] Local parsing completed, no GPT imprint call needed.")
            return (row_idx, row, True)
        else:
            print(f"  [{row_idx}] Nothing relevant missing → skipping GPT call.")
            return (row_idx, row, False)

    # Get imprint text (cached per domain, thread-safe)
    imprint_text = None
    should_fetch_imprint = False

    with imprint_lock:
        if domain in imprint_text_cache:
            imprint_text = imprint_text_cache[domain]
        elif domain not in imprint_fetching:
            # Mark as fetching to prevent duplicate requests
            imprint_fetching.add(domain)
            should_fetch_imprint = True

    if should_fetch_imprint:
        # Fetch outside the lock to avoid blocking other threads
        print(f"  [{row_idx}] Fetching imprint text for {domain}...")
        imprint_text = get_imprint_text_for_domain(domain)
        with imprint_lock:
            imprint_text_cache[domain] = imprint_text
            imprint_fetching.discard(domain)
    elif imprint_text is None:
        # Another thread is fetching, wait a bit and retry
        time.sleep(0.5)
        with imprint_lock:
            if domain in imprint_text_cache:
                imprint_text = imprint_text_cache[domain]

    if not imprint_text:
        print(f"  [{row_idx}] ⚠ No imprint text found → skipping GPT for this domain.")
        return (row_idx, row, False)

    # GPT call (cached per domain, thread-safe)
    gpt_data = None
    should_fetch_gpt = False

    with domain_lock:
        if domain in domain_cache:
            gpt_data = domain_cache[domain]
        elif domain not in domain_fetching:
            # Mark as fetching to prevent duplicate requests
            domain_fetching.add(domain)
            should_fetch_gpt = True

    if should_fetch_gpt:
        # Fetch outside the lock to avoid blocking other threads
        print(f"  [{row_idx}] Calling GPT for structured extraction...")
        gpt_data = call_gpt_for_imprint(domain, company, imprint_text)
        with domain_lock:
            domain_cache[domain] = gpt_data
            domain_fetching.discard(domain)
    elif gpt_data is None:
        # Another thread is fetching, wait a bit and retry
        time.sleep(0.5)
        with domain_lock:
            if domain in domain_cache:
                gpt_data = domain_cache[domain]

    need_any_md = need_md or need_md_vorname or need_md_nachname or need_md_salutation
    if (
        need_any_md
        and imprint_text
        and gpt_managing_directors_is_empty(gpt_data.get("managing_directors"))
    ):
        hint = extract_managing_director_from_imprint_plaintext(imprint_text)
        if hint:
            gpt_data = {
                **gpt_data,
                "managing_directors": [
                    {
                        "first_name": None,
                        "last_name": None,
                        "gender": None,
                        "full_name": hint,
                    }
                ],
            }
            with domain_lock:
                domain_cache[domain] = gpt_data

    # Apply updates
    updated = False
    row = row.copy()  # Work on a copy to avoid race conditions

    # Update full address (backward compatibility)
    gpt_addr = gpt_data.get("full_address")
    if need_addr and gpt_addr:
        print(f"  [{row_idx}] ✔ Updating address → {gpt_addr}")
        row[COL_ADDRESS] = gpt_addr
        updated = True

    # Update structured address fields
    gpt_addr_street = gpt_data.get("address_street")
    if need_addr_street and gpt_addr_street:
        print(f"  [{row_idx}] ✔ Updating street → {gpt_addr_street}")
        row[COL_ADDRESS_STREET] = gpt_addr_street
        updated = True

    gpt_addr_house = gpt_data.get("address_house_number")
    if need_addr_house and gpt_addr_house:
        print(f"  [{row_idx}] ✔ Updating house number → {gpt_addr_house}")
        row[COL_ADDRESS_HOUSE_NUMBER] = gpt_addr_house
        updated = True

    gpt_addr_postcode = gpt_data.get("address_postcode")
    if need_addr_postcode and gpt_addr_postcode:
        print(f"  [{row_idx}] ✔ Updating postcode → {gpt_addr_postcode}")
        row[COL_ADDRESS_POSTCODE] = gpt_addr_postcode
        updated = True

    gpt_addr_city = gpt_data.get("address_city")
    if need_addr_city and gpt_addr_city:
        print(f"  [{row_idx}] ✔ Updating city → {gpt_addr_city}")
        row[COL_ADDRESS_CITY] = gpt_addr_city
        updated = True

    # Update managing directors with structured format
    gpt_md_list = gpt_data.get("managing_directors") or []
    if (need_md or need_md_vorname or need_md_nachname or need_md_salutation) and gpt_md_list:
        # Format: "Herr/Frau FirstName LastName" or "FirstName LastName" if no gender
        md_parts = []
        first_md = None
        
        for md in gpt_md_list:
            if isinstance(md, dict):
                gender = md.get("gender") or ""
                first_name = md.get("first_name") or ""
                last_name = md.get("last_name") or ""
                full_name = md.get("full_name") or ""
                
                # Store first managing director for separate columns
                if first_md is None:
                    first_md = {
                        "gender": gender,
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": full_name
                    }
                
                # Build formatted string
                parts = []
                if gender:
                    parts.append(gender)
                if first_name:
                    parts.append(first_name)
                if last_name:
                    parts.append(last_name)
                
                if parts:
                    md_parts.append(" ".join(parts))
                elif full_name:
                    md_parts.append(full_name)
            elif isinstance(md, str):
                # Fallback for old format
                md_parts.append(md)
        
        if md_parts:
            md_joined = ", ".join(md_parts)
            if need_md:
                print(f"  [{row_idx}] ✔ Updating managing director(s) → {md_joined}")
                row[COL_MD] = md_joined
                updated = True
        
        # Update separate columns with first managing director data
        if first_md:
            if need_md_vorname and first_md["first_name"]:
                print(f"  [{row_idx}] ✔ Updating Vorname → {first_md['first_name']}")
                row[COL_MD_VORNAME] = first_md["first_name"]
                updated = True
            
            if need_md_nachname and first_md["last_name"]:
                print(f"  [{row_idx}] ✔ Updating Nachname → {first_md['last_name']}")
                row[COL_MD_NACHNAME] = first_md["last_name"]
                updated = True
            
            if need_md_salutation and first_md["gender"]:
                print(f"  [{row_idx}] ✔ Updating Salutation → {first_md['gender']}")
                row[COL_MD_SALUTATION] = first_md["gender"]
                updated = True

    gpt_legal = gpt_data.get("company_legal_name")
    if need_legal and gpt_legal:
        print(f"  [{row_idx}] ✔ Updating legal name → {gpt_legal}")
        row[COL_LEGAL_NAME] = gpt_legal
        updated = True

    gpt_phones = gpt_data.get("generic_company_phones") or []
    if need_phone and gpt_phones:
        phones_new = ", ".join(gpt_phones)
        print(f"  [{row_idx}] ✔ Updating phones → {phones_new}")
        row[COL_PHONE] = phones_new
        updated = True

    gpt_emails = gpt_data.get("generic_company_emails") or []
    if need_email and gpt_emails:
        emails_new = ", ".join(gpt_emails)
        print(f"  [{row_idx}] ✔ Updating emails → {emails_new}")
        row[COL_EMAIL] = emails_new
        updated = True

    if not updated:
        print(f"  [{row_idx}] ⚠ GPT did not provide new usable data for this row.")

    return (row_idx, row, updated)


def _write_csv(filepath: str, fieldnames, rows):
    """Small helper to write a full CSV file."""
    with open(filepath, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def parse_phone_number(phone_str: str) -> Tuple[str, str]:
    """
    Parse phone number into prefix (vorwahl) and number.
    Example: "+49 821 492570" -> ("+49 821", "492570")
    """
    if not phone_str:
        return ("", "")
    
    phone_str = phone_str.strip()
    # Remove common separators
    phone_str = re.sub(r'[\s\-\(\)]+', ' ', phone_str)
    
    # Try to extract German phone format (+49 XX XXXXXXX)
    match = re.match(r'^(\+49\s*\d{2,5})\s*(.+)$', phone_str)
    if match:
        return (match.group(1).strip(), match.group(2).strip())
    
    # Fallback: try to split on first space after +49
    if phone_str.startswith('+49'):
        parts = phone_str.split(' ', 2)
        if len(parts) >= 3:
            return (f"{parts[0]} {parts[1]}", parts[2])
        elif len(parts) == 2:
            return (parts[0], parts[1])
    
    # If no prefix found, return as number
    return ("", phone_str)


def transform_csv_to_new_format(input_csv: str, output_csv: str):
    """
    Transform CSV from current format to new format with different column names.
    
    New columns:
    Adress-ID;Anrede;Namenszeile;Namenszeile 1;Namenszeile 2;Namenszeile 3;PLZ;Ort;Ortsteil;
    Straße;Hausnummer;Branchencode WZ;Branchenname WZ;Dachmarkt WZ;Bundesland;
    Entscheider 1 Anrede;Entscheider 1 Titel;Entscheider 1 Vorname;Entscheider 1 Nachname;
    Entscheider 1 Funktionsnummer;Entscheider 1 Funktionsname;vorwahl_telefon;telefonnummer;
    e-mail-adresse;template;tracking_link
    """
    # New column order
    new_fieldnames = [
        "Adress-ID",
        "Anrede",
        "Namenszeile",
        "Namenszeile 1",
        "Namenszeile 2",
        "Namenszeile 3",
        "PLZ",
        "Ort",
        "Ortsteil",
        "Straße",
        "Hausnummer",
        "Branchencode WZ",
        "Branchenname WZ",
        "Dachmarkt WZ",
        "Bundesland",
        "Entscheider 1 Anrede",
        "Entscheider 1 Titel",
        "Entscheider 1 Vorname",
        "Entscheider 1 Nachname",
        "Entscheider 1 Funktionsnummer",
        "Entscheider 1 Funktionsname",
        "vorwahl_telefon",
        "telefonnummer",
        "e-mail-adresse",
        "template",
        "tracking_link",
        "Domain",
    ]
    
    with open(input_csv, "r", encoding="utf-8-sig", newline="") as f_in:
        reader = csv.DictReader(f_in, delimiter=";")
        rows = list(reader)
    
    transformed_rows = []
    
    for idx, row in enumerate(rows):
        new_row = {}
        
        # Adress-ID (empty or use index)
        new_row["Adress-ID"] = ""
        
        # Anrede (from Salutation)
        new_row["Anrede"] = (row.get(COL_MD_SALUTATION) or "").strip()
        
        # Namenszeile (from Company or Company legal name)
        # Add leading slash as in example format
        company_name = (row.get(COL_COMPANY) or "").strip()
        legal_name = (row.get(COL_LEGAL_NAME) or "").strip()
        namenszeile = legal_name if legal_name else company_name

        new_row["Namenszeile"] = namenszeile
        
        # Put full business name in Namenszeile 1
        new_row["Namenszeile 1"] = namenszeile
        new_row["Namenszeile 2"] = ""
        new_row["Namenszeile 3"] = ""
        
        # PLZ (from Postcode)
        new_row["PLZ"] = (row.get(COL_ADDRESS_POSTCODE) or "").strip()
        
        # Ort (from City)
        new_row["Ort"] = (row.get(COL_ADDRESS_CITY) or "").strip()
        
        # Ortsteil (not available, empty)
        new_row["Ortsteil"] = ""
        
        # Straße (from Street)
        new_row["Straße"] = (row.get(COL_ADDRESS_STREET) or "").strip()
        
        # Hausnummer (from House Number)
        new_row["Hausnummer"] = (row.get(COL_ADDRESS_HOUSE_NUMBER) or "").strip()
        
        # Branchencode WZ, Branchenname WZ, Dachmarkt WZ (not available, empty)
        new_row["Branchencode WZ"] = ""
        new_row["Branchenname WZ"] = ""
        new_row["Dachmarkt WZ"] = ""
        
        # Bundesland (not available, empty)
        new_row["Bundesland"] = ""
        
        # Entscheider 1 Anrede (from Salutation)
        new_row["Entscheider 1 Anrede"] = (row.get(COL_MD_SALUTATION) or "").strip()
        
        # Entscheider 1 Titel (not available, empty)
        new_row["Entscheider 1 Titel"] = ""
        
        # Entscheider 1 Vorname (from Vorname)
        new_row["Entscheider 1 Vorname"] = (row.get(COL_MD_VORNAME) or "").strip()
        
        # Entscheider 1 Nachname (from Nachname)
        new_row["Entscheider 1 Nachname"] = (row.get(COL_MD_NACHNAME) or "").strip()
        
        # Entscheider 1 Funktionsnummer (not available, empty)
        new_row["Entscheider 1 Funktionsnummer"] = ""
        
        # Entscheider 1 Funktionsname (assume "Geschäftsführer/in" if managing director exists)
        md = (row.get(COL_MD) or "").strip()
        if md:
            new_row["Entscheider 1 Funktionsname"] = "Geschäftsführer/in"
        else:
            new_row["Entscheider 1 Funktionsname"] = ""
        
        # Phone number (full number goes into telefonnummer)
        phone_str = (row.get(COL_PHONE) or "").strip()
        if phone_str:
            # Take first phone number if multiple
            first_phone = phone_str.split(",")[0].strip()
            new_row["vorwahl_telefon"] = ""
            new_row["telefonnummer"] = first_phone
        else:
            new_row["vorwahl_telefon"] = ""
            new_row["telefonnummer"] = ""
        
        # e-mail-adresse (from Generic Company Emails)
        email_str = (row.get(COL_EMAIL) or "").strip()
        if email_str:
            # Take first email if multiple
            first_email = email_str.split(",")[0].strip()
            new_row["e-mail-adresse"] = first_email
        else:
            new_row["e-mail-adresse"] = ""
        
        # template (not available, empty)
        new_row["template"] = ""
        
        # tracking_link (not available, empty)
        new_row["tracking_link"] = ""

        # Keep original Domain column from input CSV
        new_row["Domain"] = (row.get(COL_DOMAIN) or "").strip()

        transformed_rows.append(new_row)
    
    # Write transformed CSV
    with open(output_csv, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=new_fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(transformed_rows)
    
    print(f"\n✅ Transformed CSV written to: {output_csv}")
    print(f"   Transformed {len(transformed_rows)} rows")


def enrich_with_gpt(input_csv: str, output_csv: str, max_workers: Optional[int] = None):
    """
    Enrich CSV with GPT-extracted data from website imprints.
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file
        max_workers: Maximum number of concurrent workers (default: MAX_WORKERS_HTTP)
    """
    with open(input_csv, "r", encoding="utf-8-sig", newline="") as f_in:
        # Try to automatically detect whether the file is comma- or semicolon-separated.
        sample = f_in.read(4096)
        f_in.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        except csv.Error:
            # Fallback: assume comma-delimited if detection fails.
            dialect = csv.get_dialect("excel")

        reader = csv.DictReader(f_in, dialect=dialect)
        fieldnames = list(reader.fieldnames) if reader.fieldnames else []
        rows = list(reader) if fieldnames else []

    # Fallback: if autodetection produced one giant header field, retry explicitly with ';'.
    if (not fieldnames) or (
        len(fieldnames) == 1
        and isinstance(fieldnames[0], str)
        and ";" in fieldnames[0]
    ):
        with open(input_csv, "r", encoding="utf-8-sig", newline="") as f_in:
            reader = csv.DictReader(f_in, delimiter=";")
            fieldnames = list(reader.fieldnames) if reader.fieldnames else []
            rows = list(reader) if fieldnames else []

    if not fieldnames:
        raise ValueError("No header found in CSV")

    # Clean up any anonymous/extra columns stored under the key None
    # (can happen when a row has more separators than header columns)
    for row in rows:
        if None in row:
            del row[None]

    # Normalize known core headers so we can accept variants like
    # "domain", " Domain ", "website/webseite", or derive a domain from email.
    header_lookup = {_normalize_header_name(fn): fn for fn in fieldnames if fn}

    domain_source = None
    for candidate in ("domain", "website", "webseite", "url"):
        if candidate in header_lookup:
            domain_source = header_lookup[candidate]
            break

    email_source = None
    for candidate in ("generic company emails", "email", "e-mail", "e-mail-adresse", "mail"):
        if candidate in header_lookup:
            email_source = header_lookup[candidate]
            break

    if domain_source or email_source:
        for row in rows:
            if (row.get(COL_DOMAIN) or "").strip():
                continue

            if domain_source:
                row[COL_DOMAIN] = (row.get(domain_source) or "").strip()

            if not (row.get(COL_DOMAIN) or "").strip() and email_source:
                derived_domain = _extract_domain_from_email_value(row.get(email_source))
                if derived_domain:
                    row[COL_DOMAIN] = derived_domain

        if COL_DOMAIN not in fieldnames:
            fieldnames.append(COL_DOMAIN)

    company_source = None
    for candidate in ("company", "firma", "name", "unternehmen"):
        if candidate in header_lookup:
            company_source = header_lookup[candidate]
            break
    if company_source and company_source != COL_COMPANY:
        for row in rows:
            if not (row.get(COL_COMPANY) or "").strip():
                row[COL_COMPANY] = (row.get(company_source) or "").strip()
        if COL_COMPANY not in fieldnames:
            fieldnames.append(COL_COMPANY)

    # Ensure enrichment target columns exist in fieldnames.
    # This prevents DictWriter failures when GPT updates legacy columns
    # that were not present in the original input header.
    new_columns = [
        COL_ADDRESS,
        COL_MD,
        COL_LEGAL_NAME,
        COL_PHONE,
        COL_EMAIL,
        COL_ADDRESS_STREET,
        COL_ADDRESS_HOUSE_NUMBER,
        COL_ADDRESS_POSTCODE,
        COL_ADDRESS_CITY,
        COL_MD_VORNAME,
        COL_MD_NACHNAME,
        COL_MD_SALUTATION,
    ]
    for col in new_columns:
        if col not in fieldnames:
            fieldnames.append(col)

    # Ensure all rows have the new columns initialized
    for row in rows:
        for col in new_columns:
            if col not in row:
                row[col] = ""

    if max_workers is None:
        max_workers = MAX_WORKERS_HTTP

    # Thread-safe caches with locks
    imprint_text_cache: Dict[str, Optional[str]] = {}
    domain_cache: Dict[str, Dict[str, Any]] = {}
    imprint_fetching: set = set()
    domain_fetching: set = set()
    imprint_lock = Lock()
    domain_lock = Lock()

    # Process rows in parallel
    total_rows = len(rows)
    # Start with the original rows, so partial writes always contain something for every row
    processed_rows = rows.copy()  # <-- CHANGED: use original rows as baseline

    # Lists for categorized output
    _enriched: List[Dict[str, str]] = []
    _missing_data: List[Dict[str, str]] = []

    print(f"Processing {total_rows} rows with {max_workers} concurrent workers...\n")

    partial_path = output_csv + ".partial"  # <-- CHANGED: partial file path

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_row = {
            executor.submit(
                process_row,
                row,
                i + 1,
                total_rows,
                imprint_text_cache,
                domain_cache,
                imprint_fetching,
                domain_fetching,
                imprint_lock,
                domain_lock,
            ): i
            for i, row in enumerate(rows)
        }

        # Collect results as they complete with progress bar
        completed = 0
        with tqdm(total=total_rows, desc="Processing rows", unit="row") as pbar:
            for future in as_completed(future_to_row):
                try:
                    row_idx, updated_row, was_updated = future.result()
                    processed_rows[row_idx - 1] = updated_row
                    completed += 1
                    pbar.update(1)

                    # Periodically write a partial checkpoint CSV
                    if completed % 100 == 0:  # <-- CHANGED: write every 10 rows
                        print(f"\n💾 Writing partial checkpoint to {partial_path} ({completed}/{total_rows})\n")
                        _write_csv(partial_path, fieldnames, processed_rows)
                except Exception as e:
                    original_idx = future_to_row[future]
                    print(f"\n❌ Error processing row {original_idx + 1}: {e}")
                    processed_rows[original_idx] = rows[original_idx]  # Keep original on error
                    pbar.update(1)

                    # Also checkpoint after an error
                    print(f"\n💾 Writing partial checkpoint to {partial_path} after error\n")
                    _write_csv(partial_path, fieldnames, processed_rows)

    # Split processed rows into enriched vs. missing name/address
    for row in processed_rows:
        has_full_address = all(
            [
                (row.get(COL_ADDRESS_STREET) or "").strip(),
                (row.get(COL_ADDRESS_HOUSE_NUMBER) or "").strip(),
                (row.get(COL_ADDRESS_POSTCODE) or "").strip(),
                (row.get(COL_ADDRESS_CITY) or "").strip(),
            ]
        )
        has_name = all(
            [
                (row.get(COL_MD_VORNAME) or "").strip(),
                (row.get(COL_MD_NACHNAME) or "").strip(),
            ]
        )

        if has_full_address and has_name:
            _enriched.append(row)
        else:
            _missing_data.append(row)

    # Final full write (enriched only)
    _write_csv(output_csv, fieldnames, _enriched)
    print(f"\n✅ Done. Enriched CSV written to: {output_csv}")
    print(f"   Processed {total_rows} rows with {max_workers} concurrent workers")
    print(f"   Last partial checkpoint: {partial_path}")
    print(f"   Rows with complete name & address: {len(_enriched)}")
    print(f"   Rows with missing name and/or address: {len(_missing_data)}")

    # Also write missing-data rows to a separate CSV next to the main output
    missing_path = output_csv.replace(".csv", "_missing_data.csv")
    _write_csv(missing_path, fieldnames, _missing_data)
    print(f"   Rows with missing data written to: {missing_path}")

    # Return the lists so this function can be reused programmatically
    return _enriched, _missing_data


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Enrich German business CSV from website imprint using GPT."
    )
    parser.add_argument("input_csv", help="Path to input CSV")
    parser.add_argument("output_csv", help="Path to output CSV")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=f"Maximum number of concurrent workers (default: {MAX_WORKERS_HTTP})",
    )
    parser.add_argument(
        "--transform",
        action="store_true",
        help="Transform CSV to new format after enrichment",
    )
    args = parser.parse_args()

    enrich_with_gpt(args.input_csv, args.output_csv, max_workers=args.max_workers)
    
    if args.transform:
        transform_output = args.output_csv.replace(".csv", "_transformed.csv")
        transform_csv_to_new_format(args.output_csv, transform_output)
