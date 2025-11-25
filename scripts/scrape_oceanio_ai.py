import os
import csv
import time
import json
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore  # <-- uses Semaphore

import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from tqdm import tqdm

# ---------------- OpenAI client ----------------

# Assumes OPENAI_API_KEY is set in your environment
client = OpenAI()

OPENAI_MODEL = "gpt-5-mini"  # use the model name you want to call


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
    domain = domain.strip()
    if not domain:
        return None

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
  - The company's full postal address (street, house number, postal code, city, country).
  - The managing director(s) / legal representatives.
  - The full legal company name as written in the imprint (including GmbH, UG, AG, KG, etc.).
  - Generic company phone numbers (main switchboard, office numbers; ignore obviously private mobiles if clearly marked as personal).
  - Generic company email addresses (like info@, kontakt@, office@; also include named emails if they are clearly business emails in the imprint).

Output rules:
- Always respond with a single valid JSON object only, no explanation text.
- Use this exact JSON structure and keys:

{
  "full_address": "string or null",
  "managing_directors": ["list", "of", "names"],
  "company_legal_name": "string or null",
  "generic_company_phones": ["+49 ...", "..."],
  "generic_company_emails": ["info@example.com", "..."],
  "confidence": 0.0
}

Notes:
- "full_address" should be one line, including street, house number, postal code, city, country if available.
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
        # Fallback: wrap in minimal structure so we don’t crash
        print("  ⚠ GPT response was not valid JSON, raw content:")
        print(content)
        data = {
            "full_address": None,
            "managing_directors": [],
            "company_legal_name": None,
            "generic_company_phones": [],
            "generic_company_emails": [],
            "confidence": 0.0,
        }
    return data


# ---------------- CSV enrichment logic ----------------

COL_COMPANY = "Company"
COL_DOMAIN = "Domain"
COL_PHONE = "Generic Company Phones"
COL_EMAIL = "Generic Company Emails"
COL_ADDRESS = "Headquarter Raw Address"
COL_MD = "Imprint: Managing director"
COL_LEGAL_NAME = "Imprint: Company legal name"


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

    print(f"\n[{row_idx}/{total_rows}] {company} — {domain}")

    if not domain:
        print("  ⚠ No domain → skipping row.")
        return (row_idx, row, False)

    # existing values
    addr = (row.get(COL_ADDRESS) or "").strip()
    md = (row.get(COL_MD) or "").strip()
    legal = (row.get(COL_LEGAL_NAME) or "").strip()
    phone = (row.get(COL_PHONE) or "").strip()
    email = (row.get(COL_EMAIL) or "").strip()

    need_addr = address_incomplete(addr)
    need_md = not md
    need_legal = not legal
    need_phone = not phone
    need_email = not email

    if not any([need_addr, need_md, need_legal, need_phone, need_email]):
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

    # Apply updates
    updated = False
    row = row.copy()  # Work on a copy to avoid race conditions

    gpt_addr = gpt_data.get("full_address")
    if need_addr and gpt_addr:
        print(f"  [{row_idx}] ✔ Updating address → {gpt_addr}")
        row[COL_ADDRESS] = gpt_addr
        updated = True

    gpt_md_list = gpt_data.get("managing_directors") or []
    if need_md and gpt_md_list:
        md_joined = ", ".join(gpt_md_list)
        print(f"  [{row_idx}] ✔ Updating managing director(s) → {md_joined}")
        row[COL_MD] = md_joined
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


def enrich_with_gpt(input_csv: str, output_csv: str, max_workers: Optional[int] = None):
    """
    Enrich CSV with GPT-extracted data from website imprints.
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file
        max_workers: Maximum number of concurrent workers (default: MAX_WORKERS_HTTP)
    """
    with open(input_csv, "r", encoding="utf-8-sig", newline="") as f_in:
        reader = csv.DictReader(f_in, delimiter=";")
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("No header found in CSV")
        rows = list(reader)

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

    # Final full write
    _write_csv(output_csv, fieldnames, processed_rows)
    print(f"\n✅ Done. Enriched CSV written to: {output_csv}")
    print(f"   Processed {total_rows} rows with {max_workers} concurrent workers")
    print(f"   Last partial checkpoint: {partial_path}")


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
    args = parser.parse_args()

    enrich_with_gpt(args.input_csv, args.output_csv, max_workers=args.max_workers)
