# Used to complete lead lists from oceanio.com


import csv
import time
import re
from typing import Dict, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ------------- Config -------------

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10  # seconds
DELAY_BETWEEN_REQUESTS = 1.0  # polite delay in seconds

# Column names from your CSV
COL_COMPANY = "Company"
COL_DOMAIN = "Domain"
COL_PHONE = "Generic Company Phones"
COL_EMAIL = "Generic Company Emails"
COL_ADDRESS = "Headquarter Raw Address"
COL_MD = "Imprint: Managing director"
COL_LEGAL_NAME = "Imprint: Company legal name"


# ------------- HTTP helpers -------------

def fetch_url(url: str) -> Optional[requests.Response]:
    """Fetch a URL with basic error handling and a browser-like user-agent."""
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
    Try https://domain and http://domain and return the first that works.
    """
    domain = domain.strip()
    if not domain:
        return None

    for scheme in ("https://", "http://"):
        url = scheme + domain
        resp = fetch_url(url)
        if resp:
            return resp.url  # final URL after redirects
    return None


# ------------- Imprint discovery -------------

def find_imprint_url(base_html: str, base_url: str) -> Optional[str]:
    """
    Find an imprint URL ("Impressum" / "Imprint") based on:
    1) Common direct paths
    2) Links on the homepage
    """
    # 1) Common direct paths
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

    # 2) Parse homepage and search links
    soup = BeautifulSoup(base_html, "html.parser")
    candidates = []

    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"].lower()
        if "impressum" in text or "impressum" in href or "imprint" in text or "imprint" in href:
            candidates.append(a["href"])

    # Normalize candidate URLs
    for href in candidates:
        full_url = urljoin(base_url, href)
        resp = fetch_url(full_url)
        if resp:
            return resp.url

    return None


# ------------- Text extraction helpers -------------

LEGAL_ENTITY_PATTERN = re.compile(
    r"\b(GmbH|UG|AG|KG|OHG|GbR|e\.V\.|e\. K\.|e\.K\.|e\. Kfm\.|GmbH & Co\. KG)\b",
    re.IGNORECASE,
)

ZIP_PATTERN = re.compile(r"\b\d{5}\b")


def normalize_lines(text: str):
    """Yield cleaned non-empty lines from text."""
    for line in text.splitlines():
        cleaned = " ".join(line.split())
        if cleaned:
            yield cleaned


def extract_address_from_text(text: str) -> Optional[str]:
    """
    Very simple heuristic: find first line containing a 5-digit German ZIP code.
    Optionally merge with previous line if it looks like a street.
    """
    lines = list(normalize_lines(text))
    for i, line in enumerate(lines):
        if ZIP_PATTERN.search(line):
            # combine previous line if it looks like a street
            if i > 0:
                prev = lines[i - 1]
                if any(
                    s in prev.lower()
                    for s in ["straße", "str.", "str ", "weg", "platz", "allee", "ring", "gasse", "weg ", "ufer"]
                ):
                    return f"{prev}, {line}"
            return line
    return None


def extract_md_from_text(text: str) -> Optional[str]:
    """
    Heuristic: look for lines with 'Geschäftsführer', 'Inhaber', 'vertreten durch' etc.
    Returns the part after ':' if present.
    """
    keywords = ["geschäftsführer", "inhaber", "vertretungsberechtigt", "vertreten durch", "geschäftsleitung"]
    for line in normalize_lines(text):
        low = line.lower()
        if any(k in low for k in keywords):
            # e.g. "Geschäftsführer: Max Mustermann"
            if ":" in line:
                return line.split(":", 1)[1].strip()
            # fallback: drop keyword itself
            for k in keywords:
                if k in low:
                    idx = low.find(k)
                    candidate = line[idx + len(k):].strip(" :-–")
                    if candidate:
                        return candidate
    return None


def extract_legal_name_from_text(text: str) -> Optional[str]:
    """
    Heuristic: find first line containing a typical German legal entity.
    """
    best_line = None
    for line in normalize_lines(text):
        if LEGAL_ENTITY_PATTERN.search(line):
            best_line = line
            break
    return best_line


def scrape_imprint_data(domain: str) -> Dict[str, Optional[str]]:
    """
    High-level helper:
    - Find base URL
    - Find imprint URL
    - Extract address, managing director, legal name
    """
    result = {
        "address": None,
        "managing_director": None,
        "legal_name": None,
    }

    base_url = best_base_url(domain)
    if not base_url:
        return result

    # Fetch homepage HTML (again, but reusing helper for simplicity)
    resp_home = fetch_url(base_url)
    if not resp_home:
        return result

    imprint_url = find_imprint_url(resp_home.text, base_url)
    if not imprint_url:
        # As a fallback, also try to extract from homepage itself
        text = resp_home.text
    else:
        resp_imprint = fetch_url(imprint_url)
        if not resp_imprint:
            text = resp_home.text
        else:
            text = resp_imprint.text

    soup = BeautifulSoup(text, "html.parser")
    plain_text = soup.get_text("\n")

    address = extract_address_from_text(plain_text)
    md = extract_md_from_text(plain_text)
    legal_name = extract_legal_name_from_text(plain_text)

    result["address"] = address
    result["managing_director"] = md
    result["legal_name"] = legal_name

    return result


# ------------- CSV helpers -------------

def address_incomplete(addr: str) -> bool:
    """
    Decide if an address is 'weak' and should be replaced/enriched.
    Very simple: if it doesn't contain a 5-digit ZIP, treat as incomplete.
    """
    if not addr:
        return True
    if not ZIP_PATTERN.search(addr):
        return True
    return False


def md_incomplete(md: str) -> bool:
    return not md or not md.strip()


def legal_name_incomplete(name: str) -> bool:
    if not name or not name.strip():
        return True
    # if it doesn't contain any legal entity, might still be incomplete
    if not LEGAL_ENTITY_PATTERN.search(name):
        return False  # don't be too aggressive here
    return False


def enrich_csv(input_path: str, output_path: str):
    """
    Read input CSV, enrich missing fields by scraping, and write to output CSV.
    """
    with open(input_path, "r", encoding="utf-8-sig", newline="") as f_in:
        reader = csv.DictReader(f_in, delimiter=";")
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("No header found in CSV")

        rows = list(reader)

    # Cache per domain to avoid re-scraping
    domain_cache: Dict[str, Dict[str, Optional[str]]] = {}

    for idx, row in enumerate(rows, start=1):
        domain = (row.get(COL_DOMAIN) or "").strip()
        if not domain:
            print(f"[{idx}] Skipping row — no domain")
            continue

        addr = (row.get(COL_ADDRESS) or "").strip()
        md = (row.get(COL_MD) or "").strip()
        legal_name = (row.get(COL_LEGAL_NAME) or "").strip()

        need_addr = address_incomplete(addr)
        need_md = md_incomplete(md)
        need_legal_name = legal_name_incomplete(legal_name)

        print("\n" + "="*80)
        print(f"[{idx}] Processing domain: {domain}")
        print("-" * 80)

        if not (need_addr or need_md or need_legal_name):
            print("Nothing missing → skipping")
            continue

        print("Missing fields:")
        if need_addr:
            print("  - Address incomplete or missing")
        if need_md:
            print("  - Managing director missing")
        if need_legal_name:
            print("  - Legal company name missing")

        # Scrape (or load from cache)
        if domain not in domain_cache:
            print(f"[{idx}] Scraping imprint for: {domain}")
            domain_cache[domain] = scrape_imprint_data(domain)
            time.sleep(DELAY_BETWEEN_REQUESTS)
        else:
            print(f"[{idx}] Using cached scrape result for: {domain}")

        imprint_data = domain_cache[domain]

        scraped_addr = imprint_data.get("address")
        scraped_md = imprint_data.get("managing_director")
        scraped_legal_name = imprint_data.get("legal_name")

        print("\nScraped data:")
        print(f"  Address:            {scraped_addr}")
        print(f"  Managing director:  {scraped_md}")
        print(f"  Legal name:         {scraped_legal_name}")

        print("\nApplied changes:")
        changed_any = False

        if need_addr and scraped_addr:
            print(f"  ✔ Address updated → {scraped_addr}")
            row[COL_ADDRESS] = scraped_addr
            changed_any = True
        else:
            print("  ✘ Address not updated")

        if need_md and scraped_md:
            print(f"  ✔ Managing director updated → {scraped_md}")
            row[COL_MD] = scraped_md
            changed_any = True
        else:
            print("  ✘ Managing director not updated")

        if need_legal_name and scraped_legal_name:
            print(f"  ✔ Legal company name updated → {scraped_legal_name}")
            row[COL_LEGAL_NAME] = scraped_legal_name
            changed_any = True
        else:
            print("  ✘ Legal company name not updated")

        if not changed_any:
            print("  ⚠ No new data applied for this row")

        print("="*80)



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Enrich German business data from website imprints.")
    parser.add_argument("input_csv", help="Input CSV file path")
    parser.add_argument("output_csv", help="Output CSV file path")

    args = parser.parse_args()
    enrich_csv(args.input_csv, args.output_csv)
