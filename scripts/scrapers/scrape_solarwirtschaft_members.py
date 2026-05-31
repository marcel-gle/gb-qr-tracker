#!/usr/bin/env python3
"""
BSW Solarwirtschaft Mitgliedersuche Scraper
===========================================
Two-phase scraper for solarwirtschaft.de member directory:
  1. Listing pages: name, address, business URL (Germany only)
  2. Member detail pages: phone, email, domain

Abhängigkeiten:
    pip install requests lxml
    pip install tqdm  # optional

Nutzung (Projektroot):
    python scripts/scrapers/scrape_solarwirtschaft_members.py
    python scripts/scrapers/scrape_solarwirtschaft_members.py --max-pages 1
    python scripts/scrapers/scrape_solarwirtschaft_members.py --max-members 3
    python scripts/scrapers/scrape_solarwirtschaft_members.py --listing-only
    python scripts/scrapers/scrape_solarwirtschaft_members.py --resume
"""

from __future__ import annotations

import argparse
import codecs
import csv
import json
import logging
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from lxml import html as lxml_html

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover

    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable


LOGGER = logging.getLogger("scrape_solarwirtschaft_members")

DEFAULT_LISTING_URL = (
    "https://www.solarwirtschaft.de/unsere-mitglieder/mitgliedersuche/"
)
DEFAULT_OUTPUT = Path("output/solarwirtschaft_members.csv")
DEFAULT_PROGRESS = Path("output/solarwirtschaft_scraper_progress.json")
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

CSV_FIELDNAMES = [
    "name",
    "street",
    "postal_code",
    "city",
    "country",
    "business_url",
    "phone",
    "email",
    "domain",
]

WHITESPACE_RE = re.compile(r"\s+")
PLZ_CITY_RE = re.compile(r"^(\d{4,5})\s+(.+)$")
XPATH_MEMBER_ARTICLES = (
    "//div[contains(@class,'pagecreator_posttypelist--cards')]"
    "//article[contains(@class,'member')]"
)
XPATH_PHONE = (
    "//dd[contains(@class,'type-contact')]"
    "//li[contains(@class,'type--phonenumber')]//a"
)
XPATH_EMAIL = (
    "//dd[contains(@class,'type-contact')]"
    "//li[contains(@class,'type--email')]//a"
)


def configure_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def normalize_domain(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if "@" in value:
        value = value.split("@", 1)[1]
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlparse(value)
        value = parsed.netloc or parsed.path
    value = value.split("/")[0]
    value = value.lower().strip(". ")
    if value.startswith("www."):
        value = value[4:]
    return value


def decode_rot13(value: str) -> str:
    if not value:
        return ""
    return codecs.decode(value, "rot_13")


def decode_obfuscated_email(raw_href: str, raw_text: str) -> str:
    for candidate in (raw_href, raw_text):
        if not candidate:
            continue
        decoded = decode_rot13(candidate.strip())
        if decoded.lower().startswith("mailto:"):
            return decoded[7:].strip().lower()
        if "@" in decoded and " " not in decoded:
            return decoded.strip().lower()
    return ""


def make_session(user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
        }
    )
    return session


def listing_url(base_url: str, page: int) -> str:
    base = base_url.rstrip("/") + "/"
    if page <= 1:
        return base
    return f"{base}page/{page}/"


def request_html(
    session: requests.Session,
    url: str,
    timeout: float,
    referer: Optional[str] = None,
    max_retries: int = 3,
) -> Optional[str]:
    headers: dict[str, str] = {
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if referer else "none",
    }
    if referer:
        headers["Referer"] = referer

    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=timeout, headers=headers)
        except requests.RequestException as exc:
            LOGGER.debug("Request failed for %s (attempt %s): %s", url, attempt + 1, exc)
            if attempt + 1 >= max_retries:
                return None
            time.sleep(2**attempt)
            continue

        if resp.status_code == 429 or resp.status_code >= 500:
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else 2**attempt
            LOGGER.debug(
                "Status %s for %s, retrying in %.1fs",
                resp.status_code,
                url,
                wait,
            )
            if attempt + 1 >= max_retries:
                return None
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            LOGGER.debug("Non-200 status %s for %s", resp.status_code, url)
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            LOGGER.debug("Non-HTML content for %s: %s", url, content_type)
            return None

        return resp.text

    return None


def polite_sleep(delay: float, jitter: float) -> None:
    if delay <= 0:
        return
    time.sleep(delay + random.uniform(0, jitter))


def parse_max_pages(tree: lxml_html.HtmlElement) -> int:
    pagination = tree.xpath(
        "//ul[contains(@class,'pagecreator_pagination')][@data-found-posts]"
    )
    if pagination:
        found = pagination[0].get("data-found-posts")
        per_page = pagination[0].get("data-post-count")
        try:
            total = int(found)
            count = int(per_page) if per_page else 10
            if count > 0:
                return max(1, (total + count - 1) // count)
        except (TypeError, ValueError):
            pass

    page_links = tree.xpath(
        "//ul[contains(@class,'pagecreator_pagination')]"
        "//a[contains(@href,'/page/')]/@href"
    )
    max_page = 1
    for href in page_links:
        match = re.search(r"/page/(\d+)/", href)
        if match:
            max_page = max(max_page, int(match.group(1)))
    return max_page


def parse_address_lines(p_element) -> tuple[str, str, str, str]:
    if p_element is None:
        return "", "", "", ""

    inner_html = lxml_html.tostring(p_element, encoding="unicode", method="html")
    inner_html = re.sub(r"^<p[^>]*>|</p>$", "", inner_html.strip(), flags=re.I)
    chunks = re.split(r"<br\s*/?>", inner_html, flags=re.I)
    lines: List[str] = []
    for chunk in chunks:
        text = normalize_text(lxml_html.fromstring(f"<span>{chunk}</span>").text_content())
        if text:
            lines.append(text)

    if not lines:
        return "", "", "", ""

    country = lines[-1]
    postal_code = ""
    city = ""
    street_parts = lines[:-1]

    if len(lines) >= 2:
        plz_city = lines[-2]
        match = PLZ_CITY_RE.match(plz_city)
        if match:
            postal_code = match.group(1)
            city = match.group(2)
            street_parts = lines[:-2]

    street = ", ".join(street_parts) if street_parts else ""
    return street, postal_code, city, country


def is_germany(country: str) -> bool:
    return country.strip().casefold() == "deutschland"


def parse_listing_page(page_html: str, base_url: str) -> List[dict[str, str]]:
    tree = lxml_html.fromstring(page_html)
    articles = tree.xpath(XPATH_MEMBER_ARTICLES)
    rows: List[dict[str, str]] = []

    for article in articles:
        name_nodes = article.xpath(".//header//h3[contains(@class,'article__headline')]")
        link_nodes = article.xpath(".//footer//a[contains(@class,'article__link')]/@href")
        p_nodes = article.xpath(".//div[contains(@class,'article__body')]//p")

        name = normalize_text(name_nodes[0].text_content()) if name_nodes else ""
        business_url = urljoin(base_url, link_nodes[0]) if link_nodes else ""
        street, postal_code, city, country = parse_address_lines(
            p_nodes[0] if p_nodes else None
        )

        if not name and not business_url:
            continue

        rows.append(
            {
                "name": name,
                "street": street,
                "postal_code": postal_code,
                "city": city,
                "country": country,
                "business_url": business_url,
                "phone": "",
                "email": "",
                "domain": "",
            }
        )

    return rows


def parse_detail_contact(page_html: str) -> tuple[str, str]:
    tree = lxml_html.fromstring(page_html)

    phone = ""
    phone_nodes = tree.xpath(XPATH_PHONE)
    if phone_nodes:
        href = phone_nodes[0].get("href", "")
        if href.lower().startswith("tel:"):
            phone = normalize_text(href[4:])
        else:
            phone = normalize_text(phone_nodes[0].text_content())

    email = ""
    email_nodes = tree.xpath(XPATH_EMAIL)
    if email_nodes:
        email = decode_obfuscated_email(
            email_nodes[0].get("href", ""),
            email_nodes[0].text_content(),
        )

    return phone, email


def load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "listing_pages_done": [],
            "members_done": [],
            "rows": [],
        }
    data = json.loads(path.read_text(encoding="utf-8"))
    data.setdefault("listing_pages_done", [])
    data.setdefault("members_done", [])
    data.setdefault("rows", [])
    return data


def save_progress(path: Path, progress: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(rows: List[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    LOGGER.info("Wrote %d rows to %s", len(rows), output_path)


def scrape_listings(
    session: requests.Session,
    base_url: str,
    timeout: float,
    delay: float,
    jitter: float,
    max_pages: Optional[int],
    progress: dict[str, Any],
    progress_path: Path,
    html_file_listing: Optional[Path] = None,
) -> tuple[List[dict[str, str]], int]:
    pages_done = set(progress.get("listing_pages_done", []))
    seen_urls: set[str] = set()
    german_rows: List[dict[str, str]] = []

    for row in progress.get("rows", []):
        url = row.get("business_url", "")
        if url:
            seen_urls.add(url)
        if is_germany(row.get("country", "")):
            german_rows.append(dict(row))

    if html_file_listing:
        page_html = html_file_listing.read_text(encoding="utf-8", errors="replace")
        page_rows = parse_listing_page(page_html, base_url)
        for row in page_rows:
            if not is_germany(row["country"]):
                LOGGER.debug("Skipping non-DE: %s (%s)", row["name"], row["country"])
                continue
            if row["business_url"] in seen_urls:
                continue
            seen_urls.add(row["business_url"])
            german_rows.append(row)
        progress["rows"] = german_rows
        save_progress(progress_path, progress)
        return german_rows, 1

    first_url = listing_url(base_url, 1)
    first_html = request_html(session, first_url, timeout)
    if not first_html:
        LOGGER.error("Failed to fetch first listing page")
        return german_rows, 0

    tree = lxml_html.fromstring(first_html)
    total_pages = parse_max_pages(tree)
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)
    LOGGER.info("Listing pages to scrape: %d", total_pages)

    page_iter = tqdm(range(1, total_pages + 1), desc="Listing pages", unit="page")
    referer: Optional[str] = None

    for page in page_iter:
        if page in pages_done:
            LOGGER.debug("Skipping listing page %s (already done)", page)
            continue

        url = listing_url(base_url, page)
        if page == 1:
            page_html = first_html
        else:
            page_html = request_html(session, url, timeout, referer=referer)
            polite_sleep(delay, jitter)

        if not page_html:
            LOGGER.warning("Failed to fetch listing page %s", page)
            continue

        referer = url
        page_rows = parse_listing_page(page_html, base_url)
        added = 0
        for row in page_rows:
            if not is_germany(row["country"]):
                LOGGER.debug("Skipping non-DE: %s (%s)", row["name"], row["country"])
                continue
            if row["business_url"] in seen_urls:
                continue
            seen_urls.add(row["business_url"])
            german_rows.append(row)
            added += 1

        pages_done.add(page)
        progress["listing_pages_done"] = sorted(pages_done)
        progress["rows"] = german_rows
        save_progress(progress_path, progress)
        LOGGER.info("Page %s/%s: %s cards, %s new DE members", page, total_pages, len(page_rows), added)

    return german_rows, total_pages


def enrich_details(
    session: requests.Session,
    rows: List[dict[str, str]],
    timeout: float,
    delay: float,
    jitter: float,
    max_members: Optional[int],
    progress: dict[str, Any],
    progress_path: Path,
    listing_referer: str,
) -> List[dict[str, str]]:
    members_done = set(progress.get("members_done", []))
    pending_indices = [
        i for i, row in enumerate(rows) if row.get("business_url") not in members_done
    ]
    if max_members is not None:
        pending_indices = pending_indices[:max_members]

    referer = listing_referer
    member_iter = tqdm(pending_indices, desc="Member details", unit="member")

    for idx in member_iter:
        row = rows[idx]
        url = row.get("business_url", "")
        if not url:
            continue

        page_html = request_html(session, url, timeout, referer=referer)
        polite_sleep(delay, jitter)

        if page_html:
            phone, email = parse_detail_contact(page_html)
            row["phone"] = phone
            row["email"] = email
            row["domain"] = normalize_domain(email)
            referer = url
        else:
            LOGGER.warning("Failed to fetch member page: %s", url)

        members_done.add(url)
        progress["members_done"] = sorted(members_done)
        progress["rows"] = rows
        save_progress(progress_path, progress)

    return rows


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape BSW Solarwirtschaft member directory to CSV.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--progress-file",
        type=Path,
        default=DEFAULT_PROGRESS,
        help=f"Progress JSON path (default: {DEFAULT_PROGRESS})",
    )
    parser.add_argument(
        "--listing-url",
        default=DEFAULT_LISTING_URL,
        help=f"Listing base URL (default: {DEFAULT_LISTING_URL})",
    )
    parser.add_argument(
        "--html-file-listing",
        type=Path,
        default=None,
        help="Parse local listing HTML instead of fetching page 1",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent header",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Base delay between requests in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--jitter",
        type=float,
        default=0.4,
        help="Random extra delay 0..jitter seconds (default: 0.4)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limit listing pages (for testing)",
    )
    parser.add_argument(
        "--max-members",
        type=int,
        default=None,
        help="Limit detail fetches (for testing)",
    )
    parser.add_argument(
        "--listing-only",
        action="store_true",
        help="Only scrape listing pages, skip detail enrichment",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from progress JSON",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.debug)

    if args.resume and args.progress_file.exists():
        progress = load_progress(args.progress_file)
        LOGGER.info(
            "Resuming: %s listing pages, %s members done, %s rows",
            len(progress.get("listing_pages_done", [])),
            len(progress.get("members_done", [])),
            len(progress.get("rows", [])),
        )
    else:
        progress = {
            "listing_pages_done": [],
            "members_done": [],
            "rows": [],
        }

    session = make_session(args.user_agent)
    base_url = args.listing_url

    german_rows, _ = scrape_listings(
        session=session,
        base_url=base_url,
        timeout=args.timeout,
        delay=args.delay,
        jitter=args.jitter,
        max_pages=args.max_pages,
        progress=progress,
        progress_path=args.progress_file,
        html_file_listing=args.html_file_listing,
    )

    if not german_rows:
        LOGGER.error("No German members found")
        return 1

    LOGGER.info("German members from listings: %d", len(german_rows))

    if not args.listing_only:
        german_rows = enrich_details(
            session=session,
            rows=german_rows,
            timeout=args.timeout,
            delay=args.delay,
            jitter=args.jitter,
            max_members=args.max_members,
            progress=progress,
            progress_path=args.progress_file,
            listing_referer=listing_url(base_url, 1),
        )

    write_csv(german_rows, args.output)
    print(f"Scraped {len(german_rows)} German members -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
