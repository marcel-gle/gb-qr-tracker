#!/usr/bin/env python3
"""
Photovoltaikforum Firmenverzeichnis Scraper
===========================================
Lädt die komplette Firmenliste von photovoltaikforum.com und exportiert
Name, Stadt und Kategorie als CSV.

Abhängigkeiten:
    pip install requests lxml

Nutzung (Projektroot):
    python scripts/scrapers/scrape_photovoltaikforum_directory.py
    python scripts/scrapers/scrape_photovoltaikforum_directory.py -o output/pvforum.csv
    python scripts/scrapers/scrape_photovoltaikforum_directory.py --html-file page.html
"""

from __future__ import annotations

import argparse
import csv
import html
import logging
import re
import sys
from pathlib import Path
from typing import Iterable, List, Optional

import requests
from lxml import html as lxml_html

LOGGER = logging.getLogger("scrape_photovoltaikforum_directory")

DEFAULT_URL = (
    "https://www.photovoltaikforum.com/core/business-directory-company-list/"
)
DEFAULT_OUTPUT = Path("output/photovoltaikforum_companies.csv")
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

CSV_FIELDNAMES = ["name", "city", "category"]

LI_FRAGMENT_MARKER = '<li class="companyListItem"'
XPATH_NAME = "./div[1]/h3"
XPATH_CITY = "./div[contains(@class,'companyListItemContent')]/p[1]"
XPATH_CATEGORY_TEXT = (
    "./div[contains(@class,'companyListItemContent')]/p[2]/text()"
)

WHITESPACE_RE = re.compile(r"\s+")


def configure_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def make_session(user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def fetch_html(url: str, user_agent: str, timeout: float) -> str:
    session = make_session(user_agent)
    LOGGER.info("Fetching %s", url)
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def load_html_from_file(path: Path) -> str:
    LOGGER.info("Reading HTML from %s", path)
    return path.read_text(encoding="utf-8", errors="replace")


def normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def text_from_xpath(element, xpath: str) -> str:
    nodes = element.xpath(xpath)
    if not nodes:
        return ""
    if isinstance(nodes[0], str):
        parts = [n for n in nodes if isinstance(n, str) and n.strip()]
        return normalize_text("".join(parts))
    return normalize_text(nodes[0].text_content())


def iter_company_list_fragments(page_html: str) -> Iterable[str]:
    """
    Each <li> is parsed in isolation because the page HTML uses <div> where
    </div> is expected before </li>, which breaks a single-document DOM tree.
    """
    parts = page_html.split(LI_FRAGMENT_MARKER)
    for part in parts[1:]:
        body, _, _rest = part.partition("</li>")
        yield f"{LI_FRAGMENT_MARKER}{body}</li>"


def parse_company_item(item_html: str) -> Optional[dict[str, str]]:
    item = lxml_html.fromstring(item_html)
    name = html.unescape(text_from_xpath(item, XPATH_NAME))
    city = html.unescape(text_from_xpath(item, XPATH_CITY))
    category = html.unescape(text_from_xpath(item, XPATH_CATEGORY_TEXT))

    if not name and not city and not category:
        return None

    return {"name": name, "city": city, "category": category}


def parse_companies(page_html: str) -> List[dict[str, str]]:
    fragments = list(iter_company_list_fragments(page_html))
    LOGGER.info("Found %d company list items", len(fragments))

    rows: List[dict[str, str]] = []
    for fragment in fragments:
        row = parse_company_item(fragment)
        if row:
            rows.append(row)

    return rows


def write_csv(rows: List[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    LOGGER.info("Wrote %d rows to %s", len(rows), output_path)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Photovoltaikforum business directory to CSV.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"Page URL to fetch (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--html-file",
        type=Path,
        default=None,
        help="Parse local HTML file instead of fetching the URL",
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
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.debug)

    if args.html_file:
        if not args.html_file.exists():
            LOGGER.error("HTML file not found: %s", args.html_file)
            return 1
        page_html = load_html_from_file(args.html_file)
    else:
        try:
            page_html = fetch_html(args.url, args.user_agent, args.timeout)
        except requests.RequestException as exc:
            LOGGER.error("Failed to fetch page: %s", exc)
            return 1

    rows = parse_companies(page_html)
    if not rows:
        LOGGER.error("No companies parsed from page")
        return 1

    write_csv(rows, args.output)
    print(f"Parsed {len(rows)} companies -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
