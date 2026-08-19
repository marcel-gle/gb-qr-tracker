#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency
    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from list_processing.llm.local_mlstudio import LocalMLStudioClient


LOGGER = logging.getLogger("scrape_agenturmarkt_impressum")

DEFAULT_START_URL = "https://www.agenturmarkt.de/agenturen"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

EMAIL_MASK_RE = re.compile(r"\.([^<]+)<!AT([^#>]+)#\.?([^>]+)>")
EMAIL_FALLBACK_RE = re.compile(
    r"""[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}""",
    re.IGNORECASE,
)
HRB_RE = re.compile(r"\b(?:HRB|HRA|PR|GnR)\s*[\d/]+\b", re.IGNORECASE)

# Agenturmarkt exposes SEO landing pages per category at
# https://www.agenturmarkt.de/agenturen/<slug>. These paginate reliably via
# ?page=N over plain HTTP, so we prefer them over the JS/Livewire filter flow.
# Mapping: Agenturmarkt category_id -> landing page slug (verified to return 200).
CATEGORY_LANDING_SLUGS: Dict[int, str] = {
    1: "werbeagentur",
    3: "marketing-agentur",
    4: "seo-agentur",
    5: "webdesign-agentur",
    6: "social-media-agentur",
    7: "content-marketing-agentur",
    8: "eventagentur",
    9: "branding-agentur",
    10: "kreativagentur",
    11: "full-service-agentur",
    12: "media-agentur",
    13: "digital-agentur",
    14: "e-commerce-agentur",
    15: "app-agentur",
    16: "beratungsagentur",
    17: "software-agentur",
    18: "saas-agentur",
    19: "grafikdesign-agentur",
    22: "performance-marketing-agentur",
    24: "filmproduktion-agentur",
    28: "webentwicklung-agentur",
    29: "online-marketing-agentur",
    30: "it-sicherheitsagentur",
    34: "influencer-agentur",
    35: "ppc-agentur",
    36: "e-mail-marketing-agentur",
    38: "ki-agentur",
    40: "employer-branding-agentur",
    41: "recruiting-agentur",
    44: "kommunikationsagentur",
    47: "consulting-agentur",
    53: "coaching-agentur",
    381: "pr-agentur",
}

# Landing-page slugs without a verified Livewire category_id (HTTP scrape only).
# Verified 200 responses on agenturmarkt.de/agenturen/<slug>.
EXTRA_CATEGORY_SLUGS: Tuple[str, ...] = (
    "casting-agentur",
    "sprecheragentur",
    "vertrieb-agentur",
    "uiux-design-agentur",
    "model-agentur",
    "crm-agentur",
)

# Marketing / SEO / advertising categories to skip when --exclude-marketing is set.
MARKETING_SEO_EXCLUDE_SLUGS: Tuple[str, ...] = (
    "marketing-agentur",
    "online-marketing-agentur",
    "seo-agentur",
    "performance-marketing-agentur",
    "ppc-agentur",
    "content-marketing-agentur",
    "e-mail-marketing-agentur",
    "social-media-agentur",
    "influencer-agentur",
    "werbeagentur",
    "media-agentur",
    "branding-agentur",
    "employer-branding-agentur",
    "pr-agentur",
    "kommunikationsagentur",
    "digital-agentur",
    "full-service-agentur",
    "kreativagentur",
)

# Friendly names / aliases that resolve to a category_id.
CATEGORY_NAME_ALIASES: Dict[str, int] = {
    "webdesign": 5,
    "webdesigner": 5,
    "webdesign-agentur": 5,
    "webentwicklung": 28,
    "webentwickler": 28,
    "grafikdesign": 19,
    "seo": 4,
    "werbung": 1,
    "werbeagentur": 1,
    "marketing": 3,
    "socialmedia": 6,
    "social-media": 6,
    "onlinemarketing": 29,
    "online-marketing": 29,
    "fullservice": 11,
    "full-service": 11,
    "kreativ": 10,
    "digital": 13,
    "ecommerce": 14,
    "e-commerce": 14,
    "pr": 381,
    "ki": 38,
    "ai": 38,
}

# Slug aliases that map to EXTRA_CATEGORY_SLUGS (or known landing slugs).
CATEGORY_SLUG_ALIASES: Dict[str, str] = {
    "casting": "casting-agentur",
    "sprecher": "sprecheragentur",
    "vertrieb": "vertrieb-agentur",
    "uiux": "uiux-design-agentur",
    "ui-ux": "uiux-design-agentur",
    "ui-ux-design": "uiux-design-agentur",
    "ui-ux-design-agentur": "uiux-design-agentur",
    "model": "model-agentur",
    "crm": "crm-agentur",
}


def slugify_category(value: str) -> str:
    """Best-effort slug from a category name, matching Agenturmarkt's convention."""
    text = value.strip().lower()
    replacements = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def all_known_category_slugs() -> List[str]:
    """Stable sorted list of all scrapeable category landing slugs."""
    slugs = set(CATEGORY_LANDING_SLUGS.values())
    slugs.update(EXTRA_CATEGORY_SLUGS)
    return sorted(slugs)


def resolve_category_id(category: Optional[str], category_id: Optional[int]) -> Optional[int]:
    """Resolve an effective category_id from a name/alias/slug or explicit id."""
    if category_id is not None:
        return category_id
    if not category:
        return None
    key = category.strip().lower()
    if key in CATEGORY_NAME_ALIASES:
        return CATEGORY_NAME_ALIASES[key]
    slug = slugify_category(category)
    slug = CATEGORY_SLUG_ALIASES.get(slug, slug)
    for cid, known_slug in CATEGORY_LANDING_SLUGS.items():
        if known_slug == slug:
            return cid
    return None


def resolve_category_landing_slug(
    category: Optional[str], category_id: Optional[int]
) -> Optional[str]:
    """Return the landing-page slug for a category name/alias/slug or id, if known."""
    cid = resolve_category_id(category, category_id)
    if cid is not None and cid in CATEGORY_LANDING_SLUGS:
        return CATEGORY_LANDING_SLUGS[cid]
    if category:
        slug = slugify_category(category)
        slug = CATEGORY_SLUG_ALIASES.get(slug, slug)
        known = set(CATEGORY_LANDING_SLUGS.values()) | set(EXTRA_CATEGORY_SLUGS)
        if slug in known:
            return slug
    return None


LLM_SYSTEM_PROMPT = """
You extract legal imprint data from German company text.

Return exactly one JSON object with this schema:
{
  "geschaeftsfuehrer_name": "string or null",
  "geschaeftsfuehrer_anrede": "Herr or Frau or null",
  "telefon": "string or null",
  "strasse": "string or null",
  "hausnummer": "string or null",
  "postleitzahl": "string or null",
  "stadt": "string or null",
  "hrb_handelsregister_nummer": "string or null",
  "confidence": 0.0
}

Rules:
- Use only data present in the text.
- If unknown, set null.
- Keep output strictly JSON, no markdown.
""".strip()


@dataclass
class AgenturmarktRow:
    business_name: str
    slogan: str
    agenturmarkt_url: str
    email: str
    domain: str
    category: str = ""


@dataclass
class ImpressumResult:
    geschaeftsfuehrer_name: str = ""
    geschaeftsfuehrer_anrede: str = ""
    telefon: str = ""
    strasse: str = ""
    hausnummer: str = ""
    postleitzahl: str = ""
    stadt: str = ""
    hrb_handelsregister_nummer: str = ""

    def has_any_field(self) -> bool:
        return any(
            [
                self.geschaeftsfuehrer_name,
                self.geschaeftsfuehrer_anrede,
                self.telefon,
                self.strasse,
                self.hausnummer,
                self.postleitzahl,
                self.stadt,
                self.hrb_handelsregister_nummer,
            ]
        )


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


def request_html(session: requests.Session, url: str, timeout: float) -> Optional[str]:
    try:
        resp = session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        LOGGER.debug("Request failed for %s: %s", url, exc)
        return None
    if resp.status_code != 200:
        LOGGER.debug("Non-200 status %s for %s", resp.status_code, url)
        return None
    if "text/html" not in resp.headers.get("Content-Type", ""):
        LOGGER.debug("Non-HTML content for %s", url)
        return None
    return resp.text


def url_with_page(start_url: str, page: int) -> str:
    parsed = urlparse(start_url)
    query = parse_qs(parsed.query)
    query["page"] = [str(page)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


def decode_masked_email(masked: str) -> Optional[str]:
    """
    Decodes strings like:
    .de<!ATdieterhomburg#.info> -> info@dieterhomburg.de
    .solutions<!AT3plus#.marketing> -> marketing@3plus.solutions
    """
    if not masked:
        return None
    match = EMAIL_MASK_RE.search(masked)
    if not match:
        return None
    tld, domain_part, local_part = match.groups()
    tld = tld.strip(". ")
    domain_part = domain_part.strip()
    local_part = local_part.strip(". ")
    if not tld or not domain_part or not local_part:
        return None
    return f"{local_part}@{domain_part}.{tld}".lower()


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


def parse_result_card(card: BeautifulSoup) -> AgenturmarktRow:
    name_el = card.select_one("h3.name")
    slogan_el = card.select_one("p.catchphrase")
    profile_anchor = card.select_one("a.item-link[href]")

    website_anchor = None
    for attr in card.select("div.attribute"):
        icon = attr.select_one("i.fa-globe")
        if icon is not None:
            website_anchor = attr.select_one("a[href]")
            if website_anchor:
                break

    email = ""
    email_attr = card.select_one("div.attribute[x-data*='<!AT']")
    if email_attr is not None:
        x_data = email_attr.get("x-data", "")
        decoded = decode_masked_email(x_data)
        if decoded:
            email = decoded

    if not email:
        # Last-resort fallback from visible card text.
        text_email = EMAIL_FALLBACK_RE.search(card.get_text(" ", strip=True))
        if text_email:
            email = text_email.group(0).lower()

    domain = ""
    if website_anchor is not None:
        href = website_anchor.get("href", "").strip()
        domain = normalize_domain(href)
        if not domain:
            domain = normalize_domain(website_anchor.get_text(" ", strip=True))
    if not domain and email:
        domain = normalize_domain(email)

    agenturmarkt_url = ""
    if profile_anchor is not None:
        agenturmarkt_url = profile_anchor.get("href", "").strip()

    return AgenturmarktRow(
        business_name=(name_el.get_text(" ", strip=True) if name_el else ""),
        slogan=(slogan_el.get_text(" ", strip=True) if slogan_el else ""),
        agenturmarkt_url=agenturmarkt_url,
        email=email,
        domain=domain,
    )


def extract_rows_from_page(html: str) -> List[AgenturmarktRow]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.company-list-item")
    rows: List[AgenturmarktRow] = []
    for card in cards:
        row = parse_result_card(card)
        if row.business_name and row.agenturmarkt_url:
            rows.append(row)
    return rows


def scrape_agenturmarkt_rows(
    session: requests.Session,
    start_url: str,
    max_results: int,
    timeout: float,
    delay: float,
    debug: bool,
) -> List[AgenturmarktRow]:
    collected: List[AgenturmarktRow] = []
    seen_keys: set[str] = set()
    page = 1

    progress = tqdm(total=max_results, desc="Agenturmarkt scrape", unit="row")
    try:
        while len(collected) < max_results:
            page_url = url_with_page(start_url, page)
            if debug:
                LOGGER.debug("Fetching result page: %s", page_url)

            html = request_html(session, page_url, timeout)
            if not html:
                LOGGER.info("Stopping pagination: failed to fetch page %s", page)
                break

            page_rows = extract_rows_from_page(html)
            if debug:
                LOGGER.debug("Page %s extracted %s cards", page, len(page_rows))
            if not page_rows:
                LOGGER.info("Stopping pagination: no cards found at page %s", page)
                break

            added_this_page = 0
            for row in page_rows:
                dedupe_key = row.agenturmarkt_url or f"{row.business_name}|{row.domain}"
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                collected.append(row)
                added_this_page += 1
                progress.update(1)
                if len(collected) >= max_results:
                    break

            if debug:
                LOGGER.debug("Page %s added %s new rows", page, added_this_page)
            if added_this_page == 0:
                LOGGER.info("Stopping pagination: no new rows at page %s", page)
                break

            page += 1
            if delay > 0:
                time.sleep(delay)
    finally:
        progress.close()

    return collected[:max_results]


def _apply_agenturmarkt_filters_livewire(
    page: Any,
    *,
    category_id: Optional[int],
    service_id: Optional[int],
    agentur_score: Optional[float],
    debug: bool,
) -> bool:
    root = page.locator("div[wire\\:name='pages.website.page-search-results']").first
    if root.count() == 0:
        return False
    component_id = root.get_attribute("wire:id")
    if not component_id:
        return False
    if debug:
        LOGGER.debug(
            "Applying filters via Livewire: category_id=%s service_id=%s agentur_score=%s",
            category_id,
            service_id,
            agentur_score,
        )
    return bool(
        page.evaluate(
            """
            async ({ componentId, categoryId, serviceId, agenturScore }) => {
                if (!window.Livewire || !window.Livewire.find) return false;
                const component = window.Livewire.find(componentId);
                if (!component) return false;
                if (categoryId !== null) {
                    await component.set('category_id', Number(categoryId));
                }
                if (serviceId !== null) {
                    await component.set('service_id', Number(serviceId));
                }
                if (agenturScore !== null) {
                    await component.set('filter_rating', Number(agenturScore));
                }
                return true;
            }
            """,
            {
                "componentId": component_id,
                "categoryId": category_id,
                "serviceId": service_id,
                "agenturScore": agentur_score,
            },
        )
    )


def _first_card_key(page: Any) -> str:
    anchor = page.locator("div.company-list-item a.item-link").first
    if anchor.count() == 0:
        return ""
    return anchor.get_attribute("href") or ""


def _advance_to_next_page_livewire(
    page: Any,
    prev_first_key: str,
    timeout: float,
    debug: bool,
) -> bool:
    """Advance to the next result page via Livewire and wait for the list to change.

    Returns False when there is no next page or the list did not update in time,
    which the caller uses as a natural stop condition.
    """
    root = page.locator("div[wire\\:name='pages.website.page-search-results']").first
    if root.count() == 0:
        return False
    component_id = root.get_attribute("wire:id")
    if not component_id:
        return False
    called = bool(
        page.evaluate(
            """
            async ({ componentId }) => {
                if (!window.Livewire || !window.Livewire.find) return false;
                const component = window.Livewire.find(componentId);
                if (!component) return false;
                await component.call('nextPage', 'page');
                return true;
            }
            """,
            {"componentId": component_id},
        )
    )
    if not called:
        if debug:
            LOGGER.debug("Livewire nextPage call did not execute.")
        return False
    try:
        page.wait_for_function(
            """
            (prevKey) => {
                const a = document.querySelector('div.company-list-item a.item-link');
                return a && a.getAttribute('href') !== prevKey;
            }
            """,
            arg=prev_first_key,
            timeout=int(timeout * 1000),
        )
    except Exception:
        if debug:
            LOGGER.debug("Result list did not change after nextPage (likely last page).")
        return False
    return True


def scrape_agenturmarkt_rows_browser(
    start_url: str,
    max_results: int,
    timeout: float,
    delay: float,
    debug: bool,
    user_agent: str,
    *,
    category_id: Optional[int],
    service_id: Optional[int],
    agentur_score: Optional[float],
) -> List[AgenturmarktRow]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Playwright is required for filtered scraping. Install with: "
            "pip install playwright && playwright install chromium"
        ) from exc

    collected: List[AgenturmarktRow] = []
    seen_keys: set[str] = set()

    progress = tqdm(total=max_results, desc="Agenturmarkt scrape", unit="row")
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(user_agent=user_agent)
            page.goto(start_url, wait_until="networkidle", timeout=int(timeout * 1000))

            if category_id is not None or service_id is not None or agentur_score is not None:
                ok = _apply_agenturmarkt_filters_livewire(
                    page,
                    category_id=category_id,
                    service_id=service_id,
                    agentur_score=agentur_score,
                    debug=debug,
                )
                if not ok:
                    LOGGER.warning(
                        "Could not apply filters through Livewire state; "
                        "results may be unfiltered."
                    )
                # Wait for the filtered result list to settle before reading it.
                try:
                    page.wait_for_load_state("networkidle", timeout=int(timeout * 1000))
                except Exception:
                    page.wait_for_timeout(1000)

            page_index = 0
            while len(collected) < max_results:
                html = page.content()
                page_rows = extract_rows_from_page(html)
                page_index += 1
                if debug:
                    LOGGER.debug(
                        "Browser page %s extracted %s cards", page_index, len(page_rows)
                    )
                if not page_rows:
                    LOGGER.info("Stopping pagination: no cards on page %s", page_index)
                    break

                first_key_before = page_rows[0].agenturmarkt_url or _first_card_key(page)

                added_this_page = 0
                for row in page_rows:
                    dedupe_key = row.agenturmarkt_url or f"{row.business_name}|{row.domain}"
                    if dedupe_key in seen_keys:
                        continue
                    seen_keys.add(dedupe_key)
                    collected.append(row)
                    added_this_page += 1
                    progress.update(1)
                    if len(collected) >= max_results:
                        break

                if debug:
                    LOGGER.debug(
                        "Browser page %s added %s new rows", page_index, added_this_page
                    )
                if len(collected) >= max_results:
                    break
                if added_this_page == 0:
                    LOGGER.warning(
                        "Stopping pagination: only duplicate rows on page %s "
                        "(next page did not load new results)",
                        page_index,
                    )
                    break

                if not _advance_to_next_page_livewire(
                    page, first_key_before, timeout, debug
                ):
                    LOGGER.info(
                        "Stopping pagination: no further pages after page %s", page_index
                    )
                    break
                if delay > 0:
                    time.sleep(delay)

            browser.close()
    finally:
        progress.close()

    return collected[:max_results]


def get_base_url_for_domain(
    session: requests.Session,
    domain: str,
    timeout: float,
) -> Optional[str]:
    domain = normalize_domain(domain)
    if not domain:
        return None
    for scheme in ("https://", "http://"):
        test_url = f"{scheme}{domain}"
        try:
            resp = session.get(test_url, timeout=timeout)
        except requests.RequestException:
            continue
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            return resp.url
    return None


def find_imprint_url(
    session: requests.Session,
    base_html: str,
    base_url: str,
    timeout: float,
) -> Optional[str]:
    soup = BeautifulSoup(base_html, "html.parser")
    candidates: List[str] = []
    for anchor in soup.find_all("a", href=True):
        text = (anchor.get_text() or "").lower()
        href = anchor["href"].lower()
        if "impressum" in text or "impressum" in href or "imprint" in text or "imprint" in href:
            candidates.append(anchor["href"])

    for href in candidates:
        full_url = urljoin(base_url, href)
        html = request_html(session, full_url, timeout)
        if html:
            return full_url

    common_paths = [
        "/impressum",
        "/impressum/",
        "/imprint",
        "/imprint/",
        "/impressum.html",
        "/kontakt/impressum",
        "/kontakt",
    ]
    for path in common_paths:
        full_url = urljoin(base_url, path)
        html = request_html(session, full_url, timeout)
        if html:
            return full_url
    return None


def extract_text_from_url(session: requests.Session, url: str, timeout: float) -> Optional[str]:
    html = request_html(session, url, timeout)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    return text[:15000]


def _json_safe_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def infer_anrede_with_llm(client: LocalMLStudioClient, name: str) -> str:
    clean_name = _json_safe_str(name)
    if not clean_name:
        return ""
    user_prompt = (
        "Infer the German salutation from this full name.\n"
        f"Name: {clean_name}\n\n"
        "Return JSON only: {\"geschaeftsfuehrer_anrede\":\"Herr|Frau|null\"}"
    )
    try:
        content = client.chat(
            system_prompt=(
                "You infer German salutations from names. "
                "Return only 'Herr', 'Frau', or null in JSON."
            ),
            user_prompt=user_prompt,
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        payload = json.loads(content)
    except Exception:
        return ""
    anrede = _json_safe_str(payload.get("geschaeftsfuehrer_anrede"))
    return anrede if anrede in {"Herr", "Frau"} else ""


def llm_extract_impressum(client: LocalMLStudioClient, text: str, domain: str) -> ImpressumResult:
    prompt = (
        f"Domain: {domain}\n\n"
        "Impressum/Kontakt text:\n"
        f"{text}\n\n"
        "Return only the required JSON object."
    )
    fallback = ImpressumResult()
    content = client.chat(
        system_prompt=LLM_SYSTEM_PROMPT,
        user_prompt=prompt,
        response_format={"type": "json_object"},
        temperature=0.0,
    )
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return fallback

    hrb = _json_safe_str(payload.get("hrb_handelsregister_nummer"))
    if not hrb:
        text_blob = " ".join(_json_safe_str(v) for v in payload.values())
        match = HRB_RE.search(text_blob)
        if match:
            hrb = match.group(0)

    geschaeftsfuehrer_name = _json_safe_str(payload.get("geschaeftsfuehrer_name"))
    geschaeftsfuehrer_anrede = _json_safe_str(payload.get("geschaeftsfuehrer_anrede"))
    if geschaeftsfuehrer_anrede not in {"Herr", "Frau"}:
        geschaeftsfuehrer_anrede = ""
    if not geschaeftsfuehrer_anrede and geschaeftsfuehrer_name:
        geschaeftsfuehrer_anrede = infer_anrede_with_llm(client, geschaeftsfuehrer_name)

    return ImpressumResult(
        geschaeftsfuehrer_name=geschaeftsfuehrer_name,
        geschaeftsfuehrer_anrede=geschaeftsfuehrer_anrede,
        telefon=_json_safe_str(payload.get("telefon")),
        strasse=_json_safe_str(payload.get("strasse")),
        hausnummer=_json_safe_str(payload.get("hausnummer")),
        postleitzahl=_json_safe_str(payload.get("postleitzahl")),
        stadt=_json_safe_str(payload.get("stadt")),
        hrb_handelsregister_nummer=hrb,
    )


def scrape_impressum_for_domain(
    domain: str,
    client: LocalMLStudioClient,
    timeout: float,
    delay: float,
    debug: bool,
    user_agent: str,
) -> ImpressumResult:
    if not domain:
        return ImpressumResult()
    session = make_session(user_agent)
    base_url = get_base_url_for_domain(session, domain, timeout)
    if not base_url:
        if debug:
            LOGGER.debug("No reachable base URL for domain: %s", domain)
        return ImpressumResult()
    home_html = request_html(session, base_url, timeout)
    if not home_html:
        return ImpressumResult()
    imprint_url = find_imprint_url(session, home_html, base_url, timeout)
    if debug:
        LOGGER.debug("Domain %s resolved imprint URL: %s", domain, imprint_url)
    text = None
    if imprint_url:
        text = extract_text_from_url(session, imprint_url, timeout)
        if delay > 0:
            time.sleep(delay)
    if not text:
        text = BeautifulSoup(home_html, "html.parser").get_text(separator="\n")[:15000]
    if not text:
        return ImpressumResult()
    try:
        return llm_extract_impressum(client, text, domain)
    except Exception as exc:  # pragma: no cover - defensive
        if debug:
            LOGGER.debug("LLM extraction failed for %s: %s", domain, exc)
        return ImpressumResult()


def enrich_rows_with_impressum(
    rows: List[AgenturmarktRow],
    client: LocalMLStudioClient,
    max_workers: int,
    timeout: float,
    delay: float,
    debug: bool,
    user_agent: str,
) -> List[Tuple[AgenturmarktRow, ImpressumResult]]:
    domain_to_result: Dict[str, ImpressumResult] = {}
    unique_domains = sorted({row.domain for row in rows if row.domain})

    progress = tqdm(total=len(unique_domains), desc="Impressum enrich", unit="domain")
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    scrape_impressum_for_domain,
                    domain,
                    client,
                    timeout,
                    delay,
                    debug,
                    user_agent,
                ): domain
                for domain in unique_domains
            }
            for future in as_completed(future_map):
                domain = future_map[future]
                try:
                    domain_to_result[domain] = future.result()
                except Exception as exc:  # pragma: no cover - defensive
                    if debug:
                        LOGGER.debug("Domain job failed for %s: %s", domain, exc)
                    domain_to_result[domain] = ImpressumResult()
                progress.update(1)
    finally:
        progress.close()

    output: List[Tuple[AgenturmarktRow, ImpressumResult]] = []
    for row in rows:
        output.append((row, domain_to_result.get(row.domain, ImpressumResult())))
    return output


def write_csv(output_csv: str, enriched_rows: Iterable[Tuple[AgenturmarktRow, ImpressumResult]]) -> int:
    fieldnames = [
        "business_name",
        "slogan",
        "agenturmarkt_url",
        "email",
        "domain",
        "category",
        "company_name",
        "gegenstand",
        "geschaeftsfuehrer_name",
        "geschaeftsfuehrer_anrede",
        "telefon",
        "strasse",
        "hausnummer",
        "postleitzahl",
        "stadt",
        "hrb_handelsregister_nummer",
    ]
    count = 0
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for base, imp in enriched_rows:
            writer.writerow(
                {
                    "business_name": base.business_name,
                    "slogan": base.slogan,
                    "agenturmarkt_url": base.agenturmarkt_url,
                    "email": base.email,
                    "domain": base.domain,
                    "category": base.category,
                    "company_name": base.business_name,
                    "gegenstand": base.category,
                    "geschaeftsfuehrer_name": imp.geschaeftsfuehrer_name,
                    "geschaeftsfuehrer_anrede": imp.geschaeftsfuehrer_anrede,
                    "telefon": imp.telefon,
                    "strasse": imp.strasse,
                    "hausnummer": imp.hausnummer,
                    "postleitzahl": imp.postleitzahl,
                    "stadt": imp.stadt,
                    "hrb_handelsregister_nummer": imp.hrb_handelsregister_nummer,
                }
            )
            count += 1
    return count


def print_stats(enriched_rows: List[Tuple[AgenturmarktRow, ImpressumResult]]) -> None:
    fields = [
        "geschaeftsfuehrer_name",
        "geschaeftsfuehrer_anrede",
        "telefon",
        "strasse",
        "hausnummer",
        "postleitzahl",
        "stadt",
        "hrb_handelsregister_nummer",
    ]
    total = len(enriched_rows)
    populated_rows = sum(1 for _, imp in enriched_rows if imp.has_any_field())

    print("\n=== Run statistics ===")
    print(f"Total rows: {total}")
    print(f"Rows with at least one impressum field: {populated_rows}")
    print(f"Rows without impressum fields: {total - populated_rows}")
    print("\nMissing values by field:")

    for field in fields:
        missing = 0
        for _, imp in enriched_rows:
            if not getattr(imp, field):
                missing += 1
        missing_rate = (missing / total * 100.0) if total else 0.0
        print(f"- {field}: missing {missing}/{total} ({missing_rate:.2f}%)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape agenturmarkt list pages and enrich with impressum data via local ML Studio."
    )
    parser.add_argument("--start-url", default=DEFAULT_START_URL, help="Agenturmarkt search/list URL")
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Maximum number of rows to scrape (required unless --list-categories)",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Output CSV file path (required unless --list-categories)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--request-timeout", type=float, default=10.0, help="HTTP request timeout in seconds")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between requests in seconds")
    parser.add_argument("--max-workers-impressum", type=int, default=5, help="Worker count for impressum stage")
    parser.add_argument(
        "--category",
        default=None,
        help=(
            "Kategorie als Name/Alias oder Slug (z. B. 'webdesign' oder "
            "'webdesign-agentur'). Nutzt die zuverlaessige Kategorie-Landingpage "
            "mit HTTP-Pagination."
        ),
    )
    parser.add_argument(
        "--categories",
        default=None,
        help=(
            "Komma-getrennte Kategorien (Name/Alias/Slug). "
            "max-results wird gleichmaessig aufgeteilt und global dedupliziert."
        ),
    )
    parser.add_argument(
        "--all-categories",
        action="store_true",
        help="Alle bekannten Kategorie-Landingpages scrapen (siehe --list-categories).",
    )
    parser.add_argument(
        "--exclude-categories",
        default=None,
        help="Komma-getrennte Kategorien die ausgelassen werden (Name/Alias/Slug).",
    )
    parser.add_argument(
        "--exclude-marketing",
        action="store_true",
        help=(
            "Marketing-/SEO-/Werbe-Kategorien auslassen "
            "(marketing, seo, ppc, social-media, werbung, branding, PR, …)."
        ),
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="Bekannte Kategorie-Slugs ausgeben und beenden.",
    )
    parser.add_argument("--category-id", type=int, default=None, help="Agenturmarkt Kategorie filter ID")
    parser.add_argument("--service-id", type=int, default=None, help="Agenturmarkt Dienstleistung filter ID")
    parser.add_argument("--agentur-score", type=float, default=None, help="Agenturmarkt Agentur-Score filter")
    parser.add_argument(
        "--filter-mode",
        choices=["auto", "http", "browser"],
        default="auto",
        help="How to apply list filters (browser required when filters are set).",
    )
    parser.add_argument(
        "--skip-impressum",
        action="store_true",
        help="Nur Agenturmarkt-Listen scrapen (kein ML-Studio / kein Domain-Impressum).",
    )
    parser.add_argument(
        "--mlstudio-base-url",
        default=os.environ.get("ML_STUDIO_BASE_URL", "http://localhost:1234/v1"),
        help="Local ML Studio base URL",
    )
    parser.add_argument(
        "--local-model",
        default=os.environ.get("LOCAL_MODEL", "openai/gpt-oss-20b"),
        help="Local model name served by ML Studio",
    )
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP User-Agent")
    return parser.parse_args()


def _parse_category_tokens(raw: str) -> List[str]:
    slugs: List[str] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        slug = resolve_category_landing_slug(token, None)
        if not slug:
            raise ValueError(f"Unbekannte Kategorie: {token!r}")
        if slug not in slugs:
            slugs.append(slug)
    return slugs


def _resolve_requested_category_slugs(args: argparse.Namespace) -> List[str]:
    if args.all_categories:
        slugs = all_known_category_slugs()
    elif args.categories:
        slugs = _parse_category_tokens(args.categories)
        if not slugs:
            raise ValueError("--categories ist leer")
    elif args.category or args.category_id is not None:
        slug = resolve_category_landing_slug(args.category, args.category_id)
        if slug:
            slugs = [slug]
        else:
            # Fall back to single-category browser path via category_id.
            return []
    else:
        return []

    exclude: set[str] = set()
    if args.exclude_marketing:
        exclude.update(MARKETING_SEO_EXCLUDE_SLUGS)
    if args.exclude_categories:
        exclude.update(_parse_category_tokens(args.exclude_categories))
    if exclude:
        before = len(slugs)
        slugs = [s for s in slugs if s not in exclude]
        LOGGER.info(
            "Excluded %s categories (%s remaining): %s",
            before - len(slugs),
            len(slugs),
            ", ".join(sorted(exclude)),
        )
    return slugs


def _scrape_one_category(
    *,
    slug: Optional[str],
    category_id: Optional[int],
    args: argparse.Namespace,
    max_results: int,
    session: Optional[requests.Session],
) -> List[AgenturmarktRow]:
    has_browser_only_filters = (
        args.service_id is not None or args.agentur_score is not None
    )
    custom_start_url = args.start_url != DEFAULT_START_URL
    start_url = args.start_url
    effective_category_id = category_id

    if args.filter_mode == "browser":
        use_browser = True
    elif args.filter_mode == "http":
        use_browser = False
        if slug and not custom_start_url:
            start_url = f"{DEFAULT_START_URL}/{slug}"
        if has_browser_only_filters:
            LOGGER.warning(
                "--service-id/--agentur-score erfordern den Browser-Modus und "
                "werden bei --filter-mode http ignoriert."
            )
    else:  # auto
        if has_browser_only_filters:
            use_browser = True
        elif slug and not custom_start_url:
            use_browser = False
            start_url = f"{DEFAULT_START_URL}/{slug}"
            LOGGER.info(
                "Nutze Kategorie-Landingpage fuer zuverlaessige HTTP-Pagination: %s",
                start_url,
            )
        elif effective_category_id is not None:
            use_browser = True
        else:
            use_browser = False

    if use_browser:
        rows = scrape_agenturmarkt_rows_browser(
            start_url=start_url,
            max_results=max_results,
            timeout=args.request_timeout,
            delay=args.delay,
            debug=args.debug,
            user_agent=args.user_agent,
            category_id=effective_category_id,
            service_id=args.service_id,
            agentur_score=args.agentur_score,
        )
    else:
        assert session is not None
        rows = scrape_agenturmarkt_rows(
            session=session,
            start_url=start_url,
            max_results=max_results,
            timeout=args.request_timeout,
            delay=args.delay,
            debug=args.debug,
        )

    if slug:
        for row in rows:
            row.category = slug
    return rows


def main() -> None:
    args = parse_args()
    configure_logging(args.debug)

    if args.list_categories:
        for slug in all_known_category_slugs():
            print(slug)
        return

    if args.max_results is None:
        raise ValueError("--max-results is required")
    if not args.output_csv:
        raise ValueError("--output-csv is required")
    if args.max_results <= 0:
        raise ValueError("--max-results must be > 0")

    LOGGER.info("Starting Agenturmarkt scrape (max_results=%s)", args.max_results)

    category_slugs = _resolve_requested_category_slugs(args)
    if args.category and not category_slugs and args.category_id is None:
        LOGGER.warning(
            "Kategorie '%s' konnte keiner bekannten Landingpage zugeordnet werden.",
            args.category,
        )

    collected: List[AgenturmarktRow] = []
    seen_keys: set[str] = set()
    session = make_session(args.user_agent)

    if category_slugs:
        per_category = max(1, args.max_results // len(category_slugs))
        remainder = args.max_results % len(category_slugs)
        LOGGER.info(
            "Scraping %s categories (~%s rows each, total cap %s)",
            len(category_slugs),
            per_category,
            args.max_results,
        )
        for idx, slug in enumerate(category_slugs):
            if len(collected) >= args.max_results:
                break
            quota = per_category + (1 if idx < remainder else 0)
            remaining = args.max_results - len(collected)
            quota = min(quota, remaining)
            LOGGER.info("Category %s/%s: %s (quota=%s)", idx + 1, len(category_slugs), slug, quota)
            cid = resolve_category_id(slug, None)
            batch = _scrape_one_category(
                slug=slug,
                category_id=cid,
                args=args,
                max_results=quota,
                session=session,
            )
            added = 0
            for row in batch:
                dedupe_key = row.agenturmarkt_url or f"{row.business_name}|{row.domain}"
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                collected.append(row)
                added += 1
                if len(collected) >= args.max_results:
                    break
            LOGGER.info("Category %s added %s new rows (total=%s)", slug, added, len(collected))
    else:
        # Single run without category landing pages (optional browser Livewire filters).
        collected = _scrape_one_category(
            slug=None,
            category_id=resolve_category_id(args.category, args.category_id),
            args=args,
            max_results=args.max_results,
            session=session,
        )

    rows = collected[: args.max_results]
    LOGGER.info("Collected %s rows from Agenturmarkt", len(rows))

    if args.skip_impressum:
        enriched = [(row, ImpressumResult()) for row in rows]
        LOGGER.info("Skipping impressum enrichment (--skip-impressum)")
    else:
        client = LocalMLStudioClient(
            base_url=args.mlstudio_base_url,
            model=args.local_model,
            max_concurrent_requests=max(1, args.max_workers_impressum),
        )
        enriched = enrich_rows_with_impressum(
            rows=rows,
            client=client,
            max_workers=max(1, args.max_workers_impressum),
            timeout=args.request_timeout,
            delay=args.delay,
            debug=args.debug,
            user_agent=args.user_agent,
        )

    written = write_csv(args.output_csv, enriched)
    LOGGER.info("Wrote %s rows to %s", written, args.output_csv)
    print_stats(enriched)


if __name__ == "__main__":
    main()
