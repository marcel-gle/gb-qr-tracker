"""Imprint page fetch: static HTTP first, Playwright fallback for JS SPAs."""

from __future__ import annotations

import logging
import re
from threading import Lock
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10
MAX_TEXT_CHARS = 15_000
BROWSER_GOTO_TIMEOUT_MS = 15_000
BROWSER_SELECTOR_TIMEOUT_MS = 5_000

IMPRINT_KEYWORDS = (
    "handelsregister",
    "geschäftsführ",
    "geschaeftsfuehr",
    "vertretungsberechtigt",
    "umsatzsteuer-id",
    "umsatzsteuer id",
    "amtsgericht",
    "§ 5 ddg",
    "digitale-dienste-gesetz",
    "medienstaatsvertrag",
    "mstv",
    "tmg",
    "verantwortlich für den inhalt",
)

FALLBACK_IMPRINT_PATHS = (
    "/impressum",
    "/impressum/",
    "/impressum/impressum",
    "/impressum/impressum/",
    "/impressum.html",
    "/imprint",
    "/imprint/",
    "/imprint.html",
    "/kontakt/impressum",
    "/kontakt/impressum/",
    "/kontakt",
    "/kontakt/",
)

BROWSER_WAIT_SELECTORS = (
    "text=Handelsregister",
    "text=Geschäftsführ",
    "text=Geschaeftsfuehr",
    "text=Vertretungsberechtigt",
    "text=Impressum",
    "h1:has-text('Impressum')",
)

_browser_lock = Lock()
_playwright: Any = None
_browser: Any = None


def _fetch_url(url: str) -> Optional[requests.Response]:
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


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS]
    return text


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def _texts_similar(a: str, b: str, *, threshold: float = 0.85) -> bool:
    na = _normalize_text(a)[:2000]
    nb = _normalize_text(b)[:2000]
    if not na or not nb:
        return False
    if na == nb:
        return True
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    prefix_len = min(500, len(shorter))
    if longer.startswith(shorter[:prefix_len]):
        return len(shorter) / max(len(longer), 1) >= threshold
    return False


def _imprint_keyword_hits(text: str) -> int:
    lower = text.lower()
    return sum(1 for kw in IMPRINT_KEYWORDS if kw in lower)


def looks_like_imprint(text: str, *, home_text: str | None = None) -> bool:
    """Return True when extracted text plausibly contains legal Impressum content."""
    if not text or len(text.strip()) < 50:
        return False
    if home_text and _texts_similar(text, home_text):
        return False
    hits = _imprint_keyword_hits(text)
    if hits >= 2:
        return True
    if hits >= 1 and "impressum" in text.lower():
        return True
    return False


def _score_imprint_candidate(href: str, link_text: str) -> int:
    href_lower = href.lower().split("?", 1)[0].rstrip("/")
    text_lower = link_text.strip().lower()
    path = urlparse(href_lower).path.rstrip("/") or href_lower
    score = 0

    if path.endswith("/impressum/impressum") or path.endswith("/kontakt/impressum"):
        score += 120
    elif path.endswith("/impressum") or path.endswith("/imprint"):
        score += 100
    elif path.endswith("/impressum.html") or path.endswith("/imprint.html"):
        score += 95

    if text_lower in ("impressum", "imprint", "legal notice", "legal", "impressum & datenschutz"):
        score += 30

    if "/impressum/" in path and not path.endswith("/impressum"):
        if not path.endswith("/impressum/impressum"):
            score -= 60

    if any(x in path for x in ("/shop", "/strom", "/solar", "/ratgeber", "/blog", "/news")):
        score -= 40

    return score


def find_imprint_url(base_html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(base_html, "html.parser")
    ranked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip()
        href = a["href"]
        href_lower = href.lower()
        text_lower = text.lower()
        if not any(key in text_lower for key in ("impressum", "imprint")) and not any(
            key in href_lower for key in ("impressum", "imprint")
        ):
            continue
        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)
        ranked.append((_score_imprint_candidate(href, text), href))

    for _, href in sorted(ranked, key=lambda item: item[0], reverse=True):
        url = urljoin(base_url, href)
        resp = _fetch_url(url)
        if resp:
            return resp.url

    for path in FALLBACK_IMPRINT_PATHS:
        url = urljoin(base_url, path)
        resp = _fetch_url(url)
        if resp:
            return resp.url
    return None


def _ensure_browser():
    global _playwright, _browser
    if _browser is not None:
        return _browser
    from playwright.sync_api import sync_playwright

    _playwright = sync_playwright().start()
    _browser = _playwright.chromium.launch(headless=True)
    return _browser


def close_browser_pool() -> None:
    """Release shared Playwright browser resources (call after batch imprint scrape)."""
    global _playwright, _browser
    with _browser_lock:
        if _browser is not None:
            try:
                _browser.close()
            except Exception:
                pass
            _browser = None
        if _playwright is not None:
            try:
                _playwright.stop()
            except Exception:
                pass
            _playwright = None


def _fetch_text_browser(url: str) -> Optional[str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeout
    except ImportError:
        logger.warning(
            "Playwright not installed; skipping browser imprint fetch for %s. "
            "Install with: pip install playwright && playwright install chromium",
            url,
        )
        return None

    with _browser_lock:
        page = None
        try:
            browser = _ensure_browser()
            page = browser.new_page(user_agent=USER_AGENT)
            page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_GOTO_TIMEOUT_MS)
            for selector in BROWSER_WAIT_SELECTORS:
                try:
                    page.wait_for_selector(selector, timeout=BROWSER_SELECTOR_TIMEOUT_MS)
                    break
                except PlaywrightTimeout:
                    continue
            page.wait_for_timeout(500)
            text = page.inner_text("body")
            if len(text) > MAX_TEXT_CHARS:
                text = text[:MAX_TEXT_CHARS]
            return text
        except Exception as exc:
            logger.warning("Browser imprint fetch failed for %s: %s", url, exc)
            return None
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass


def extract_text_from_url(url: str, *, home_text: str | None = None) -> Optional[str]:
    resp = _fetch_url(url)
    static_text = _html_to_text(resp.text) if resp else None

    if static_text and looks_like_imprint(static_text, home_text=home_text):
        logger.debug("Imprint static fetch OK: %s", url)
        return static_text

    browser_text = _fetch_text_browser(url)
    if browser_text and looks_like_imprint(browser_text, home_text=home_text):
        logger.info("Imprint browser fetch OK: %s", url)
        return browser_text

    if browser_text and static_text and not looks_like_imprint(static_text, home_text=home_text):
        return browser_text

    return static_text or browser_text


def get_imprint_text_for_domain(domain: str) -> Optional[str]:
    base_url = normalize_domain_to_base_url(domain)
    if not base_url:
        return None
    home_resp = _fetch_url(base_url)
    if not home_resp:
        return None
    home_text = _html_to_text(home_resp.text)

    imprint_url = find_imprint_url(home_resp.text, base_url)
    if imprint_url:
        return extract_text_from_url(imprint_url, home_text=home_text)

    for path in FALLBACK_IMPRINT_PATHS[:6]:
        url = urljoin(base_url, path)
        text = extract_text_from_url(url, home_text=home_text)
        if text and looks_like_imprint(text, home_text=home_text):
            return text

    if looks_like_imprint(home_text):
        return home_text
    return extract_text_from_url(base_url, home_text=home_text)


def normalize_domain_to_base_url(domain: str) -> Optional[str]:
    domain = (domain or "").strip()
    if not domain:
        return None
    if domain.startswith("http://"):
        domain = domain[len("http://") :]
    elif domain.startswith("https://"):
        domain = domain[len("https://") :]
    domain = domain.split("/")[0].rstrip("/")
    for scheme in ("https://", "http://"):
        url = scheme + domain
        resp = _fetch_url(url)
        if resp:
            return resp.url
    return None
