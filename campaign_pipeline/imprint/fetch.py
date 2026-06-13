"""Imprint page fetch: static HTTP first, Playwright fallback for JS SPAs."""

from __future__ import annotations

import logging
import re
import warnings
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from threading import Lock
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, SSLError

from ..models import normalize_domain

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10
MAX_TEXT_CHARS = 15_000
BROWSER_GOTO_TIMEOUT_MS = 20_000
BROWSER_SELECTOR_TIMEOUT_MS = 5_000
BROWSER_CLOUDFLARE_WAIT_MS = 3_000

IMPRINT_KEYWORDS = (
    "impressum",
    "imprint",
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

_browser_executor: ThreadPoolExecutor | None = None
_browser_executor_lock = Lock()
_browser_pw_state: dict[str, Any] = {"playwright": None, "browser": None}


def _is_html_response(resp: requests.Response) -> bool:
    content_type = resp.headers.get("Content-Type", "").lower()
    if "text/html" in content_type or "application/xhtml" in content_type:
        return True
    if not content_type or content_type.startswith("text/"):
        snippet = (resp.text or "")[:800].lower()
        return "<html" in snippet or "<!doctype html" in snippet
    return False


def _fetch_url(url: str, *, verify: bool = True) -> Optional[requests.Response]:
    try:
        with warnings.catch_warnings():
            if not verify:
                warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT,
                verify=verify,
            )
        if 200 <= resp.status_code < 300 and _is_html_response(resp):
            return resp
    except SSLError as exc:
        if verify:
            logger.debug("SSL error for %s, retrying without certificate verify: %s", url, exc)
            return _fetch_url(url, verify=False)
    except RequestException:
        return None
    return None


def _candidate_hosts(domain: str) -> list[str]:
    host = (domain or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return []
    hosts = [host]
    if not host.startswith("www."):
        hosts.append(f"www.{host}")
    return list(dict.fromkeys(hosts))


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


def _get_browser_executor() -> ThreadPoolExecutor:
    """Single-thread pool: Playwright sync API must not cross threads or asyncio."""
    global _browser_executor
    with _browser_executor_lock:
        if _browser_executor is None:
            _browser_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="playwright")
        return _browser_executor


def _browser_thread_ensure() -> Any:
    if _browser_pw_state["browser"] is not None:
        return _browser_pw_state["browser"]
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    _browser_pw_state["playwright"] = pw
    _browser_pw_state["browser"] = browser
    return browser


def _browser_thread_close() -> None:
    browser = _browser_pw_state.get("browser")
    pw = _browser_pw_state.get("playwright")
    if browser is not None:
        try:
            browser.close()
        except Exception:
            pass
    if pw is not None:
        try:
            pw.stop()
        except Exception:
            pass
    _browser_pw_state["browser"] = None
    _browser_pw_state["playwright"] = None


def _browser_thread_fetch(url: str, content_mode: str) -> Optional[str]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    page = None
    context = None
    try:
        browser = _browser_thread_ensure()
        context = browser.new_context(
            user_agent=USER_AGENT,
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_GOTO_TIMEOUT_MS)
        # Extra wait helps Cloudflare / SPA challenges settle.
        page.wait_for_timeout(BROWSER_CLOUDFLARE_WAIT_MS)
        if content_mode == "html":
            html = page.content()
            if len(html) > MAX_TEXT_CHARS:
                html = html[:MAX_TEXT_CHARS]
            return html
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
    finally:
        if page is not None:
            try:
                page.close()
            except Exception:
                pass
        if context is not None:
            try:
                context.close()
            except Exception:
                pass


def close_browser_pool() -> None:
    """Release shared Playwright browser resources (call after batch scoring/imprint)."""
    global _browser_executor
    with _browser_executor_lock:
        if _browser_executor is None:
            return
        try:
            _browser_executor.submit(_browser_thread_close).result(timeout=30)
        except Exception:
            pass
        _browser_executor.shutdown(wait=False, cancel_futures=True)
        _browser_executor = None


def _fetch_page_browser(url: str, *, content_mode: str = "text") -> Optional[str]:
    """Fetch page via Playwright. content_mode: 'text' (body inner_text) or 'html' (full page HTML)."""
    try:
        import playwright  # noqa: F401
    except ImportError:
        logger.warning(
            "Playwright not installed; skipping browser fetch for %s. "
            "Install with: pip install playwright && playwright install chromium",
            url,
        )
        return None

    executor = _get_browser_executor()
    worker_timeout = (BROWSER_GOTO_TIMEOUT_MS / 1000) + BROWSER_CLOUDFLARE_WAIT_MS / 1000 + 30
    try:
        return executor.submit(_browser_thread_fetch, url, content_mode).result(timeout=worker_timeout)
    except FuturesTimeout:
        logger.warning("Browser fetch timed out for %s", url)
        return None
    except Exception as exc:
        logger.warning("Browser fetch failed for %s: %s", url, exc)
        return None


def _fetch_text_browser(url: str) -> Optional[str]:
    return _fetch_page_browser(url, content_mode="text")


def fetch_page_html_browser(url: str) -> Optional[str]:
    """Return full rendered HTML for scoring technical-signal extraction."""
    return _fetch_page_browser(url, content_mode="html")


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
    home_html = home_resp.text if home_resp else None
    if not home_html:
        home_html = fetch_page_html_browser(base_url)
    if not home_html:
        return None
    home_text = _html_to_text(home_html)

    imprint_url = find_imprint_url(home_html, base_url)
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
    raw = (domain or "").strip()
    if not raw:
        return None
    if raw.lower().startswith("http://"):
        raw = raw[7:]
    elif raw.lower().startswith("https://"):
        raw = raw[8:]
    host = raw.split("/")[0].rstrip("/")
    for candidate in _candidate_hosts(host):
        for scheme in ("https://", "http://"):
            resp = _fetch_url(scheme + candidate)
            if resp:
                return resp.url
    cleaned = normalize_domain(host)
    if cleaned:
        logger.debug(
            "HTTP probe failed for %s; using constructed base URL https://%s/",
            domain,
            cleaned,
        )
        return f"https://{cleaned}/"
    return None
