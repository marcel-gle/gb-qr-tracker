from __future__ import annotations

import logging
from typing import Optional

from ..imprint.fetch import (
    _fetch_url,
    fetch_page_html_browser,
    normalize_domain_to_base_url,
)
from .content import ScoringPageContent
from .extract import (
    extract_technical_signals,
    extract_visible_text,
    technical_signals_too_sparse,
)

logger = logging.getLogger(__name__)

MIN_VISIBLE_TEXT_FOR_SPA = 200


def _needs_browser_fallback(
    *,
    mode: str,
    browser_fallback: bool,
    http_ok: bool,
    visible_text: str,
    technical_signals: str,
) -> bool:
    if not browser_fallback:
        return False
    if not http_ok:
        return True
    if len((visible_text or "").strip()) < MIN_VISIBLE_TEXT_FOR_SPA:
        return True
    if mode == "text_and_technical" and technical_signals_too_sparse(technical_signals):
        return True
    return False


def fetch_scoring_content(
    domain: str,
    *,
    mode: str = "text_only",
    browser_fallback: bool = False,
) -> Optional[ScoringPageContent]:
    base_url = normalize_domain_to_base_url(domain)
    if not base_url:
        logger.warning("Could not normalize domain for scoring: %s", domain)
        return None

    resp = _fetch_url(base_url)
    html = resp.text if resp else ""
    http_ok = bool(resp and html)

    visible_text = extract_visible_text(html) if html else ""
    technical_signals = extract_technical_signals(html) if mode == "text_and_technical" and html else None
    fetch_source = "http"

    if _needs_browser_fallback(
        mode=mode,
        browser_fallback=browser_fallback,
        http_ok=http_ok,
        visible_text=visible_text,
        technical_signals=technical_signals or "",
    ):
        browser_html = fetch_page_html_browser(base_url)
        if browser_html:
            fetch_source = "browser"
            visible_text = extract_visible_text(browser_html)
            if mode == "text_and_technical":
                technical_signals = extract_technical_signals(browser_html)
        elif browser_fallback:
            logger.warning("Browser fallback unavailable or failed for %s", domain)

    if not (visible_text or "").strip() and not technical_signals:
        logger.warning("No scoring content for %s", domain)
        return None

    return ScoringPageContent(
        visible_text=visible_text or "",
        technical_signals=technical_signals,
        fetch_source=fetch_source,
    )
