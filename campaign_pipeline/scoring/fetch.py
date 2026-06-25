from __future__ import annotations

import logging
from typing import Mapping, Optional

from ..imprint.fetch import (
    _fetch_url,
    fetch_page_html_browser,
    normalize_domain_to_base_url,
)
from .content import ScoringPageContent
from .extract import (
    extract_technical_signals_struct,
    extract_visible_text,
    technical_signals_too_sparse,
)

logger = logging.getLogger(__name__)

MIN_VISIBLE_TEXT_FOR_SPA = 200


def _normalize_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    if not headers:
        return {}
    return {str(k): str(v) for k, v in headers.items()}


def _needs_browser_fallback(
    *,
    mode: str,
    browser_fallback: bool,
    http_ok: bool,
    visible_text: str,
    technical_signals,
    enabled_keys: list[str] | None = None,
) -> bool:
    if not browser_fallback:
        return False
    if not http_ok:
        return True
    if len((visible_text or "").strip()) < MIN_VISIBLE_TEXT_FOR_SPA:
        return True
    if mode == "text_and_technical" and technical_signals is not None:
        if technical_signals_too_sparse(technical_signals, enabled_keys=enabled_keys):
            return True
    return False


def fetch_scoring_content(
    domain: str,
    *,
    mode: str = "text_only",
    browser_fallback: bool = False,
    enabled_signal_keys: list[str] | None = None,
) -> Optional[ScoringPageContent]:
    base_url = normalize_domain_to_base_url(domain)
    if not base_url:
        logger.warning("Could not normalize domain for scoring: %s", domain)
        return None

    fetch_url = base_url
    resp = _fetch_url(base_url)
    html = resp.text if resp else ""
    http_ok = bool(resp and html)
    response_headers = _normalize_headers(resp.headers if resp else None)
    final_url = str(resp.url) if resp and getattr(resp, "url", None) else base_url

    visible_text = extract_visible_text(html) if html else ""
    technical_signals = (
        extract_technical_signals_struct(
            html,
            response_headers,
            final_url=final_url,
            fetch_url=fetch_url,
        )
        if mode == "text_and_technical" and html
        else None
    )
    fetch_source = "http"

    if _needs_browser_fallback(
        mode=mode,
        browser_fallback=browser_fallback,
        http_ok=http_ok,
        visible_text=visible_text,
        technical_signals=technical_signals,
        enabled_keys=enabled_signal_keys,
    ):
        browser_html = fetch_page_html_browser(base_url)
        if browser_html:
            fetch_source = "browser"
            html = browser_html
            response_headers = {}
            final_url = base_url
            visible_text = extract_visible_text(browser_html)
            if mode == "text_and_technical":
                technical_signals = extract_technical_signals_struct(
                    browser_html,
                    response_headers,
                    final_url=final_url,
                    fetch_url=fetch_url,
                )
        elif browser_fallback:
            logger.warning("Browser fallback unavailable or failed for %s", domain)

    if not (visible_text or "").strip() and technical_signals is None:
        logger.warning("No scoring content for %s", domain)
        return None

    return ScoringPageContent(
        visible_text=visible_text or "",
        raw_html=html or "",
        response_headers=response_headers,
        final_url=final_url,
        fetch_url=fetch_url,
        technical_signals=technical_signals,
        fetch_source=fetch_source,
    )
