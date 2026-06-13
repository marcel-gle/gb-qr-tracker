from __future__ import annotations

import re
from typing import List

from bs4 import BeautifulSoup

MAX_VISIBLE_TEXT_CHARS = 20_000
MAX_TECHNICAL_SIGNALS_CHARS = 3_000
MIN_LAYOUT_TABLE_COUNT = 5

_DOCTYPE_RE = re.compile(r"<!DOCTYPE\s+[^>]+>", re.IGNORECASE)
_JQUERY_OLD_RE = re.compile(r"jquery[.-]?(1|2)\.\d+", re.IGNORECASE)
_FIXED_WIDTH_RE = re.compile(r"width\s*:\s*(\d{3,4})px", re.IGNORECASE)
_LEGACY_URL_RE = re.compile(r"""["']([^"']*\.(?:shtml|cgi|pl))(?:\?[^"']*)?["']""", re.IGNORECASE)
_COOKIE_MARKUP_RE = re.compile(
    r"""(?:id|class)=["'][^"']*(?:cookie|consent|borlabs|usercentrics)[^"']*["']""",
    re.IGNORECASE,
)
_PHP5_RE = re.compile(r"PHP\s*/\s*5\.\d", re.IGNORECASE)


def extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines)[:MAX_VISIBLE_TEXT_CHARS]


def _meta_content(soup: BeautifulSoup, *, name: str | None = None, prop: str | None = None) -> str | None:
    if name:
        tag = soup.find("meta", attrs={"name": re.compile(f"^{re.escape(name)}$", re.I)})
        if tag and tag.get("content"):
            return str(tag["content"]).strip()
    if prop:
        tag = soup.find("meta", attrs={"property": re.compile(f"^{re.escape(prop)}$", re.I)})
        if tag and tag.get("content"):
            return str(tag["content"]).strip()
    return None


def _script_src_signals(html_lower: str) -> List[str]:
    signals: List[str] = []
    if "googletagmanager.com" in html_lower or "gtag(" in html_lower or "gtag/js" in html_lower:
        signals.append("tracking: google tag manager / gtag")
    if "connect.facebook.net" in html_lower or "facebook.com/tr" in html_lower or "fbq(" in html_lower:
        signals.append("tracking: meta / facebook pixel")
    if _JQUERY_OLD_RE.search(html_lower):
        match = _JQUERY_OLD_RE.search(html_lower)
        signals.append(f"jquery_legacy: {match.group(0) if match else 'detected'}")
    return signals


def _legacy_embed_signals(html_lower: str) -> List[str]:
    signals: List[str] = []
    for token in ("flash", "silverlight", "shockwave"):
        if token in html_lower:
            signals.append(f"legacy_embed: {token}")
    return signals


def extract_technical_signals(html: str) -> str:
    """Deterministic technical signals from raw HTML for outdated-website scoring."""
    if not html or not html.strip():
        return "- html: (empty)"

    html_lower = html.lower()
    soup = BeautifulSoup(html, "html.parser")
    lines: List[str] = []

    doctype = _DOCTYPE_RE.search(html[:500])
    if doctype:
        lines.append(f"- doctype: {doctype.group(0).strip()}")
    else:
        lines.append("- doctype: (not found)")

    generator = _meta_content(soup, name="generator")
    if generator:
        lines.append(f"- meta_generator: {generator}")
        gen_lower = generator.lower()
        if "wordpress" in gen_lower:
            version_match = re.search(r"wordpress\s*([\d.]+)", gen_lower)
            if version_match:
                lines.append(f"- cms_wordpress: {version_match.group(1)}")
        if "joomla" in gen_lower:
            lines.append("- cms_joomla: detected")
        if "drupal" in gen_lower:
            lines.append("- cms_drupal: detected")
    else:
        lines.append("- meta_generator: (not found)")

    viewport = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
    if viewport and viewport.get("content"):
        lines.append(f"- viewport: {str(viewport['content']).strip()}")
    else:
        lines.append("- viewport: (missing)")

    lines.extend(f"- {s}" for s in _script_src_signals(html_lower))
    if not any("tracking:" in ln for ln in lines):
        lines.append("- tracking: (none detected)")

    cookie_hits = len(_COOKIE_MARKUP_RE.findall(html))
    if cookie_hits:
        lines.append(f"- cookie_banner_markup: detected ({cookie_hits} hint(s))")
    else:
        lines.append("- cookie_banner_markup: (not detected)")

    lines.extend(f"- {s}" for s in _legacy_embed_signals(html_lower))

    if "<frameset" in html_lower or html_lower.count("<frame") >= 2:
        lines.append("- frames: detected")
    iframe_count = html_lower.count("<iframe")
    if iframe_count >= 3:
        lines.append(f"- iframe_count: {iframe_count}")

    table_count = html_lower.count("<table")
    if table_count >= MIN_LAYOUT_TABLE_COUNT:
        lines.append(f"- layout_tables: {table_count}")

    legacy_urls = sorted(set(_LEGACY_URL_RE.findall(html)))
    if legacy_urls:
        lines.append(f"- legacy_urls: {', '.join(legacy_urls[:5])}")

    if _PHP5_RE.search(html):
        lines.append("- php: 5.x hint detected")

    fixed_widths = sorted({m.group(1) for m in _FIXED_WIDTH_RE.finditer(html)})
    if fixed_widths:
        lines.append(f"- fixed_pixel_widths: {', '.join(fixed_widths[:5])}")

    output = "\n".join(lines)
    return output[:MAX_TECHNICAL_SIGNALS_CHARS]


def technical_signals_too_sparse(signals: str) -> bool:
    """True when static HTML yielded almost no usable technical hints."""
    if not signals or signals.strip() == "- html: (empty)":
        return True
    non_trivial = [
        ln
        for ln in signals.splitlines()
        if ln.strip()
        and "(not found)" not in ln
        and "(missing)" not in ln
        and "(none detected)" not in ln
        and "(not detected)" not in ln
        and "(empty)" not in ln
    ]
    return len(non_trivial) < 2
