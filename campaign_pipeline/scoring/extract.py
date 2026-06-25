from __future__ import annotations

import re
from dataclasses import dataclass, fields
from typing import List, Mapping, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

MAX_VISIBLE_TEXT_CHARS = 20_000
MAX_HTML_BODY_CHARS = 500_000
MIN_LAYOUT_TABLE_COUNT = 5
MIN_INLINE_STYLE_COUNT = 8

_DOCTYPE_RE = re.compile(r"<!DOCTYPE\s+[^>]+>", re.IGNORECASE)
_JQUERY_OLD_RE = re.compile(r"jquery[.-]?(1|2)\.\d+", re.IGNORECASE)
_FIXED_WIDTH_RE = re.compile(r"width\s*:\s*(\d{3,4})px", re.IGNORECASE)
_LEGACY_URL_RE = re.compile(r"""["']([^"']*\.(?:shtml|cgi|pl))(?:\?[^"']*)?["']""", re.IGNORECASE)
_COOKIE_MARKUP_RE = re.compile(
    r"""(?:id|class)=["'][^"']*(?:cookie|consent|borlabs|usercentrics)[^"']*["']""",
    re.IGNORECASE,
)
_PHP5_RE = re.compile(r"PHP\s*/\s*5\.\d", re.IGNORECASE)
_LEGACY_CHARSET_RE = re.compile(
    r"charset\s*=\s*[\"']?\s*(iso-8859-1|windows-1252|cp1252)",
    re.IGNORECASE,
)
_LEGACY_JS_RE = re.compile(
    r"jquery-ui[.-]?1\.(?:8|9|10)|jquery\.ui[.-]?1\.(?:8|9|10)|"
    r"mootools|prototype\.js|scriptaculous|yui(?:/|-)",
    re.IGNORECASE,
)
_LEGACY_ANALYTICS_RE = re.compile(
    r"(google-analytics\.com/ga\.js|/ga\.js|urchin\.js|urchinTracker)",
    re.IGNORECASE,
)
_MIXED_CONTENT_RE = re.compile(
    r"""(?:src|href)\s*=\s*["']http://[^"']+["']""",
    re.IGNORECASE,
)
_QUERYSTRING_URL_RE = re.compile(
    r"""["'](?:[^"']*/)?(?:index|default)\.(?:php|asp|aspx)\?[^"']*(?:id|cat|page)=\d+[^"']*["']""",
    re.IGNORECASE,
)
_RETRO_MARKER_RE = re.compile(
    r"(hit counter|besucherzähler|best viewed in|optimiert für|besucherstatistik|"
    r"webring|powered by|besucher seit|visitor counter|web counter)",
    re.IGNORECASE,
)
_HEAD_RE = re.compile(r"(<head[^>]*>)(.*?)(</head>)", re.IGNORECASE | re.DOTALL)


@dataclass
class TechnicalSignals:
    cookie_banner: bool = False
    modern_tracking: bool = False
    legacy_analytics: bool = False
    viewport_present: bool = False
    legacy_doctype: bool = False
    jquery_legacy: bool = False
    cms_outdated: bool = False
    legacy_embed: bool = False
    php5_hint: bool = False
    layout_tables: bool = False
    frames: bool = False
    heavy_iframes: bool = False
    legacy_urls: bool = False
    fixed_pixel_width: bool = False
    served_over_https: bool = False
    mixed_content: bool = False
    legacy_charset: bool = False
    presentational_html: bool = False
    ie_targeting: bool = False
    inline_style_heavy: bool = False
    social_meta_present: bool = False
    legacy_js_libs: bool = False
    favicon_only_ico: bool = False
    retro_markers: bool = False
    mailto_only_contact: bool = False
    querystring_urls: bool = False

    def triggered_field_names(self) -> list[str]:
        return [f.name for f in fields(self) if getattr(self, f.name)]


def preserve_head_html(html: str, max_chars: int = MAX_HTML_BODY_CHARS) -> str:
    if not html or len(html) <= max_chars:
        return html or ""
    match = _HEAD_RE.search(html)
    if not match:
        return html[:max_chars]
    head_block = match.group(0)
    prefix = html[: match.start()]
    suffix = html[match.end() :]
    budget = max_chars - len(prefix) - len(head_block)
    if budget < 0:
        return (prefix + head_block)[:max_chars]
    return prefix + head_block + suffix[:budget]


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


def _parse_version(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for piece in re.split(r"[.\-]", version):
        if piece.isdigit():
            parts.append(int(piece))
    return tuple(parts)


def _version_lt(version: str, major: int, minor: int = 0) -> bool:
    parsed = _parse_version(version)
    if not parsed:
        return False
    target = (major, minor)
    padded = parsed + (0,) * max(0, len(target) - len(parsed))
    return tuple(padded[: len(target)]) < target


def _cms_outdated_from_generator(generator: str) -> bool:
    gen_lower = generator.lower()
    if "wordpress" in gen_lower:
        match = re.search(r"wordpress\s*([\d.]+)", gen_lower)
        if match and _version_lt(match.group(1), 4):
            return True
    if "joomla" in gen_lower:
        match = re.search(r"joomla[!/\s]*([\d.]+)", gen_lower)
        if match:
            major = _parse_version(match.group(1))
            if major and major[0] <= 2:
                return True
        return True
    if "drupal" in gen_lower:
        match = re.search(r"drupal\s*([\d.]+)", gen_lower)
        if match:
            major = _parse_version(match.group(1))
            if major and major[0] <= 7:
                return True
    return False


def _is_legacy_doctype(doctype: str) -> bool:
    lower = doctype.lower()
    return "html 4" in lower or "xhtml 1" in lower


def _header_value(headers: Mapping[str, str], name: str) -> str:
    lower = name.lower()
    for key, value in headers.items():
        if key.lower() == lower:
            return str(value)
    return ""


def _url_is_https(url: str) -> bool:
    try:
        return urlparse(url or "").scheme.lower() == "https"
    except Exception:
        return False


def extract_technical_signals_struct(
    html: str,
    headers: Mapping[str, str] | None = None,
    *,
    final_url: str = "",
    fetch_url: str = "",
) -> TechnicalSignals:
    signals = TechnicalSignals()
    if not html or not html.strip():
        return signals

    headers = headers or {}
    html = preserve_head_html(html)
    html_lower = html.lower()
    soup = BeautifulSoup(html, "html.parser")

    signals.served_over_https = _url_is_https(final_url or fetch_url)
    if signals.served_over_https and _MIXED_CONTENT_RE.search(html):
        signals.mixed_content = True

    if _COOKIE_MARKUP_RE.search(html):
        signals.cookie_banner = True

    if (
        "googletagmanager.com" in html_lower
        or "gtag(" in html_lower
        or "gtag/js" in html_lower
        or "connect.facebook.net" in html_lower
        or "facebook.com/tr" in html_lower
        or "fbq(" in html_lower
    ):
        signals.modern_tracking = True
    if _LEGACY_ANALYTICS_RE.search(html):
        signals.legacy_analytics = True

    viewport = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
    if viewport and viewport.get("content"):
        signals.viewport_present = True

    doctype = _DOCTYPE_RE.search(html[:500])
    if doctype and _is_legacy_doctype(doctype.group(0)):
        signals.legacy_doctype = True

    if _JQUERY_OLD_RE.search(html_lower):
        signals.jquery_legacy = True

    generator = _meta_content(soup, name="generator") or ""
    if generator and _cms_outdated_from_generator(generator):
        signals.cms_outdated = True

    for token in ("flash", "silverlight", "shockwave"):
        if token in html_lower:
            signals.legacy_embed = True
            break

    powered_by = _header_value(headers, "X-Powered-By")
    if _PHP5_RE.search(html) or _PHP5_RE.search(powered_by):
        signals.php5_hint = True

    if html_lower.count("<table") >= MIN_LAYOUT_TABLE_COUNT:
        signals.layout_tables = True

    if "<frameset" in html_lower or html_lower.count("<frame") >= 2:
        signals.frames = True

    if html_lower.count("<iframe") >= 3:
        signals.heavy_iframes = True

    if _LEGACY_URL_RE.findall(html):
        signals.legacy_urls = True

    if _FIXED_WIDTH_RE.search(html):
        signals.fixed_pixel_width = True

    if _LEGACY_CHARSET_RE.search(html):
        signals.legacy_charset = True

    if (
        soup.find("font")
        or soup.find("center")
        or soup.find(attrs={"bgcolor": True})
        or soup.find(attrs={"align": re.compile(r"^(left|right|center|justify)$", re.I)})
    ):
        signals.presentational_html = True

    if (
        "<!--[if" in html_lower
        or "x-ua-compatible" in html_lower
        or re.search(r"lt\s+ie\s*[89]", html_lower)
    ):
        signals.ie_targeting = True

    inline_styles = len(re.findall(r"\sstyle\s*=", html_lower))
    has_stylesheet = bool(soup.find("link", attrs={"rel": re.compile(r"stylesheet", re.I)}))
    style_tags = soup.find_all("style")
    style_in_head = any(tag.find_parent("head") is not None for tag in style_tags)
    if inline_styles >= MIN_INLINE_STYLE_COUNT and (not has_stylesheet or (style_tags and not style_in_head)):
        signals.inline_style_heavy = True

    og_title = _meta_content(soup, prop="og:title")
    og_image = _meta_content(soup, prop="og:image")
    twitter_card = _meta_content(soup, name="twitter:card") or _meta_content(soup, name="twitter:title")
    if og_title or og_image or twitter_card:
        signals.social_meta_present = True

    if _LEGACY_JS_RE.search(html_lower):
        signals.legacy_js_libs = True

    has_favicon_ico = "favicon.ico" in html_lower
    has_modern_icon = bool(
        soup.find("link", attrs={"rel": re.compile(r"apple-touch-icon", re.I)})
        or soup.find("link", attrs={"rel": re.compile(r"icon", re.I), "type": re.compile(r"png", re.I)})
        or "site.webmanifest" in html_lower
    )
    if has_favicon_ico and not has_modern_icon:
        signals.favicon_only_ico = True

    visible_text = extract_visible_text(html)
    if _RETRO_MARKER_RE.search(html) or _RETRO_MARKER_RE.search(visible_text):
        signals.retro_markers = True

    has_mailto = bool(soup.find("a", href=re.compile(r"^mailto:", re.I)))
    has_form = bool(soup.find("form"))
    if has_mailto and not has_form:
        signals.mailto_only_contact = True

    if _QUERYSTRING_URL_RE.search(html):
        signals.querystring_urls = True

    return signals


def format_technical_signals(signals: TechnicalSignals) -> str:
    lines = [f"- {name}: detected" for name in signals.triggered_field_names()]
    if not lines:
        return "- (no signals detected)"
    return "\n".join(lines)


def extract_technical_signals(html: str) -> str:
    """Backward-compatible string formatter."""
    return format_technical_signals(extract_technical_signals_struct(html))


def technical_signals_too_sparse(
    signals: TechnicalSignals | str,
    *,
    enabled_keys: list[str] | None = None,
) -> bool:
    from .indicators.registry import is_scoring_key_triggered

    if isinstance(signals, str):
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

    keys = enabled_keys or list_detected_scoring_keys(signals)
    triggered = sum(1 for key in keys if is_scoring_key_triggered(key, signals))
    return triggered < 2


def list_detected_scoring_keys(signals: TechnicalSignals) -> list[str]:
    from .indicators.registry import list_detected_scoring_keys as _list

    return _list(signals)
