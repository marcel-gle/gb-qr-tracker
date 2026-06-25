from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict

if TYPE_CHECKING:
    from ..extract import TechnicalSignals

ScoringPredicate = Callable[["TechnicalSignals"], bool]


def _neg(field: str) -> ScoringPredicate:
    def pred(signals: TechnicalSignals) -> bool:
        return not bool(getattr(signals, field, False))

    return pred


def _pos(field: str) -> ScoringPredicate:
    def pred(signals: TechnicalSignals) -> bool:
        return bool(getattr(signals, field, False))

    return pred


INDICATOR_REGISTRY: Dict[str, dict] = {
    "no_cookie_banner": {"tier": "baseline", "default_weight": 1.0, "predicate": _neg("cookie_banner")},
    "no_modern_tracking": {"tier": "baseline", "default_weight": 1.0, "predicate": _neg("modern_tracking")},
    "no_viewport": {"tier": "baseline", "default_weight": 1.5, "predicate": _neg("viewport_present")},
    "legacy_doctype": {"tier": "baseline", "default_weight": 1.0, "predicate": _pos("legacy_doctype")},
    "jquery_legacy": {"tier": "baseline", "default_weight": 1.5, "predicate": _pos("jquery_legacy")},
    "cms_outdated": {"tier": "baseline", "default_weight": 2.0, "predicate": _pos("cms_outdated")},
    "legacy_embed": {"tier": "baseline", "default_weight": 2.0, "predicate": _pos("legacy_embed")},
    "php5_hint": {"tier": "baseline", "default_weight": 2.0, "predicate": _pos("php5_hint")},
    "layout_tables": {"tier": "baseline", "default_weight": 1.0, "predicate": _pos("layout_tables")},
    "frames": {"tier": "baseline", "default_weight": 1.5, "predicate": _pos("frames")},
    "heavy_iframes": {"tier": "baseline", "default_weight": 1.0, "predicate": _pos("heavy_iframes")},
    "legacy_urls": {"tier": "baseline", "default_weight": 1.5, "predicate": _pos("legacy_urls")},
    "fixed_pixel_width": {"tier": "baseline", "default_weight": 1.0, "predicate": _pos("fixed_pixel_width")},
    "no_https": {"tier": "strong", "default_weight": 2.0, "predicate": _neg("served_over_https")},
    "mixed_content": {"tier": "strong", "default_weight": 1.5, "predicate": _pos("mixed_content")},
    "legacy_charset": {"tier": "strong", "default_weight": 1.5, "predicate": _pos("legacy_charset")},
    "presentational_html": {"tier": "strong", "default_weight": 2.0, "predicate": _pos("presentational_html")},
    "ie_targeting": {"tier": "strong", "default_weight": 1.5, "predicate": _pos("ie_targeting")},
    "inline_style_heavy": {"tier": "strong", "default_weight": 1.5, "predicate": _pos("inline_style_heavy")},
    "no_social_meta": {"tier": "strong", "default_weight": 0.5, "predicate": _neg("social_meta_present")},
    "legacy_js_libs": {"tier": "moderate", "default_weight": 1.0, "predicate": _pos("legacy_js_libs")},
    "favicon_only_ico": {"tier": "moderate", "default_weight": 0.5, "predicate": _pos("favicon_only_ico")},
    "retro_markers": {"tier": "moderate", "default_weight": 2.0, "predicate": _pos("retro_markers")},
    "mailto_only_contact": {"tier": "moderate", "default_weight": 0.5, "predicate": _pos("mailto_only_contact")},
    "querystring_urls": {"tier": "moderate", "default_weight": 0.5, "predicate": _pos("querystring_urls")},
    "legacy_analytics": {"tier": "moderate", "default_weight": 1.0, "predicate": _pos("legacy_analytics")},
}


def is_scoring_key_triggered(key: str, signals: TechnicalSignals) -> bool:
    entry = INDICATOR_REGISTRY.get(key)
    if entry is None:
        return False
    predicate = entry["predicate"]
    return bool(predicate(signals))


def list_detected_scoring_keys(signals: TechnicalSignals) -> list[str]:
    return [key for key in INDICATOR_REGISTRY if is_scoring_key_triggered(key, signals)]
