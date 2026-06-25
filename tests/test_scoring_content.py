from __future__ import annotations

from campaign_pipeline.config import ScoreConfig
from campaign_pipeline.scoring.compute import compute_veraltung_score
from campaign_pipeline.scoring.content import ScoringPageContent
from campaign_pipeline.scoring.extract import (
    TechnicalSignals,
    extract_technical_signals_struct,
    extract_visible_text,
    preserve_head_html,
)
from campaign_pipeline.scoring.truncate import prepare_llm_visible_text, word_count
from campaign_pipeline.steps.scoring import DomainScoringService, evaluate_pass

MODERN_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta property="og:title" content="Makler">
  <meta property="og:image" content="/img.jpg">
  <meta name="generator" content="WordPress 6.4">
  <script src="https://www.googletagmanager.com/gtag/js?id=G-ABC"></script>
  <link rel="stylesheet" href="/style.css">
  <link rel="apple-touch-icon" href="/icon.png">
</head>
<body>
  <div id="cookie-consent-banner">Cookies akzeptieren</div>
  <h1>Immobilien Makler GmbH</h1>
  <p>Verkauf und Vermietung von Wohnungen.</p>
  <form><input type="text"></form>
</body>
</html>
"""

LEGACY_HTML = """
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
  <meta charset="iso-8859-1">
  <meta name="generator" content="WordPress 3.9.2">
  <script src="/js/jquery-1.11.3.min.js"></script>
  <script src="/js/ga.js"></script>
</head>
<body>
  <center><font size="4">Willkommen</font></center>
  <table><tr><td>Nav</td></tr></table>
  <table><tr><td>Content</td></tr></table>
  <table><tr><td>Side</td></tr></table>
  <table><tr><td>Footer</td></tr></table>
  <table><tr><td>Extra</td></tr></table>
  <object data="flash.swf" type="application/x-shockwave-flash"></object>
  <a href="mailto:info@example.de">Kontakt</a>
  <p>Copyright 2012 — best viewed in Internet Explorer</p>
</body>
</html>
"""

HTTPS_MIXED_HTML = """
<!DOCTYPE html>
<html><head></head>
<body><img src="http://example.com/a.jpg"></body></html>
"""


def test_extract_visible_text_strips_scripts():
    text = extract_visible_text(MODERN_HTML)
    assert "googletagmanager" not in text
    assert "Immobilien Makler GmbH" in text
    assert "Cookies akzeptieren" in text


def test_extract_technical_signals_modern():
    signals = extract_technical_signals_struct(
        MODERN_HTML,
        {},
        final_url="https://makler.de/",
        fetch_url="https://makler.de/",
    )
    assert signals.viewport_present
    assert signals.modern_tracking
    assert signals.cookie_banner
    assert signals.social_meta_present
    assert signals.served_over_https
    assert not signals.legacy_doctype


def test_extract_technical_signals_legacy():
    signals = extract_technical_signals_struct(
        LEGACY_HTML,
        {},
        final_url="http://legacy.de/",
        fetch_url="http://legacy.de/",
    )
    assert signals.legacy_doctype
    assert signals.jquery_legacy
    assert not signals.viewport_present
    assert signals.layout_tables
    assert signals.legacy_embed
    assert signals.legacy_charset
    assert signals.presentational_html
    assert signals.legacy_analytics
    assert not signals.modern_tracking
    assert signals.retro_markers
    assert signals.mailto_only_contact
    assert not signals.served_over_https


def test_legacy_analytics_separate_from_modern_tracking():
    html = '<html><script src="/ga.js"></script></html>'
    signals = extract_technical_signals_struct(html, {}, final_url="http://x.de/", fetch_url="http://x.de/")
    assert signals.legacy_analytics
    assert not signals.modern_tracking


def test_mixed_content_on_https():
    signals = extract_technical_signals_struct(
        HTTPS_MIXED_HTML,
        {},
        final_url="https://secure.de/",
        fetch_url="https://secure.de/",
    )
    assert signals.served_over_https
    assert signals.mixed_content


def test_php5_from_header():
    signals = extract_technical_signals_struct(
        "<html></html>",
        {"X-Powered-By": "PHP/5.6.40"},
        final_url="http://x.de/",
        fetch_url="http://x.de/",
    )
    assert signals.php5_hint


def test_preserve_head_html_keeps_head():
    head = "<head>" + ("x" * 100) + "</head>"
    body = "y" * 10000
    html = f"<html>{head}<body>{body}</body></html>"
    preserved = preserve_head_html(html, max_chars=500)
    assert "<head>" in preserved
    assert preserved.count("x") == 100


def test_compute_veraltung_score_clamps():
    signals = extract_technical_signals_struct(
        LEGACY_HTML,
        {},
        final_url="http://legacy.de/",
        fetch_url="http://legacy.de/",
    )
    weights = {key: 2.0 for key in (
        "no_https", "legacy_charset", "jquery_legacy", "legacy_embed", "layout_tables",
        "retro_markers", "legacy_analytics", "presentational_html",
    )}
    total, technical_sum, scored, _detected = compute_veraltung_score(signals, 3, weights, max_score=10)
    assert total <= 10
    assert technical_sum >= len(scored) * 2.0 - 0.01
    assert len(scored) >= 3


def test_compute_respects_prompt_subset():
    signals = TechnicalSignals(jquery_legacy=True, legacy_charset=True, cookie_banner=True)
    weights = {"jquery_legacy": 2.0}
    total, technical_sum, scored, detected = compute_veraltung_score(signals, 0, weights)
    assert scored == ["jquery_legacy"]
    assert "legacy_charset" in detected or "jquery_legacy" in detected


def test_scoring_page_content_for_llm_is_visible_only():
    content = ScoringPageContent(
        visible_text="Makler Text",
        raw_html="<html>ignored</html>",
    )
    assert content.for_llm("text_and_technical") == "Makler Text"
    assert content.for_llm("text_only") == "Makler Text"


def test_prepare_llm_visible_text_deterministic_and_capped():
    text = "\n".join(f"Line {i} immobilien" for i in range(500))
    a, meta_a = prepare_llm_visible_text(text, max_chars=1000, keywords=["immobilien"])
    b, meta_b = prepare_llm_visible_text(text, max_chars=1000, keywords=["immobilien"])
    assert a == b
    assert meta_a["sent_chars"] <= 1000
    assert meta_a["truncated"]


def test_word_count_short_circuit():
    assert word_count("one two three") == 3
    assert word_count("x") == 1


def test_evaluate_pass_requires_makler_boolean():
    cfg = ScoreConfig(field="score", scale="0-10", pass_threshold=6)

    norm, raw, passed = evaluate_pass(
        {"score": 7, "makler": True},
        cfg,
        {"require_boolean": {"makler": True}},
    )
    assert passed and norm == 7.0

    norm, raw, passed = evaluate_pass(
        {"score": 7, "makler": False},
        cfg,
        {"require_boolean": {"makler": True}},
    )
    assert not passed

    norm, raw, passed = evaluate_pass(
        {"score": 4, "makler": True},
        cfg,
        {"require_boolean": {"makler": True}},
    )
    assert not passed
