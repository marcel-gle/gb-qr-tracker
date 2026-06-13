from __future__ import annotations

from campaign_pipeline.config import ScoreConfig
from campaign_pipeline.scoring.content import ScoringPageContent
from campaign_pipeline.scoring.extract import extract_technical_signals, extract_visible_text
from campaign_pipeline.steps.scoring import evaluate_pass

MODERN_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="generator" content="WordPress 6.4">
  <script src="https://www.googletagmanager.com/gtag/js?id=G-ABC"></script>
</head>
<body>
  <div id="cookie-consent-banner">Cookies akzeptieren</div>
  <h1>Immobilien Makler GmbH</h1>
  <p>Verkauf und Vermietung von Wohnungen.</p>
</body>
</html>
"""

LEGACY_HTML = """
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
  <meta name="generator" content="WordPress 3.9.2">
  <script src="/js/jquery-1.11.3.min.js"></script>
</head>
<body>
  <table><tr><td>Nav</td></tr></table>
  <table><tr><td>Content</td></tr></table>
  <table><tr><td>Side</td></tr></table>
  <table><tr><td>Footer</td></tr></table>
  <table><tr><td>Extra</td></tr></table>
  <object data="flash.swf" type="application/x-shockwave-flash"></object>
  <p>Copyright 2012</p>
</body>
</html>
"""


def test_extract_visible_text_strips_scripts():
    text = extract_visible_text(MODERN_HTML)
    assert "googletagmanager" not in text
    assert "Immobilien Makler GmbH" in text
    assert "Cookies akzeptieren" in text


def test_extract_technical_signals_modern():
    signals = extract_technical_signals(MODERN_HTML)
    assert "viewport:" in signals
    assert "present" in signals or "width=device-width" in signals
    assert "tracking: google tag manager" in signals
    assert "cookie_banner_markup: detected" in signals


def test_extract_technical_signals_legacy():
    signals = extract_technical_signals(LEGACY_HTML)
    assert "HTML 4" in signals or "DOCTYPE" in signals
    assert "jquery_legacy" in signals
    assert "viewport: (missing)" in signals
    assert "layout_tables: 5" in signals
    assert "legacy_embed: flash" in signals


def test_scoring_page_content_for_llm_modes():
    content = ScoringPageContent(
        visible_text="Makler Text",
        technical_signals="- doctype: HTML 4",
    )
    text_only = content.for_llm("text_only")
    assert text_only == "Makler Text"
    assert "TECHNISCHE SIGNALE" not in text_only

    full = content.for_llm("text_and_technical")
    assert "SICHTBARER WEBSEITEN-TEXT" in full
    assert "TECHNISCHE SIGNALE" in full
    assert "Makler Text" in full
    assert "doctype: HTML 4" in full


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
