from __future__ import annotations

from unittest.mock import MagicMock, patch

from campaign_pipeline.imprint.fetch import (
    _score_imprint_candidate,
    extract_text_from_url,
    find_imprint_url,
    looks_like_imprint,
)


SAMPLE_IMPRINT = """
Impressum
COPPEN GmbH
Robert-Bunsen-Straße 1
67098 Bad Dürkheim
Handelsregister: HRB 68658
Registergericht: Amtsgericht Ludwigshafen
Umsatzsteuer-ID: DE358982640
Geschäftsführung:
Thilo Zelmer
"""

SAMPLE_MARKETING = """
COPPEN - Solaranlagen & Wärmepumpen aus Deutschland
Premium Photovoltaik-Lösungen für Ihr Zuhause
"""


def test_looks_like_imprint_accepts_legal_text():
    assert looks_like_imprint(SAMPLE_IMPRINT) is True


def test_looks_like_imprint_rejects_marketing():
    assert looks_like_imprint(SAMPLE_MARKETING) is False


def test_looks_like_imprint_rejects_homepage_clone():
    assert looks_like_imprint(SAMPLE_MARKETING, home_text=SAMPLE_MARKETING) is False


def test_score_imprint_candidate_prefers_legal_paths():
    assert _score_imprint_candidate("/impressum", "Impressum") > _score_imprint_candidate(
        "/impressum/strom", "Strom"
    )
    assert _score_imprint_candidate("/impressum/impressum", "Impressum") > _score_imprint_candidate(
        "/impressum/shop", "Shop"
    )


def test_find_imprint_url_ranks_candidates(monkeypatch):
    html = """
    <html><body>
      <a href="/impressum/strom">Strom</a>
      <a href="/impressum">Impressum</a>
    </body></html>
    """
    calls: list[str] = []

    def fake_fetch(url: str):
        calls.append(url)
        resp = MagicMock()
        resp.status_code = 200
        resp.url = url
        resp.headers = {"Content-Type": "text/html"}
        return resp

    monkeypatch.setattr("campaign_pipeline.imprint.fetch._fetch_url", fake_fetch)
    result = find_imprint_url(html, "https://example.de/")
    assert result == "https://example.de/impressum"
    assert calls[0].endswith("/impressum")


@patch("campaign_pipeline.imprint.fetch._fetch_text_browser")
@patch("campaign_pipeline.imprint.fetch._fetch_url")
def test_extract_text_from_url_uses_browser_when_static_is_spa(mock_fetch, mock_browser):
    resp = MagicMock()
    resp.text = f"<html><body>{SAMPLE_MARKETING}</body></html>"
    resp.status_code = 200
    mock_fetch.return_value = resp
    mock_browser.return_value = SAMPLE_IMPRINT

    text = extract_text_from_url("https://example.de/impressum", home_text=SAMPLE_MARKETING)

    mock_browser.assert_called_once_with("https://example.de/impressum")
    assert "Thilo Zelmer" in text
    assert looks_like_imprint(text) is True


@patch("campaign_pipeline.imprint.fetch._fetch_text_browser")
@patch("campaign_pipeline.imprint.fetch._fetch_url")
def test_extract_text_from_url_skips_browser_when_static_ok(mock_fetch, mock_browser):
    resp = MagicMock()
    resp.text = f"<html><body>{SAMPLE_IMPRINT}</body></html>"
    mock_fetch.return_value = resp

    text = extract_text_from_url("https://example.de/impressum")

    mock_browser.assert_not_called()
    assert "Thilo Zelmer" in text
