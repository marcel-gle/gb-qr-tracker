from __future__ import annotations

from unittest.mock import MagicMock, patch

from campaign_pipeline.imprint.fetch import (
    _collect_internal_links,
    _html_to_text,
    _is_html_response,
    _response_html,
    _score_imprint_candidate,
    extract_text_from_url,
    find_imprint_url,
    find_imprint_url_deep,
    looks_like_imprint,
    normalize_domain_to_base_url,
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


def test_is_html_response_accepts_missing_content_type_with_html_body():
    resp = MagicMock()
    resp.headers = {}
    resp.text = "<!DOCTYPE html><html><body>Hi</body></html>"
    assert _is_html_response(resp) is True


@patch("campaign_pipeline.imprint.fetch._fetch_url", return_value=None)
def test_normalize_domain_to_base_url_falls_back_when_http_blocked(mock_fetch):
    url = normalize_domain_to_base_url("immoprofi-mueller.de")
    assert url == "https://immoprofi-mueller.de/"
    assert mock_fetch.call_count >= 2


@patch("campaign_pipeline.imprint.fetch._fetch_url", return_value=None)
def test_normalize_domain_to_base_url_falls_back_for_valid_host(mock_fetch):
    url = normalize_domain_to_base_url("r-schultes.de")
    assert url == "https://r-schultes.de/"


@patch("campaign_pipeline.imprint.fetch._fetch_url")
def test_normalize_domain_to_base_url_tries_www_variant(mock_fetch):
    def side_effect(url: str, **kwargs):
        if url == "https://www.example.de":
            resp = MagicMock()
            resp.url = url
            resp.status_code = 200
            resp.headers = {"Content-Type": "text/html"}
            resp.text = "<html></html>"
            return resp
        return None

    mock_fetch.side_effect = side_effect
    assert normalize_domain_to_base_url("example.de") == "https://www.example.de"


@patch("campaign_pipeline.imprint.fetch._fetch_url")
def test_normalize_prefers_host_with_usable_body(mock_fetch):
    # Bare host answers 200 with an empty body (e.g. www-only certificate +
    # verify=False retry); the www host serves real content. We must pick www.
    def side_effect(url: str, **kwargs):
        resp = MagicMock()
        resp.url = url.rstrip("/") + "/"
        resp.status_code = 200
        resp.headers = {"Content-Type": "text/html; charset=utf-8"}
        if "www." in url:
            body = "<html><body>" + ("content " * 60) + "</body></html>"
        else:
            body = ""
        resp.text = body
        resp.content = body.encode()
        return resp

    mock_fetch.side_effect = side_effect
    assert normalize_domain_to_base_url("schultz-logistik.de") == (
        "https://www.schultz-logistik.de/"
    )


@patch("campaign_pipeline.imprint.fetch._fetch_url")
def test_normalize_falls_back_to_first_responder_when_no_usable_body(mock_fetch):
    def side_effect(url: str, **kwargs):
        resp = MagicMock()
        resp.url = url
        resp.status_code = 200
        resp.headers = {"Content-Type": "text/html"}
        resp.text = ""
        resp.content = b""
        return resp

    mock_fetch.side_effect = side_effect
    # No candidate has a usable body -> return the first responding URL.
    assert normalize_domain_to_base_url("example.de") == "https://example.de"


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

    def fake_fetch(url: str, **kwargs):
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


def _make_resp(body: bytes, content_type: str = "text/html"):
    """Build a requests-like response whose .text respects requests' rules."""
    import requests

    resp = requests.Response()
    resp.status_code = 200
    resp._content = body
    resp.headers["Content-Type"] = content_type
    return resp


def test_response_html_fixes_missing_charset_utf8():
    # Server omits charset (requests would default to ISO-8859-1) but the page
    # declares UTF-8 via a meta tag. We should decode as UTF-8, not mojibake.
    body = (
        '<html><head><meta charset="utf-8"></head>'
        "<body>Geschäftsführer: Söhne</body></html>"
    ).encode("utf-8")
    resp = _make_resp(body, content_type="text/html")
    html = _response_html(resp)
    assert "Geschäftsführer" in html
    assert "Söhne" in html
    assert "Ã¤" not in html


def test_response_html_respects_declared_header_charset():
    body = "<html><body>Straße</body></html>".encode("latin-1")
    resp = _make_resp(body, content_type="text/html; charset=iso-8859-1")
    assert "Straße" in _response_html(resp)


def test_html_to_text_collapses_blank_runs():
    html = "<html><body>A" + ("<br>" * 40) + "Impressum</body></html>"
    text = _html_to_text(html)
    assert "\n\n\n" not in text
    assert "A" in text and "Impressum" in text


def test_collect_internal_links_same_host_only():
    html = """
    <a href="/spedition-roder.de/index.html">Site</a>
    <a href="https://other.com/impressum">External</a>
    <a href="#top">Anchor</a>
    <a href="mailto:x@y.de">Mail</a>
    """
    links = _collect_internal_links(html, "http://speditionroder.de/")
    assert links == ["http://speditionroder.de/spedition-roder.de/index.html"]


def test_find_imprint_url_deep_follows_inner_page(monkeypatch):
    root_html = '<a href="/spedition-roder.de/index.html">EUGEN RODER Spedition GmbH</a>'
    inner_html = '<a href="7.html">Kontakt / Datenschutz / Impressum</a>'

    def fake_fetch(url: str, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.url = url
        resp.headers = {"Content-Type": "text/html"}
        if url.endswith("/spedition-roder.de/index.html"):
            resp.text = inner_html
            resp.content = inner_html.encode()
        elif url.endswith("7.html"):
            resp.text = "<html>Impressum</html>"
            resp.content = b"<html>Impressum</html>"
        else:
            return None
        return resp

    monkeypatch.setattr("campaign_pipeline.imprint.fetch._fetch_url", fake_fetch)
    result = find_imprint_url_deep(root_html, "http://speditionroder.de/")
    assert result == "http://speditionroder.de/spedition-roder.de/7.html"
