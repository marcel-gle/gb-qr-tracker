from __future__ import annotations

from pathlib import Path

from scripts.send_letter.send_letters_onlinebrief24 import (
    REQUIRED_CONTACT_FIELDS,
    _normalize_header,
    _resolve_cmyk_color,
    load_contacts_csv,
)


def test_normalize_header_maps_campaign_pipeline_columns():
    assert _normalize_header("first_name_1") == "Vorname"
    assert _normalize_header("last_name_1") == "Nachname"
    assert _normalize_header("salutation_1") == "Anrede"
    assert _normalize_header("company_name") == "Unternehmen"
    assert _normalize_header("tracking_link") == "QR Code URL"
    assert _normalize_header("tracking_url") == "Tracking Code URL"


def test_load_contacts_csv_accepts_final_with_links_format(tmp_path: Path):
    csv_path = tmp_path / "contacts.csv"
    csv_path.write_text(
        "company_name;street;house_number;postcode;city;first_name_1;last_name_1;"
        "Template;tracking_link;tracking_url\n"
        "Test GmbH;Hauptstr.;1;10115;Berlin;Max;Muster;letter.pdf;https://qr.example/a;"
        "https://track.example/a\n",
        encoding="utf-8",
    )
    contacts, delimiter = load_contacts_csv(csv_path)
    assert delimiter == ";"
    assert len(contacts) == 1
    row = contacts[0]
    missing = [field for field in REQUIRED_CONTACT_FIELDS if field not in row]
    assert missing == []
    assert row["Vorname"] == "Max"
    assert row["Nachname"] == "Muster"
    assert row["Unternehmen"] == "Test GmbH"
    assert row["Template"] == "letter.pdf"


def test_resolve_cmyk_color_presets_and_custom():
    assert _resolve_cmyk_color({}) is not None
    assert _resolve_cmyk_color({"color": "link_blue"}) is not None
    custom = _resolve_cmyk_color({"color": {"cmyk": [1, 0.5, 0, 0]}})
    assert custom is not None
