from __future__ import annotations

from campaign_pipeline.imprint.extract import (
    filter_company_phones,
    filter_managing_directors,
    is_privacy_officer_director,
    normalize_llm_extraction,
    sanitize_imprint_text_for_extraction,
)


def test_sanitize_imprint_text_removes_fax_lines_and_segments():
    text = """
Impressum
Muster GmbH
Telefon: +49 30 123456
Fax: +49 30 654321
Tel. +49 89 111222 Fax +49 89 333444
Geschäftsführer: Anna Schmidt
Datenschutzbeauftragter: Max Mustermann
""".strip()
    cleaned = sanitize_imprint_text_for_extraction(text)
    assert "654321" not in cleaned
    assert "333444" not in cleaned
    assert "+49 30 123456" in cleaned
    assert "+49 89 111222" in cleaned
    assert "Anna Schmidt" in cleaned
    assert "Max Mustermann" not in cleaned


def test_filter_company_phones_drops_fax_entries():
    phones = [
        "+49 30 123456",
        "Fax: +49 30 654321",
        "Telefax 089 999888",
        "089 555666",
    ]
    assert filter_company_phones(phones) == ["+49 30 123456", "089 555666"]


def test_filter_managing_directors_drops_datenschutzbeauftragter():
    directors = [
        {"first_name": "Anna", "last_name": "Schmidt", "full_name": "Anna Schmidt"},
        {
            "first_name": "Max",
            "last_name": "Muster",
            "full_name": "Max Muster, Datenschutzbeauftragter",
        },
        {"full_name": "Datenschutzkoordinatorin Lisa Beispiel"},
    ]
    kept = filter_managing_directors(directors)
    assert len(kept) == 1
    assert kept[0]["last_name"] == "Schmidt"
    assert is_privacy_officer_director(kept[0]) is False


def test_normalize_llm_extraction_applies_phone_and_director_filters():
    data = {
        "generic_company_phones": ["030 111", "Fax 030 222"],
        "managing_directors": [
            {"full_name": "Peter Geschäftsführer"},
            {"full_name": "Eva Datenschutzbeauftragte"},
        ],
    }
    out = normalize_llm_extraction(data)
    assert out["generic_company_phones"] == ["030 111"]
    assert len(out["managing_directors"]) == 1
    assert out["managing_directors"][0]["full_name"] == "Peter Geschäftsführer"
