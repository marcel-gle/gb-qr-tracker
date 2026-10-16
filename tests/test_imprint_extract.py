from __future__ import annotations

import json

from campaign_pipeline.imprint.extract import (
    ImprintExtractor,
    extract_privacy_contacts,
    filter_company_emails,
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


IMPRINT_WITH_PRIVACY_BLOCK = """
Impressum
Muster GmbH
Hauptstraße 1
80331 München
Telefon: +49 89 123456
E-Mail: info@muster.de

Geschäftsführer: Anna Schmidt

Datenschutzbeauftragter:

Max Mustermann
DSB-Consulting GmbH
Tel.: 0711 987654
E-Mail: mustermann@dsb-consulting.de

Registergericht: Amtsgericht München
""".strip()


def test_sanitize_removes_whole_multiline_privacy_block():
    cleaned = sanitize_imprint_text_for_extraction(IMPRINT_WITH_PRIVACY_BLOCK)
    assert "Max Mustermann" not in cleaned
    assert "987654" not in cleaned
    assert "dsb-consulting" not in cleaned
    assert "+49 89 123456" in cleaned
    assert "Anna Schmidt" in cleaned
    assert "Amtsgericht München" in cleaned


def test_sanitize_privacy_block_ends_at_section_heading_without_blank_line():
    text = """
Unser externer Datenschutzbeauftragter ist Max Mustermann
Tel. 0711 987654
Registergericht: Amtsgericht Stuttgart
""".strip()
    cleaned = sanitize_imprint_text_for_extraction(text)
    assert "987654" not in cleaned
    assert "Amtsgericht Stuttgart" in cleaned


def test_sanitize_keeps_director_who_is_also_privacy_officer():
    text = "Geschäftsführer und Datenschutzbeauftragter: Hans Müller\nTel. 030 555666"
    cleaned = sanitize_imprint_text_for_extraction(text)
    assert "Hans Müller" in cleaned
    assert "030 555666" in cleaned


def test_extract_privacy_contacts_ignores_numbers_also_used_by_company():
    text = """
Telefon: 089 123456

Datenschutzbeauftragter: Max Mustermann
Tel.: +49 (0)89 123456
Tel. direkt: 0711 987654
E-Mail: mustermann@dsb-consulting.de
""".strip()
    phone_keys, emails = extract_privacy_contacts(text)
    assert phone_keys == {"711987654"}
    assert emails == {"mustermann@dsb-consulting.de"}


def test_filter_company_phones_drops_privacy_numbers_in_any_format():
    phones = ["+49 89 123456", "+49 (0) 711 / 98 76 54"]
    assert filter_company_phones(phones, {"711987654"}) == ["+49 89 123456"]


def test_filter_company_emails_drops_privacy_mailboxes():
    emails = ["info@muster.de", "datenschutz@muster.de", "DSB@muster.de", "x@dsb.de"]
    assert filter_company_emails(emails, {"x@dsb.de"}) == ["info@muster.de"]


class _FakeLLM:
    def __init__(self, response: dict) -> None:
        self.response = response
        self.user_prompt = ""

    def chat(self, *, system_prompt, user_prompt, response_format=None, temperature=None):
        self.user_prompt = user_prompt
        return json.dumps(self.response)


def test_call_llm_drops_privacy_phone_returned_by_llm():
    llm = _FakeLLM(
        {
            "generic_company_phones": ["0711 987654", "+49 89 123456"],
            "generic_company_emails": ["mustermann@dsb-consulting.de", "info@muster.de"],
            "managing_directors": [],
        }
    )
    data = ImprintExtractor(llm, enable_northdata=False)._call_llm(
        "muster.de", "Muster GmbH", IMPRINT_WITH_PRIVACY_BLOCK
    )
    assert "987654" not in llm.user_prompt
    assert data["generic_company_phones"] == ["+49 89 123456"]
    assert data["generic_company_emails"] == ["info@muster.de"]
