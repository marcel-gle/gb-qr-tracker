from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from campaign_pipeline.imprint.extract import ImprintExtractor
from campaign_pipeline.io.mappers import map_row_to_canonical
from campaign_pipeline.io.writers import write_business_rows, write_final_business_rows
from campaign_pipeline.io.readers import load_rows_as_business
from campaign_pipeline.models import BusinessRow


def test_mapper_sets_input_phone_and_phone():
    canonical = map_row_to_canonical({"Domain": "example.de", "Telefon": "+49 30 111"})
    assert canonical["phone"] == "+49 30 111"
    assert canonical["input_phone"] == "+49 30 111"


def test_imprint_phone_set_even_when_phone_already_filled():
    llm = MagicMock()
    extractor = ImprintExtractor(llm, enable_northdata=False)
    row = BusinessRow.from_dict(
        {"domain": "example.de", "phone": "+49 30 111", "input_phone": "+49 30 111"}
    )
    with patch.object(
        extractor,
        "extract_for_domain",
        return_value={
            "full_address": "Hauptstr. 1, 10115 Berlin",
            "address_street": "Hauptstr.",
            "address_house_number": "1",
            "address_postcode": "10115",
            "address_city": "Berlin",
            "generic_company_phones": ["+49 89 999"],
            "generic_company_emails": [],
            "managing_directors": [
                {"first_name": "Anna", "last_name": "Schmidt", "gender": "Frau", "full_name": "Anna Schmidt"}
            ],
            "company_legal_name": None,
            "confidence": 0.9,
        },
    ):
        extractor.apply_to_row(row)

    assert row.phone == "+49 30 111"
    assert row.input_phone == "+49 30 111"
    assert row.imprint_phone == "+49 89 999"


def _imprint_data(**overrides):
    data = {
        "full_address": "Buchberg 11, 83730 Fischbachau",
        "address_street": "Buchberg",
        "address_house_number": "11",
        "address_postcode": "83730",
        "address_city": "Fischbachau",
        "generic_company_phones": [],
        "generic_company_emails": [],
        "managing_directors": [],
        "company_legal_name": None,
        "confidence": 0.9,
    }
    data.update(overrides)
    return data


def test_complete_imprint_address_replaces_input_address_as_a_whole():
    extractor = ImprintExtractor(MagicMock(), enable_northdata=False)
    row = BusinessRow.from_dict({"domain": "example.de", "city": "Elbach", "street": "Alte Str.", "house_number": "9"})
    with patch.object(extractor, "extract_for_domain", return_value=_imprint_data()):
        extractor.apply_to_row(row)
    assert (row.street, row.house_number, row.postcode, row.city) == ("Buchberg", "11", "83730", "Fischbachau")


def test_complete_imprint_address_without_house_number_splits_street():
    extractor = ImprintExtractor(MagicMock(), enable_northdata=False)
    row = BusinessRow.from_dict({"domain": "example.de", "house_number": "9"})
    data = _imprint_data(address_street="Buchberg 11", address_house_number=None)
    with patch.object(extractor, "extract_for_domain", return_value=data):
        extractor.apply_to_row(row)
    assert (row.street, row.house_number) == ("Buchberg", "11")


def test_partial_imprint_address_only_fills_empty_fields():
    extractor = ImprintExtractor(MagicMock(), enable_northdata=False)
    row = BusinessRow.from_dict({"domain": "example.de", "city": "Elbach", "postcode": "83730"})
    data = _imprint_data(address_postcode=None, address_city="Fischbachau")
    with patch.object(extractor, "extract_for_domain", return_value=data):
        extractor.apply_to_row(row)
    assert (row.street, row.postcode, row.city) == ("Buchberg", "83730", "Elbach")


def test_imprint_phone_fills_phone_when_empty():
    llm = MagicMock()
    extractor = ImprintExtractor(llm, enable_northdata=False)
    row = BusinessRow.from_dict({"domain": "example.de"})
    with patch.object(
        extractor,
        "extract_for_domain",
        return_value={
            "full_address": None,
            "address_street": None,
            "address_house_number": None,
            "address_postcode": None,
            "address_city": None,
            "generic_company_phones": ["089 555"],
            "generic_company_emails": [],
            "managing_directors": [],
            "company_legal_name": None,
            "confidence": 0.5,
        },
    ):
        extractor.apply_to_row(row)

    assert row.imprint_phone == "089 555"
    assert row.phone == "089 555"


def test_apollo_and_phone_fields_round_trip_csv(tmp_path: Path):
    path = tmp_path / "rows.csv"
    row = BusinessRow.from_dict(
        {
            "domain": "example.de",
            "phone": "030 111",
            "input_phone": "030 111",
            "imprint_phone": "089 222",
            "apollo_phone_1": "+491701234567",
            "apollo_phone_type_1": "mobile",
            "apollo_status_1": "ok",
            "apollo_request_id_1": "12345",
            "first_name_1": "Max",
            "last_name_1": "Muster",
        }
    )
    write_business_rows(path, [row])
    loaded = load_rows_as_business(path)
    assert len(loaded) == 1
    out = loaded[0]
    assert out.phone == "030 111"
    assert out.input_phone == "030 111"
    assert out.imprint_phone == "089 222"
    assert out.apollo[0]["phone"] == "+491701234567"
    assert out.apollo[0]["phone_type"] == "mobile"
    assert out.apollo[0]["status"] == "ok"
    assert out.apollo[0]["request_id"] == "12345"
    d = out.to_dict()
    assert d["apollo_phone_1"] == "+491701234567"


def test_apollo_slots_keep_director_index_and_new_columns():
    row = BusinessRow.from_dict(
        {
            "domain": "example.de",
            "first_name_1": "Max",
            "last_name_1": "Muster",
            "first_name_2": "Eva",
            "last_name_2": "Beispiel",
            "apollo_status_2": "rejected",
            "apollo_reject_reason_2": "domain_mismatch",
            "apollo_match_confidence_2": "high",
            "apollo_person_id_2": "p2",
        }
    )
    assert not row.apollo[0]["status"]
    assert row.apollo[1]["status"] == "rejected"
    assert row.apollo[1]["reject_reason"] == "domain_mismatch"
    d = row.to_dict()
    assert d["apollo_person_id_2"] == "p2"
    assert d["apollo_match_confidence_2"] == "high"


def test_final_csv_includes_apollo_and_provenance_phones(tmp_path: Path):
    path = tmp_path / "final.csv"
    row = BusinessRow.from_dict(
        {
            "domain": "example.de",
            "phone": "030 111",
            "input_phone": "030 111",
            "imprint_phone": "089 222",
            "first_name_1": "Max",
            "last_name_1": "Muster",
            "apollo_phone_1": "+491701234567",
            "apollo_phone_type_1": "mobile",
            "apollo_phones_all_1": "+491701234567, +4930999",
            "apollo_status_1": "ok",
            "apollo_person_id_1": "p1",
            "template": "t.pdf",
        }
    )
    write_final_business_rows(path, [row])
    header, values = [line.split(";") for line in path.read_text(encoding="utf-8").splitlines()]
    out = dict(zip(header, values))
    assert header[-1] == "Template"
    assert out["phone"] == "030 111"
    assert out["input_phone"] == "030 111"
    assert out["imprint_phone"] == "089 222"
    assert out["apollo_phone_1"] == "+491701234567"
    assert out["apollo_phone_type_1"] == "mobile"
    assert out["apollo_phones_all_1"] == "+491701234567, +4930999"
    assert "apollo_status_1" not in header
    assert "apollo_person_id_1" not in header


def test_apollo_details_round_trip_and_final_subset(tmp_path: Path):
    row = BusinessRow.from_dict(
        {
            "domain": "example.de",
            "first_name_1": "Max",
            "last_name_1": "Muster",
            "apollo_title_1": "Geschäftsführer",
            "apollo_email_1": "max@example.de",
            "apollo_linkedin_url_1": "https://linkedin.com/in/max",
            "apollo_seniority_1": "owner",
            "apollo_org_phone": "+49 30 999",
            "apollo_org_employees": "12",
            "template": "t.pdf",
        }
    )
    path = tmp_path / "rows.csv"
    write_business_rows(path, [row])
    loaded = load_rows_as_business(path)[0]
    assert loaded.apollo[0]["title"] == "Geschäftsführer"
    assert loaded.apollo[0]["seniority"] == "owner"
    assert loaded.apollo_org == {"phone": "+49 30 999", "employees": "12"}
    assert "apollo_org_employees" not in loaded.extra

    final = tmp_path / "final.csv"
    write_final_business_rows(final, [loaded])
    header, values = [line.split(";") for line in final.read_text(encoding="utf-8").splitlines()]
    out = dict(zip(header, values))
    assert out["apollo_title_1"] == "Geschäftsführer"
    assert out["apollo_email_1"] == "max@example.de"
    assert out["apollo_linkedin_url_1"] == "https://linkedin.com/in/max"
    assert out["apollo_org_phone"] == "+49 30 999"
    assert "apollo_seniority_1" not in header
    assert "apollo_org_employees" not in header
    assert header[-1] == "Template"
