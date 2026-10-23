from campaign_pipeline.email_fill import best_email_for_row, fill_row_email
from campaign_pipeline.models import BusinessRow


def _row(**kwargs) -> BusinessRow:
    data = {"domain": "firma.de", "company_name": "Firma GmbH"}
    data.update(kwargs)
    return BusinessRow.from_dict(data)


def test_keeps_existing_email():
    row = _row(email="info@firma.de", first_name_1="Max", last_name_1="Muster")
    row.apollo = [{"email": "max.muster@firma.de", "email_status": "verified"}]
    assert fill_row_email(row) is False
    assert row.email == "info@firma.de"


def test_prefers_verified_apollo_matching_name_over_info():
    row = _row(first_name_1="Max", last_name_1="Muster")
    row.extra = {"Generic Company Emails": "info@firma.de"}
    row.apollo = [{"email": "max.muster@firma.de", "email_status": "verified"}]
    assert fill_row_email(row) is True
    assert row.email == "max.muster@firma.de"


def test_prefers_name_match_over_generic_even_without_apollo():
    row = _row(first_name_1="Heike", last_name_1="Fiebes")
    row.extra = {
        "Generic Company Emails": "info@fiebes.de; heike.fiebes@fiebes.de",
        "Imprint: Email": "kontakt@fiebes.de",
    }
    assert best_email_for_row(row) == "heike.fiebes@fiebes.de"


def test_falls_back_to_info_when_nothing_personal():
    row = _row(first_name_1="Max", last_name_1="Muster")
    row.extra = {"Imprint: Email": "info@firma.de"}
    assert fill_row_email(row) is True
    assert row.email == "info@firma.de"


def test_skips_datenschutz_mailbox():
    row = _row(first_name_1="Max", last_name_1="Muster")
    row.extra = {
        "Imprint: Email": "datenschutz@firma.de",
        "Generic Company Emails": "info@firma.de",
    }
    assert best_email_for_row(row) == "info@firma.de"


def test_umlaut_name_match():
    row = _row(first_name_1="Jürgen", last_name_1="Müller")
    row.extra = {"Generic Company Emails": "info@firma.de, juergen.mueller@firma.de"}
    assert best_email_for_row(row) == "juergen.mueller@firma.de"


def test_extra_candidates_from_imprint_scrape():
    row = _row(first_name_1="Eva", last_name_1="Beispiel")
    assert (
        best_email_for_row(
            row,
            extra_candidates=["info@firma.de", "eva.beispiel@firma.de"],
        )
        == "eva.beispiel@firma.de"
    )
