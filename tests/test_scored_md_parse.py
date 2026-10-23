from campaign_pipeline.imprint.apollo import count_apollo_candidates
from campaign_pipeline.models import (
    BusinessRow,
    parse_managing_director_field,
    split_person_name,
)


def test_split_person_name_basic():
    assert split_person_name("Heike Fiebes") == ("Heike", "Fiebes")
    assert split_person_name("Dr. Max Mustermann") == ("Max", "Mustermann")
    assert split_person_name("Awais Ahmad Zahid") == ("Awais Ahmad", "Zahid")
    assert split_person_name("Mustermann, Max") == ("Max", "Mustermann")


def test_parse_managing_director_field_multiple():
    parsed = parse_managing_director_field("Christopher Ahrens, Tristan Peveling")
    assert parsed == [
        ("Christopher", "Ahrens", "Christopher Ahrens"),
        ("Tristan", "Peveling", "Tristan Peveling"),
    ]


def test_parse_managing_director_field_und():
    parsed = parse_managing_director_field("Marvin Scholl und Christian Papke")
    assert [p[:2] for p in parsed] == [("Marvin", "Scholl"), ("Christian", "Papke")]


def test_from_dict_fills_names_from_ocean_imprint_md():
    row = BusinessRow.from_dict(
        {
            "domain": "fiebes.de",
            "first_name_1": "",
            "last_name_1": "",
            "Imprint: Managing director": "Heike Fiebes",
        }
    )
    assert row.directors[0]["first_name"] == "Heike"
    assert row.directors[0]["last_name"] == "Fiebes"
    assert row.directors[0]["imprint_managing_director"] == "Heike Fiebes"


def test_from_dict_keeps_explicit_names_over_ocean_md():
    row = BusinessRow.from_dict(
        {
            "domain": "a.de",
            "first_name_1": "Eva",
            "last_name_1": "Beispiel",
            "Imprint: Managing director": "Jemand Anders",
        }
    )
    assert row.directors[0]["first_name"] == "Eva"
    assert row.directors[0]["last_name"] == "Beispiel"


def test_from_dict_parses_multiple_ocean_mds_into_slots():
    row = BusinessRow.from_dict(
        {
            "domain": "papke-scholl.de",
            "Imprint: Managing director": "Marvin Scholl, Christian Papke",
        }
    )
    assert row.directors[0]["first_name"] == "Marvin"
    assert row.directors[0]["last_name"] == "Scholl"
    assert row.directors[1]["first_name"] == "Christian"
    assert row.directors[1]["last_name"] == "Papke"
    assert count_apollo_candidates([row]) == 2


def test_scored_sample_rows_become_apollo_candidates():
    rows = [
        BusinessRow.from_dict({"domain": "fiebes.de", "Imprint: Managing director": "Heike Fiebes"}),
        BusinessRow.from_dict(
            {
                "domain": "ahrensundpeveling.de",
                "Imprint: Managing director": "Christopher Ahrens, Tristan Peveling",
            }
        ),
        BusinessRow.from_dict(
            {"domain": "rps-hh.de", "Imprint: Managing director": "Awais Ahmad Zahid"}
        ),
    ]
    assert count_apollo_candidates(rows) == 4
