from __future__ import annotations

from unittest.mock import MagicMock

from campaign_pipeline.imprint import northdata


SEARCH_HTML = """
<html><body>
  <a href="/THT%20Rheinland%20Logistik%20GmbH,%20N%C3%B6rvenich/Amtsgericht%20Aachen%20HRB%2012714">THT</a>
  <a href="/Some%20Other%20GmbH,%20Berlin/HRB%2099999">Other</a>
</body></html>
"""

COMPANY_HTML = """
<html><head>
<script type="application/ld+json">
{
  "@type": "LocalBusiness",
  "name": "THT Rheinland Logistik GmbH",
  "member": [
    {"@type": "Person", "givenName": "Ulrich", "familyName": "Dick",
     "jobTitle": "Geschäftsführer", "name": "Dick, Ulrich"}
  ]
}
</script>
</head><body>THT</body></html>
"""

JOINT_HTML = """
<html><head>
<script type="application/ld+json">
{"@type": "Organization", "name": "Kosse GmbH", "member": [
  {"@type": "Person", "givenName": "Enrico und Rinaldo", "familyName": "Kosse",
   "jobTitle": "Geschäftsführer", "name": "Kosse, Enrico und Rinaldo"}
]}
</script></head><body></body></html>
"""


def _session_returning(pages: dict):
    sess = MagicMock()

    def _get(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        for key, html in pages.items():
            if key in url:
                resp.text = html
                return resp
        resp.text = ""
        return resp

    sess.get.side_effect = _get
    return sess


def test_lookup_extracts_current_director():
    sess = _session_returning({"search": SEARCH_HTML, "HRB%2012714": COMPANY_HTML})
    result = northdata.lookup_managing_directors(
        "THT Rheinland Logistik GmbH", "Nörvenich", session=sess
    )
    assert result == [
        {
            "first_name": "Ulrich",
            "last_name": "Dick",
            "gender": None,
            "full_name": "Ulrich Dick",
        }
    ]


def test_lookup_splits_joint_entries():
    search = '<a href="/Kosse%20GmbH,%20Marienwerder/HRB%208417">Kosse</a>'
    sess = _session_returning({"search": search, "HRB%208417": JOINT_HTML})
    result = northdata.lookup_managing_directors("Kosse GmbH", "Marienwerder", session=sess)
    firsts = sorted(r["first_name"] for r in result)
    assert firsts == ["Enrico", "Rinaldo"]
    assert all(r["last_name"] == "Kosse" for r in result)


def test_lookup_returns_empty_without_name_match():
    search = '<a href="/Totally%20Different%20AG,%20Hamburg/HRB%201">X</a>'
    sess = _session_returning({"search": search})
    result = northdata.lookup_managing_directors("My Unique Company GmbH", session=sess)
    assert result == []


def test_lookup_empty_company_name():
    assert northdata.lookup_managing_directors("") == []


def _empty_extraction() -> dict:
    return {
        "full_address": None,
        "address_street": None,
        "address_house_number": None,
        "address_postcode": None,
        "address_city": None,
        "managing_directors": [],
        "company_legal_name": None,
        "generic_company_phones": [],
        "generic_company_emails": [],
        "confidence": 0.0,
    }


def test_northdata_fallback_skipped_when_imprint_not_reached(monkeypatch):
    from campaign_pipeline.imprint.extract import ImprintExtractor
    from campaign_pipeline.models import BusinessRow

    ex = ImprintExtractor(MagicMock(), enable_northdata=True)
    calls: list = []
    monkeypatch.setattr(
        ex,
        "_northdata_directors",
        lambda name, city: calls.append((name, city))
        or [{"first_name": "Wrong", "last_name": "Guess", "gender": None, "full_name": "Wrong Guess"}],
    )
    # Fetch failed entirely: no address in the extraction result.
    monkeypatch.setattr(ex, "extract_for_domain", lambda d, c="", *, force=False: _empty_extraction())

    row = BusinessRow.from_dict({"domain": "down.de", "company_name": "Schultz GmbH"})
    ex.apply_to_row(row)

    assert calls == []
    assert not any((d or {}).get("first_name") for d in row.directors)


def test_northdata_fallback_runs_when_address_present(monkeypatch):
    from campaign_pipeline.imprint.extract import ImprintExtractor
    from campaign_pipeline.models import BusinessRow

    ex = ImprintExtractor(MagicMock(), enable_northdata=True)
    ex._salutation_service = type("S", (), {"infer_salutation": staticmethod(lambda n: "Herr")})()
    calls: list = []
    monkeypatch.setattr(
        ex,
        "_northdata_directors",
        lambda name, city: calls.append((name, city))
        or [{"first_name": "Ulrich", "last_name": "Dick", "gender": None, "full_name": "Ulrich Dick"}],
    )
    reached = _empty_extraction()
    reached["address_street"] = "Gewerbepark"
    reached["address_city"] = "Nörvenich"
    monkeypatch.setattr(ex, "extract_for_domain", lambda d, c="", *, force=False: reached)

    row = BusinessRow.from_dict({"domain": "tht.de", "company_name": "THT Rheinland Logistik GmbH"})
    ex.apply_to_row(row)

    assert calls == [("THT Rheinland Logistik GmbH", "Nörvenich")]
    assert row.directors[0]["first_name"] == "Ulrich"
    assert row.directors[0]["last_name"] == "Dick"


def test_parse_members_ignores_non_management_titles():
    html = """
    <script type="application/ld+json">
    {"member": [
      {"@type": "Person", "givenName": "A", "familyName": "B", "jobTitle": "Wirtschaftsprüfer"},
      {"@type": "Person", "givenName": "C", "familyName": "D", "jobTitle": "Geschäftsführer"}
    ]}
    </script>
    """
    members = northdata._parse_members(html)
    assert members == [
        {"first_name": "C", "last_name": "D", "gender": None, "full_name": "C D"}
    ]
