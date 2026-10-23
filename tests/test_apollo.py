from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

from campaign_pipeline.imprint.apollo import (
    ApolloPhoneClient,
    match_and_reveal_for_row,
    parse_match_response,
    parse_phone_numbers,
    poll_apollo_pending,
    verify_match,
)
from campaign_pipeline.models import BusinessRow
from campaign_pipeline.steps.imprint_scrape import run_apollo_enrichment


def _resp(status_code: int, payload: Any = None):
    r = MagicMock()
    r.status_code = status_code
    r.content = b"{}" if payload is not None else b""
    r.text = ""
    r.headers = {}
    r.json.return_value = payload if payload is not None else {}
    return r


def _person(
    *,
    pid: str = "p1",
    first: str = "Eva",
    last: str = "Beispiel",
    confidence: str = "high",
    domain: str = "firma.de",
    org_id: str = "o1",
    current: bool = True,
) -> Dict[str, Any]:
    return {
        "person": {
            "id": pid,
            "first_name": first,
            "last_name": last,
            "match_confidence": confidence,
            "organization_id": org_id,
            "organization": {"id": org_id, "primary_domain": domain},
            "employment_history": [
                {"current": current, "organization_id": org_id, "title": "Geschäftsführerin"},
                {"current": False, "organization_id": "old", "title": "Vertrieb"},
            ],
        }
    }


def _poll_ok(pid: str = "p1", numbers: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    return {
        "webhook_result": {
            "people": [
                {
                    "id": pid,
                    "status": "success",
                    "phone_numbers": numbers
                    if numbers is not None
                    else [
                        {
                            "sanitized_number": "+491701234567",
                            "type_cd": "mobile",
                            "status_cd": "valid_number",
                            "confidence_cd": "high",
                        }
                    ],
                }
            ]
        }
    }


class FakeSession:
    """Routes Apollo calls: match (no reveal), reveal (by id), poll (GET)."""

    def __init__(self, match_payload: Any, poll_payloads: List[Any] | None = None):
        self.match_payload = match_payload
        self.poll_payloads = list(poll_payloads or [])
        self.calls: List[Dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any):
        params = kwargs.get("params") or {}
        self.calls.append({"method": method, "url": url, "params": params})
        if method == "GET":
            return self.poll_payloads.pop(0) if self.poll_payloads else _resp(200, _poll_ok())
        if params.get("reveal_phone_number") == "true":
            return _resp(200, {"request_id": 718432950164203900})
        if isinstance(self.match_payload, MagicMock):
            return self.match_payload
        payload = self.match_payload(params) if callable(self.match_payload) else self.match_payload
        return _resp(200, payload)

    def calls_of(self, kind: str) -> List[Dict[str, Any]]:
        if kind == "poll":
            return [c for c in self.calls if c["method"] == "GET"]
        reveal = kind == "reveal"
        return [
            c
            for c in self.calls
            if c["method"] == "POST" and (c["params"].get("reveal_phone_number") == "true") == reveal
        ]


def _row(**extra: str) -> BusinessRow:
    data = {"domain": "firma.de", "company_name": "Firma GmbH", "first_name_1": "Eva", "last_name_1": "Beispiel"}
    data.update(extra)
    return BusinessRow.from_dict(data)


# --------------------------------------------------------------------------- #
# verify_match
# --------------------------------------------------------------------------- #


def test_verify_accepts_high_and_medium():
    for conf in ("high", "medium"):
        match = parse_match_response(_person(confidence=conf))
        assert verify_match(match, "firma.de", "Eva", "Beispiel").accepted


def test_verify_rejects_low_confidence():
    match = parse_match_response(_person(confidence="low"))
    assert verify_match(match, "firma.de", "Eva", "Beispiel").reason == "low_confidence"


def test_verify_rejects_no_match():
    match = parse_match_response({"person": None})
    assert verify_match(match, "firma.de", "Eva", "Beispiel").reason == "no_match"


def test_verify_rejects_other_current_employer():
    match = parse_match_response(_person(domain="andere-firma.de"))
    assert verify_match(match, "firma.de", "Eva", "Beispiel").reason == "domain_mismatch"


def test_verify_rejects_when_not_current_at_org():
    match = parse_match_response(_person(current=False))
    assert verify_match(match, "firma.de", "Eva", "Beispiel").reason == "not_current"


def test_verify_rejects_name_mismatch():
    match = parse_match_response(_person(first="Eva", last="Andere"))
    assert verify_match(match, "firma.de", "Eva", "Beispiel").reason == "name_mismatch"


def test_verify_normalizes_umlauts_www_and_double_first_names():
    match = parse_match_response(_person(first="Jürgen", last="Müller", domain="firma.de"))
    assert verify_match(match, "www.firma.de", "Juergen-Peter", "MUELLER").accepted


# --------------------------------------------------------------------------- #
# Phone parsing
# --------------------------------------------------------------------------- #


def test_parse_phone_numbers_filters_invalid_and_orders_best_first():
    phones = parse_phone_numbers(
        [
            {"sanitized_number": "+49301", "type_cd": "work_hq", "status_cd": "valid_number", "confidence_cd": "high"},
            {"sanitized_number": "+49999", "type_cd": "mobile", "status_cd": "invalid_number"},
            {"sanitized_number": "+49170", "type_cd": "mobile", "status_cd": "valid_number", "confidence_cd": "high"},
            {"sanitized_number": "+49171", "type_cd": "mobile", "status_cd": "valid_number", "confidence_cd": "low"},
        ]
    )
    assert [p.number for p in phones] == ["+49170", "+49301", "+49171"]


# --------------------------------------------------------------------------- #
# Client + row flow
# --------------------------------------------------------------------------- #


def test_match_request_does_not_reveal_and_sends_company_context():
    session = FakeSession(_person())
    client = ApolloPhoneClient(api_key="k", session=session)
    client.match_person("Eva", "Beispiel", "firma.de", organization_name="Firma GmbH", linkedin_url="https://linkedin.com/in/eva")
    params = session.calls[0]["params"]
    assert params["reveal_phone_number"] == "false"
    assert params["organization_name"] == "Firma GmbH"
    assert params["linkedin_url"] == "https://linkedin.com/in/eva"
    assert "poll_only" not in params


def test_missing_api_key_makes_no_requests():
    session = FakeSession(_person())
    client = ApolloPhoneClient(api_key="", session=session)
    assert client.match_person("Eva", "Beispiel", "firma.de").error == "missing_api_key"
    assert session.calls == []


def test_verified_match_reveals_by_id_and_polls_number():
    session = FakeSession(
        _person(),
        [_resp(404, {"error_code": "result_pending", "retry_after_seconds": 2}), _resp(200, _poll_ok())],
    )
    client = ApolloPhoneClient(api_key="k", session=session)
    row = _row()

    stats = match_and_reveal_for_row(row, client)
    assert stats["matched"] == 1 and stats["revealed"] == 1
    reveal = session.calls_of("reveal")[0]["params"]
    assert reveal["id"] == "p1"
    assert reveal["reveal_phone_number"] == "true"
    assert reveal["poll_only"] == "true"
    assert "first_name" not in reveal and "domain" not in reveal
    assert row.apollo[0]["status"] == "pending"
    assert row.apollo[0]["request_id"] == "718432950164203900"

    sleeps: List[float] = []
    poll_stats = poll_apollo_pending([row], client, sleep_fn=sleeps.append, timeout_s=60)
    assert poll_stats["ok"] == 1
    assert sleeps == [2.0]
    slot = row.apollo[0]
    assert slot["status"] == "ok"
    assert slot["phone"] == "+491701234567"
    assert slot["phone_type"] == "mobile"
    assert slot["match_confidence"] == "high"
    assert slot["person_id"] == "p1"


def test_rejected_match_never_reveals():
    session = FakeSession(_person(domain="andere-firma.de"))
    client = ApolloPhoneClient(api_key="k", session=session)
    row = _row()
    stats = match_and_reveal_for_row(row, client)
    assert stats["rejected"] == 1
    assert stats["rejected_domain_mismatch"] == 1
    assert session.calls_of("reveal") == []
    assert row.apollo[0]["status"] == "rejected"
    assert row.apollo[0]["reject_reason"] == "domain_mismatch"
    assert row.apollo[0]["phone"] is None


def test_poll_ignores_numbers_of_other_person_id():
    session = FakeSession(_person(), [_resp(200, _poll_ok(pid="someone-else"))])
    client = ApolloPhoneClient(api_key="k", session=session)
    row = _row()
    match_and_reveal_for_row(row, client)
    poll_apollo_pending([row], client, sleep_fn=lambda s: None, timeout_s=60)
    assert row.apollo[0]["status"] == "none"
    assert row.apollo[0]["phone"] is None


def test_poll_timeout_keeps_pending_for_next_run():
    session = FakeSession(_person(), [_resp(404, {"error_code": "result_pending"})] * 5)
    client = ApolloPhoneClient(api_key="k", session=session)
    row = _row()
    match_and_reveal_for_row(row, client)
    stats = poll_apollo_pending([row], client, sleep_fn=lambda s: None, timeout_s=-1)
    assert stats["timeout"] == 1
    assert row.apollo[0]["status"] == "pending"
    assert row.apollo[0]["reject_reason"] == "poll_timeout"

    # Next run resumes polling instead of revealing (and paying) again.
    resume = match_and_reveal_for_row(row, client)
    assert resume["resumed"] == 1
    assert len(session.calls_of("reveal")) == 1


def test_cache_prevents_second_match_and_reveal(tmp_path: Path):
    cache = tmp_path / "apollo.cache.pkl"
    session = FakeSession(_person())
    client = ApolloPhoneClient(api_key="k", session=session, cache_path=cache)
    row = _row()
    match_and_reveal_for_row(row, client)
    poll_apollo_pending([row], client, sleep_fn=lambda s: None, timeout_s=60)
    client.save_cache()

    session2 = FakeSession(_person())
    client2 = ApolloPhoneClient(api_key="k", session=session2, cache_path=cache)
    fresh_row = _row()
    stats = match_and_reveal_for_row(fresh_row, client2)
    assert stats["cached"] == 1
    assert session2.calls == []
    assert fresh_row.apollo[0]["phone"] == "+491701234567"


def test_cached_rejection_is_not_rematched(tmp_path: Path):
    cache = tmp_path / "apollo.cache.pkl"
    client = ApolloPhoneClient(api_key="k", session=FakeSession(_person(confidence="low")), cache_path=cache)
    match_and_reveal_for_row(_row(), client)
    client.save_cache()

    session2 = FakeSession(_person())
    client2 = ApolloPhoneClient(api_key="k", session=session2, cache_path=cache)
    row = _row()
    match_and_reveal_for_row(row, client2)
    assert session2.calls == []
    assert row.apollo[0]["status"] == "rejected"
    assert row.apollo[0]["reject_reason"] == "low_confidence"


def test_rate_limit_is_retried():
    session = MagicMock()
    limited = _resp(429, {})
    limited.headers = {"Retry-After": "1"}
    session.request.side_effect = [limited, _resp(200, _person())]
    sleeps: List[float] = []
    client = ApolloPhoneClient(api_key="k", session=session, sleep_fn=sleeps.append)
    match = client.match_person("Eva", "Beispiel", "firma.de")
    assert match.status == "matched"
    assert sleeps == [1.0]


def test_match_and_reveal_disable_waterfall_and_email_reveal():
    session = FakeSession(_person())
    client = ApolloPhoneClient(api_key="k", session=session)
    match_and_reveal_for_row(_row(), client)
    for call in session.calls:
        assert call["params"]["run_waterfall_phone"] == "false"
        assert call["params"]["run_waterfall_email"] == "false"
        assert call["params"]["reveal_personal_emails"] == "false"


def test_same_person_twice_in_one_run_is_matched_and_revealed_once():
    session = FakeSession(_person())
    client = ApolloPhoneClient(api_key="k", session=session)
    rows = [_row(), _row()]
    for row in rows:
        match_and_reveal_for_row(row, client)
    assert len(session.calls_of("match")) == 1
    assert len(session.calls_of("reveal")) == 1
    poll_apollo_pending(rows, client, sleep_fn=lambda s: None, timeout_s=60)
    assert len(session.calls_of("poll")) == 1
    assert [r.apollo[0]["phone"] for r in rows] == ["+491701234567", "+491701234567"]


def test_same_apollo_person_on_other_domain_is_not_revealed_again():
    session = FakeSession(lambda params: _person(domain=params["domain"]))
    client = ApolloPhoneClient(api_key="k", session=session)
    first = _row()
    match_and_reveal_for_row(first, client)
    poll_apollo_pending([first], client, sleep_fn=lambda s: None, timeout_s=60)

    second = _row(domain="firma-holding.de")
    stats = match_and_reveal_for_row(second, client)
    assert stats["cached"] == 1
    assert len(session.calls_of("reveal")) == 1
    assert second.apollo[0]["phone"] == "+491701234567"


def test_auth_error_stops_all_further_requests():
    unauthorized = _resp(401, {})
    session = FakeSession(unauthorized)
    client = ApolloPhoneClient(api_key="bad", session=session)
    rows = [_row(), _row(domain="b.de", first_name_1="Max", last_name_1="Muster")]
    for row in rows:
        match_and_reveal_for_row(row, client)
    assert len(session.calls) == 1
    assert client.fatal_error == "http_401"
    assert rows[1].apollo[0]["reject_reason"] == "aborted_http_401"


def test_poll_server_error_stays_pending():
    session = FakeSession(_person(), [_resp(503, {}), _resp(200, _poll_ok())])
    client = ApolloPhoneClient(api_key="k", session=session)
    row = _row()
    match_and_reveal_for_row(row, client)
    stats = poll_apollo_pending([row], client, sleep_fn=lambda s: None, timeout_s=60)
    assert stats == {"ok": 1, "none": 0, "error": 0, "timeout": 0}
    assert len(session.calls_of("reveal")) == 1


def test_expired_request_allows_single_new_reveal_next_run():
    session = FakeSession(_person(), [_resp(410, {"error_code": "request_id_expired"})])
    client = ApolloPhoneClient(api_key="k", session=session)
    row = _row()
    match_and_reveal_for_row(row, client)
    poll_apollo_pending([row], client, sleep_fn=lambda s: None, timeout_s=60)
    assert row.apollo[0]["status"] == "error"

    stats = match_and_reveal_for_row(row, client)
    assert stats["revealed"] == 1
    assert len(session.calls_of("match")) == 1
    assert len(session.calls_of("reveal")) == 2


def test_apollo_skips_rows_removed_by_address_dedupe():
    from campaign_pipeline.steps.imprint_scrape import apollo_target_rows

    addr = {"company_name": "Firma GmbH", "street": "Hauptstr.", "house_number": "1", "postcode": "10115"}
    a = BusinessRow.from_dict({"domain": "a.de", **addr})
    b = BusinessRow.from_dict({"domain": "b.de", **addr})
    c = BusinessRow.from_dict({"domain": "c.de"})
    assert apollo_target_rows([a, b, c], [b, c]) == [c]
    assert apollo_target_rows([a, b, c], [a, b, c]) == [a, c]


def _rich_person(**kwargs: Any) -> Dict[str, Any]:
    data = _person(**kwargs)
    data["person"].update(
        {
            "name": "Eva Beispiel",
            "title": "Geschäftsführerin",
            "headline": "Geschäftsführerin bei Firma GmbH",
            "seniority": "owner",
            "departments": ["c_suite", "master_marketing"],
            "email": "eva@firma.de",
            "email_status": "verified",
            "linkedin_url": "http://www.linkedin.com/in/eva-beispiel",
            "city": "München",
            "state": "Bavaria",
            "country": "Germany",
        }
    )
    data["person"]["employment_history"][0]["start_date"] = "2015-03-01"
    data["person"]["organization"].update(
        {
            "name": "Firma GmbH",
            "phone": "+49 89 123456",
            "linkedin_url": "https://www.linkedin.com/company/firma",
            "industry": "marketing & advertising",
            "keywords": ["seo", "webdesign"],
            "estimated_num_employees": 12,
            "annual_revenue_printed": "1.2M",
            "founded_year": 2009,
            "street_address": "Hauptstr. 1",
            "postal_code": "80331",
            "city": "München",
            "country": "Germany",
            "short_description": "Agentur\nfür Webdesign.",
            "technology_names": ["WordPress", "Google Analytics"],
        }
    )
    return data


def test_match_details_are_stored_for_verified_person():
    client = ApolloPhoneClient(api_key="k", session=FakeSession(_rich_person()))
    row = _row()
    match_and_reveal_for_row(row, client)
    slot = row.apollo[0]
    assert slot["title"] == "Geschäftsführerin"
    assert slot["seniority"] == "owner"
    assert slot["departments"] == "c suite, master marketing"
    assert slot["position_start"] == "2015-03-01"
    assert slot["email"] == "eva@firma.de"
    assert slot["email_status"] == "verified"
    assert slot["linkedin_url"] == "http://www.linkedin.com/in/eva-beispiel"
    assert slot["city"] == "München"
    org = row.apollo_org
    assert org["phone"] == "+49 89 123456"
    assert org["employees"] == "12"
    assert org["domain"] == "firma.de"
    assert org["keywords"] == "seo, webdesign"
    assert org["description"] == "Agentur für Webdesign."
    assert org["technologies"] == "WordPress, Google Analytics"


def test_locked_email_placeholder_is_dropped():
    data = _rich_person()
    data["person"]["email"] = "email_not_unlocked@domain.com"
    client = ApolloPhoneClient(api_key="k", session=FakeSession(data))
    row = _row()
    match_and_reveal_for_row(row, client)
    assert not row.apollo[0].get("email")
    assert not row.apollo[0].get("email_status")


def test_rejected_match_stores_no_details():
    client = ApolloPhoneClient(api_key="k", session=FakeSession(_rich_person(domain="andere-firma.de")))
    row = _row()
    match_and_reveal_for_row(row, client)
    assert row.apollo[0]["status"] == "rejected"
    assert not row.apollo[0].get("email")
    assert not row.apollo[0].get("title")
    assert not row.apollo_org


def test_details_come_from_cache_without_new_requests(tmp_path: Path):
    cache = tmp_path / "apollo.cache.pkl"
    client = ApolloPhoneClient(api_key="k", session=FakeSession(_rich_person()), cache_path=cache)
    row = _row()
    match_and_reveal_for_row(row, client)
    poll_apollo_pending([row], client, sleep_fn=lambda s: None, timeout_s=60)
    client.save_cache()

    session2 = FakeSession(_rich_person())
    fresh = _row()
    match_and_reveal_for_row(fresh, ApolloPhoneClient(api_key="k", session=session2, cache_path=cache))
    assert session2.calls == []
    assert fresh.apollo[0]["email"] == "eva@firma.de"
    assert fresh.apollo[0]["phone"] == "+491701234567"
    assert fresh.apollo_org["phone"] == "+49 89 123456"


def test_run_apollo_enrichment_stats_and_phases():
    rows = [
        _row(),
        BusinessRow.from_dict({"domain": "leer.de"}),
    ]
    client = ApolloPhoneClient(api_key="k", session=FakeSession(_person()))
    phases: List[str] = []
    stats = run_apollo_enrichment(
        rows,
        client,
        max_directors=3,
        progress_callback=lambda d, t, e, phase: phases.append(phase),
        sleep_fn=lambda s: None,
    )
    assert stats["apollo_candidates"] == 1
    assert stats["apollo_ok"] == 1
    assert stats["apollo_rejected"] == 0
    assert "apollo_match" in phases and "apollo_poll" in phases


def test_poll_progress_updates_per_request_id():
    """UI must see progress inside a poll round, not only between rounds."""
    rows = []
    session = FakeSession(
        _person(),
        [
            _resp(404, {"error_code": "pending", "retry_after_seconds": 1}),
            _resp(404, {"error_code": "pending", "retry_after_seconds": 1}),
            _resp(200, _poll_ok(pid="p1")),
            _resp(200, _poll_ok(pid="p2")),
        ],
    )
    client = ApolloPhoneClient(api_key="k", session=session, sleep_fn=lambda s: None)

    for i, (domain, pid, first, last) in enumerate(
        [
            ("a.de", "p1", "Eva", "Eins"),
            ("b.de", "p2", "Max", "Zwei"),
        ]
    ):
        row = BusinessRow.from_dict(
            {"domain": domain, "company_name": "F", "first_name_1": first, "last_name_1": last}
        )
        row.apollo = [
            {
                "status": "pending",
                "request_id": f"rid-{i}",
                "person_id": pid,
            }
        ]
        rows.append(row)

    ticks: List[tuple] = []
    poll_apollo_pending(
        rows,
        client,
        sleep_fn=lambda s: None,
        timeout_s=120,
        progress_callback=lambda d, t, e: ticks.append((d, t)),
    )
    # First round: 0/2, 1/2, 2/2 (before/after each of 2 rids) while still pending,
    # then second round resolves both.
    assert any(d == 0 and t == 2 for d, t in ticks)
    assert any(d == 1 and t == 2 for d, t in ticks)
    assert any(d == 2 and t == 2 for d, t in ticks)
    assert rows[0].apollo[0]["status"] == "ok"
    assert rows[1].apollo[0]["status"] == "ok"


def test_apollo_only_step_writes_imprint_and_reuses_cache(tmp_path: Path, monkeypatch):
    from campaign_pipeline.config import CampaignConfig
    from campaign_pipeline.io.readers import load_rows_as_business
    from campaign_pipeline.io.writers import write_business_rows
    from campaign_pipeline.naming import stage_path
    from campaign_pipeline.steps.imprint_scrape import run_apollo_only_step

    campaign = tmp_path / "camp"
    (campaign / "lists").mkdir(parents=True)
    (campaign / ".pipeline").mkdir(parents=True)
    cfg = CampaignConfig(campaign_dir=campaign, base_name="test")

    addr = {
        "company_name": "Firma GmbH",
        "street": "Hauptstr.",
        "house_number": "1",
        "postcode": "10115",
        "city": "Berlin",
    }
    write_business_rows(
        stage_path(campaign, "test", "imprint"),
        [
            BusinessRow.from_dict(
                {
                    "domain": "firma.de",
                    **addr,
                    "first_name_1": "Eva",
                    "last_name_1": "Beispiel",
                }
            ),
            BusinessRow.from_dict(
                {
                    "domain": "dup.de",
                    **addr,
                    "first_name_1": "Max",
                    "last_name_1": "Muster",
                }
            ),
        ],
    )

    monkeypatch.setenv("APOLLO_API_KEY", "test-key")
    session = FakeSession(_person())
    real_client = ApolloPhoneClient

    def fake_client(**kwargs):
        kwargs["api_key"] = "test-key"
        kwargs["session"] = session
        kwargs.setdefault("sleep_fn", lambda s: None)
        return real_client(**kwargs)

    monkeypatch.setattr("campaign_pipeline.steps.imprint_scrape.ApolloPhoneClient", fake_client)

    out, stats = run_apollo_only_step(cfg, sleep_fn=lambda s: None)
    assert out.exists()
    assert stats["apollo_only"] is True
    assert stats["scraped"] == 0
    assert stats["apollo_skipped_address_dup"] == 1
    assert stats["apollo_candidates"] == 1
    assert stats["apollo_ok"] == 1

    rows = load_rows_as_business(out)
    by_domain = {r.domain: r for r in rows}
    assert by_domain["firma.de"].apollo[0].get("phone") == "+491701234567"
    # Address-dup row must not have been queried (no apollo slot filled via match).
    assert not (by_domain["dup.de"].apollo and by_domain["dup.de"].apollo[0].get("person_id"))

    # Second run must hit cache — no new HTTP calls for the same person.
    calls_after_first = len(session.calls)
    run_apollo_only_step(cfg, sleep_fn=lambda s: None)
    assert len(session.calls) == calls_after_first


def test_apollo_only_step_requires_imprint(tmp_path: Path):
    from campaign_pipeline.config import CampaignConfig
    from campaign_pipeline.steps.imprint_scrape import run_apollo_only_step
    import pytest

    campaign = tmp_path / "camp"
    (campaign / "lists").mkdir(parents=True)
    cfg = CampaignConfig(campaign_dir=campaign, base_name="test")
    with pytest.raises(FileNotFoundError):
        run_apollo_only_step(cfg)


def test_apollo_only_step_custom_input_and_output(tmp_path: Path, monkeypatch):
    from campaign_pipeline.config import CampaignConfig
    from campaign_pipeline.io.readers import load_rows_as_business
    from campaign_pipeline.io.writers import write_business_rows
    from campaign_pipeline.steps.imprint_scrape import run_apollo_only_step

    campaign = tmp_path / "camp"
    (campaign / "lists").mkdir(parents=True)
    (campaign / ".pipeline").mkdir(parents=True)
    cfg = CampaignConfig(campaign_dir=campaign, base_name="test")

    custom_in = campaign / "lists" / "custom_source.csv"
    custom_out = campaign / "lists" / "custom_with_apollo.csv"
    write_business_rows(
        custom_in,
        [
            BusinessRow.from_dict(
                {
                    "domain": "firma.de",
                    "company_name": "Firma GmbH",
                    "street": "Hauptstr.",
                    "house_number": "1",
                    "postcode": "10115",
                    "first_name_1": "Eva",
                    "last_name_1": "Beispiel",
                }
            )
        ],
    )

    monkeypatch.setenv("APOLLO_API_KEY", "test-key")
    session = FakeSession(_person())
    real_client = ApolloPhoneClient

    def fake_client(**kwargs):
        kwargs["api_key"] = "test-key"
        kwargs["session"] = session
        kwargs.setdefault("sleep_fn", lambda s: None)
        return real_client(**kwargs)

    monkeypatch.setattr("campaign_pipeline.steps.imprint_scrape.ApolloPhoneClient", fake_client)

    out, stats = run_apollo_only_step(
        cfg,
        input_path=custom_in,
        output_path=custom_out,
        sleep_fn=lambda s: None,
    )
    assert out == custom_out
    assert stats["input_path"] == str(custom_in)
    assert stats["output_path"] == str(custom_out)
    assert custom_out.exists()
    assert not (campaign / "lists" / "test_imprint.csv").exists()
    row = load_rows_as_business(custom_out)[0]
    assert row.apollo[0].get("phone") == "+491701234567"


def test_apollo_only_step_skips_without_api_key(tmp_path: Path, monkeypatch):
    from campaign_pipeline.config import CampaignConfig
    from campaign_pipeline.io.writers import write_business_rows
    from campaign_pipeline.naming import stage_path
    from campaign_pipeline.steps.imprint_scrape import run_apollo_only_step

    campaign = tmp_path / "camp"
    (campaign / "lists").mkdir(parents=True)
    cfg = CampaignConfig(campaign_dir=campaign, base_name="test")
    write_business_rows(
        stage_path(campaign, "test", "imprint"),
        [BusinessRow.from_dict({"domain": "a.de", "first_name_1": "A", "last_name_1": "B"})],
    )
    monkeypatch.delenv("APOLLO_API_KEY", raising=False)
    _, stats = run_apollo_only_step(cfg)
    assert stats["apollo_skipped_no_key"] == 1
