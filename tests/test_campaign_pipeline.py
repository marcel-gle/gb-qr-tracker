from __future__ import annotations

from pathlib import Path

import pytest

from campaign_pipeline.config import CampaignConfig, ScoreConfig
from campaign_pipeline.models import BusinessRow, normalize_domain
from campaign_pipeline.naming import stage_path
from campaign_pipeline.registry import PipelineRegistry
from campaign_pipeline.steps.scoring import evaluate_pass, extract_score_from_result, normalize_score
from campaign_pipeline.imprint.extract import gender_to_salutation

SAMPLES = Path(__file__).resolve().parent.parent / "campaign_pipeline" / "samples"


def _make_pipeline(tmp_path: Path):
    from campaign_pipeline.pipeline import CampaignPipeline

    campaign = tmp_path / "camp"
    campaign.mkdir()
    (campaign / "lists" / "incoming").mkdir(parents=True)
    cfg = CampaignConfig(campaign_dir=campaign, base_name="test")
    return CampaignPipeline(cfg), campaign


def test_normalize_domain():
    assert normalize_domain("https://WWW.Example.de/path") == "example.de"
    assert normalize_domain("", email_fallback="info@test.co.uk") == "test.co.uk"


def test_gender_to_salutation():
    assert gender_to_salutation("Herr") == "Herr"
    assert gender_to_salutation("Frau") == "Frau"
    assert gender_to_salutation("male") == "Herr"
    assert gender_to_salutation("female") == "Frau"
    assert gender_to_salutation(None) is None
    assert gender_to_salutation("null") is None


def test_binary_solar_fields():
    cfg = ScoreConfig(field="score", scale="binary", pass_threshold=1)
    norm, raw, passed = extract_score_from_result(
        {"score": 1, "verkauft": True, "installiert": False}, cfg
    )
    assert passed and norm == 1.0

    norm, raw, passed = extract_score_from_result(
        {"verkauft": False, "installiert": True}, cfg
    )
    assert passed and norm == 1.0

    norm, raw, passed = extract_score_from_result(
        {"score": 0, "verkauft": False, "installiert": False}, cfg
    )
    assert not passed and norm == 0.0

    norm, raw, passed = extract_score_from_result(
        {"verkauft": False, "installiert": False}, cfg
    )
    assert not passed and norm == 0.0


def test_evaluate_pass_without_pass_rules_uses_score_only():
    cfg = ScoreConfig(field="score", scale="0-10", pass_threshold=6)
    _, _, passed = evaluate_pass({"score": 7, "makler": False}, cfg, None)
    assert passed
    _, _, failed = evaluate_pass({"score": 5, "makler": True}, cfg, {})
    assert not failed


def test_score_config_scales():
    cfg = ScoreConfig(scale="0-5", pass_threshold=4)
    norm, raw, passed = normalize_score(4.5, cfg)
    assert norm == 4.5 and passed

    cfg10 = ScoreConfig(field="score", scale="0-10", pass_threshold=6)
    norm, raw, passed = extract_score_from_result({"score": 7}, cfg10)
    assert passed and norm == 7

    cfgb = ScoreConfig(scale="binary", pass_threshold=1)
    norm, raw, passed = normalize_score(True, cfgb)
    assert norm == 1.0 and passed


def test_business_row_directors():
    row = BusinessRow.from_dict(
        {
            "domain": "test.de",
            "first_name_1": "Max",
            "last_name_1": "Muster",
            "salutation_1": "Herr",
        }
    )
    d = row.to_dict()
    assert d["first_name_1"] == "Max"


def test_merge_and_dedupe_domain(tmp_path: Path):
    import shutil

    pipe, campaign = _make_pipeline(tmp_path)
    pipe.ensure_campaign_dirs()

    shutil.copy(SAMPLES / "raw_a.csv", campaign / "lists" / "incoming" / "raw_a.csv")
    shutil.copy(SAMPLES / "raw_b.csv", campaign / "lists" / "incoming" / "raw_b.csv")

    stats = pipe.merge_raw(
        [campaign / "lists" / "incoming" / "raw_a.csv", campaign / "lists" / "incoming" / "raw_b.csv"]
    )
    assert stats["rows_total"] >= 4

    dedupe_stats = pipe.dedupe_domain()
    assert dedupe_stats["removed"] >= 1

    raw_path = stage_path(campaign, "test", "raw")
    deduped_path = stage_path(campaign, "test", "raw_deduped")
    assert not raw_path.exists()
    assert deduped_path.exists()
    assert deduped_path.name == "test_raw_deduped.csv"
    assert dedupe_stats["kept"] < stats["rows_total"]


def test_registry_incremental(tmp_path: Path):
    campaign = tmp_path / "camp2"
    reg = PipelineRegistry(campaign_dir=campaign)
    reg.mark("a.de", "final")
    reg.save()
    reg2 = PipelineRegistry.load(campaign)
    assert "a.de" in reg2.domains
    assert reg2.domains["a.de"].stage == "final"
    assert reg2.should_process("a.de", "scored") is False
    assert reg2.should_process("b.de", "scored") is True


def test_stage_paths(tmp_path: Path):
    p = stage_path(tmp_path, "camp", "raw")
    assert p.name == "camp_raw.csv"


def test_final_review_respects_keep_decisions(tmp_path: Path):
    from campaign_pipeline.steps.final_review import (
        load_flagged_indices,
        run_final_review,
        save_review_decisions,
    )
    from campaign_pipeline.naming import review_decisions_path, review_issues_path, stage_path
    from campaign_pipeline.io.writers import write_business_rows

    campaign = tmp_path / "camp"
    (campaign / "lists").mkdir(parents=True)
    (campaign / ".pipeline").mkdir(parents=True)
    cfg = CampaignConfig(campaign_dir=campaign, base_name="test", score_config=ScoreConfig(scale="binary", pass_threshold=1))

    rows = [
        BusinessRow.from_dict(
            {
                "domain": "a.de",
                "match_score": 1,
                "first_name_1": "Max",
                "last_name_1": "Muster",
                "salutation_1": "Herr",
                "street": "Hauptstr.",
                "house_number": "1",
                "postcode": "10115",
                "city": "Berlin",
                "template": "t.pdf",
            }
        ),
        BusinessRow.from_dict(
            {
                "domain": "b.de",
                "match_score": 1,
                "first_name_1": "Erika",
                "last_name_1": "Test",
                "salutation_1": "Frau",
                "street": "Nebenstr.",
                "house_number": "2",
                "postcode": "80331",
                "city": "München",
                "template": "t.pdf",
            }
        ),
    ]
    imprint_path = stage_path(campaign, "test", "imprint")
    write_business_rows(imprint_path, rows)

    issues_path = review_issues_path(campaign, "test")
    issues_path.write_text(
        "row_index;domain;company_name;address;issue;severity;details\n"
        "2;b.de;Test;Nebenstr. 2, 80331 München;suspicious_data;medium;looks odd\n",
        encoding="utf-8",
    )
    assert load_flagged_indices(issues_path) == {2}

    _, stats = run_final_review(cfg)
    assert stats["kept"] == 1
    assert stats["removed_issues"] == 1

    save_review_decisions(review_decisions_path(campaign, "test"), {2: "keep"})
    _, stats2 = run_final_review(cfg)
    assert stats2["kept"] == 2
    assert stats2["kept_despite_flag"] == 1

    final_path = stage_path(campaign, "test", "final")
    assert final_path.exists()
    header = final_path.read_text(encoding="utf-8").splitlines()[0].split(";")
    from campaign_pipeline.models import final_csv_fieldnames

    assert header == final_csv_fieldnames()
    assert "domain" not in header
    assert "match_score" not in header
    assert "Template" in header
    assert final_path.read_text(encoding="utf-8").splitlines()[1].endswith("t.pdf")


def test_business_row_has_score_result():
    empty = BusinessRow.from_dict({"domain": "a.de"})
    assert empty.has_score_result() is False

    with_score = BusinessRow.from_dict(
        {"domain": "b.de", "match_score": 6, "domain_analysis_raw": '{"score": 6, "makler": true}'}
    )
    assert with_score.has_score_result() is True

    score_only = BusinessRow.from_dict({"domain": "c.de", "match_score": 4})
    assert score_only.has_score_result() is True


def test_run_scoring_only_missing_preserves_existing(tmp_path: Path, monkeypatch):
    from campaign_pipeline.io.readers import load_rows_as_business
    from campaign_pipeline.io.writers import write_business_rows
    from campaign_pipeline.steps.run_scoring import run_scoring
    from campaign_pipeline.steps.scoring import DomainScoringService
    from unittest.mock import MagicMock

    campaign = tmp_path / "camp"
    (campaign / "lists").mkdir(parents=True)
    cfg = CampaignConfig(
        campaign_dir=campaign,
        base_name="test",
        scoring_prompt_name="immo_makler_webseite",
    )

    rows = [
        BusinessRow.from_dict(
            {
                "domain": "done.de",
                "match_score": 7,
                "domain_analysis_raw": '{"score": 7, "makler": true}',
                "passed_score_filter": True,
            }
        ),
        BusinessRow.from_dict({"domain": "pending.de", "company_name": "Pending GmbH"}),
    ]
    scored_path = stage_path(campaign, "test", "scored")
    write_business_rows(scored_path, rows)

    def fake_score_row(self, row: BusinessRow) -> bool:
        row.domain_analysis_raw = {"score": 5, "makler": True}
        row.match_score = 5.0
        row.score_raw = 5.0
        row.passed_score_filter = False
        return True

    monkeypatch.setattr(DomainScoringService, "score_row", fake_score_row)

    _, stats = run_scoring(cfg, MagicMock(), only_missing=True)
    assert stats["missing_before"] == 1
    assert stats["scored"] == 1
    assert stats["missing_after"] == 0

    reloaded = load_rows_as_business(scored_path)
    by_domain = {r.domain: r for r in reloaded}
    assert by_domain["done.de"].match_score == 7.0
    assert by_domain["pending.de"].match_score == 5.0
    assert by_domain["pending.de"].domain_analysis_raw == {"score": 5, "makler": True}
