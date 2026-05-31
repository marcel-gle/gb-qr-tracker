from __future__ import annotations

from pathlib import Path
from typing import Literal

StageName = Literal["raw", "raw_deduped", "scored", "imprint", "final"]

CAMPAIGN_SUBFOLDERS = ("lists", "lists/incoming", "templates", "pdf_output")
PIPELINE_DIR_NAME = ".pipeline"


def lists_dir(campaign_dir: Path) -> Path:
    return campaign_dir / "lists"


def incoming_dir(campaign_dir: Path) -> Path:
    return campaign_dir / "lists" / "incoming"


def pipeline_dir(campaign_dir: Path) -> Path:
    return campaign_dir / PIPELINE_DIR_NAME


def stage_path(campaign_dir: Path, base: str, stage: StageName) -> Path:
    return lists_dir(campaign_dir.resolve()) / f"{base}_{stage}.csv"


def state_path(campaign_dir: Path) -> Path:
    return pipeline_dir(campaign_dir) / "state.json"


def manifest_path(campaign_dir: Path) -> Path:
    return pipeline_dir(campaign_dir) / "manifest.json"


def review_issues_path(campaign_dir: Path, base: str) -> Path:
    return lists_dir(campaign_dir) / f"{base}_final.review_issues.csv"


def review_decisions_path(campaign_dir: Path, base: str) -> Path:
    return pipeline_dir(campaign_dir) / f"{base}_final.review_decisions.json"


def cache_path(campaign_dir: Path, name: str) -> Path:
    return pipeline_dir(campaign_dir) / f"{name}.cache.pkl"
