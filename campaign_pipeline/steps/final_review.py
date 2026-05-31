from __future__ import annotations

import csv
import json
import logging
import re
from pathlib import Path
from typing import Iterable, List, Optional

from ..config import CampaignConfig
from ..io.readers import load_rows_as_business
from ..io.writers import write_final_business_rows
from ..models import BusinessRow
from ..naming import review_decisions_path, review_issues_path, stage_path
from ..registry import PipelineRegistry

logger = logging.getLogger(__name__)

POSTCODE_PATTERN = re.compile(r"^\d{5}$")

REQUIRED_FIELDS = [
    "first_name_1",
    "last_name_1",
    "salutation_1",
    "street",
    "house_number",
    "postcode",
    "city",
    "template",
]


def _row_dict(row: BusinessRow, max_directors: int) -> dict:
    return row.to_dict(max_directors=max_directors)


def _missing_fields(row: BusinessRow, max_directors: int) -> List[str]:
    data = _row_dict(row, max_directors)
    missing: List[str] = []
    for field in REQUIRED_FIELDS:
        val = (data.get(field) or "").strip()
        if not val:
            missing.append(field)
    return missing


def _postcode_valid(postcode: Optional[str]) -> bool:
    if not postcode or not str(postcode).strip():
        return False
    return bool(POSTCODE_PATTERN.match(str(postcode).strip()))


def load_review_issues(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        return [dict(row) for row in reader]


def load_flagged_indices(issues_path: Path) -> set[int]:
    indices: set[int] = set()
    for issue in load_review_issues(issues_path):
        raw = (issue.get("row_index") or "").strip()
        if raw.isdigit():
            indices.add(int(raw))
    return indices


def load_review_decisions(path: Path) -> dict[int, str]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[int, str] = {}
    for key, value in raw.items():
        if str(key).isdigit() and value in ("keep", "discard"):
            out[int(key)] = value
    return out


def save_review_decisions(path: Path, decisions: dict[int, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {str(k): v for k, v in sorted(decisions.items()) if v in ("keep", "discard")}
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def run_final_review(
    config: CampaignConfig,
    input_path: Path | None = None,
    *,
    min_score: float | None = None,
    registry: Optional[PipelineRegistry] = None,
) -> tuple[Path, dict]:
    path = input_path or stage_path(config.campaign_dir, config.base_name, "imprint")
    rows = load_rows_as_business(path, max_directors=config.max_directors)
    threshold = min_score if min_score is not None else config.score_config.pass_threshold

    issues_path = review_issues_path(config.campaign_dir, config.base_name)
    decisions_path = review_decisions_path(config.campaign_dir, config.base_name)
    flagged_indices = load_flagged_indices(issues_path)
    decisions = load_review_decisions(decisions_path)
    keep_overrides = {idx for idx, choice in decisions.items() if choice == "keep"}

    kept: List[BusinessRow] = []
    stats = {
        "input": len(rows),
        "removed_score": 0,
        "removed_required": 0,
        "removed_postcode": 0,
        "removed_issues": 0,
        "kept": 0,
        "flagged_for_review": len(flagged_indices),
        "kept_despite_flag": 0,
    }

    for idx, row in enumerate(rows, start=1):
        score = row.match_score
        if score is None or score < threshold:
            stats["removed_score"] += 1
            if registry:
                registry.record_drop(row.domain, "failed_final_score")
            continue
        missing = _missing_fields(row, config.max_directors)
        if missing:
            stats["removed_required"] += 1
            if registry:
                registry.record_drop(row.domain, "failed_required_fields")
            continue
        if not _postcode_valid(row.postcode):
            stats["removed_postcode"] += 1
            if registry:
                registry.record_drop(row.domain, "invalid_postcode")
            continue
        if idx in flagged_indices and idx not in keep_overrides:
            stats["removed_issues"] += 1
            if registry:
                registry.record_drop(row.domain, "llm_review_flagged")
            continue
        if idx in flagged_indices and idx in keep_overrides:
            stats["kept_despite_flag"] += 1
        kept.append(row)
        if registry:
            registry.mark(row.domain, "final")

    output = stage_path(config.campaign_dir, config.base_name, "final")
    write_final_business_rows(output, kept, max_directors=config.max_directors)
    stats["kept"] = len(kept)
    logger.info("Final review: %s", stats)
    return output, stats


def output_check_stats(rows: Iterable[BusinessRow]) -> dict:
    rows_list = list(rows)
    n = len(rows_list)
    if n == 0:
        return {"count": 0}
    fields = [
        "company_name",
        "street",
        "house_number",
        "postcode",
        "city",
        "email",
        "phone",
        "match_score",
    ]
    missing = {}
    for field in fields:
        count = sum(
            1
            for r in rows_list
            if not (getattr(r, field, None) or str(getattr(r, field, "") or "").strip())
        )
        if count:
            missing[field] = count
    invalid_plz = sum(1 for r in rows_list if r.postcode and not _postcode_valid(r.postcode))
    return {"count": n, "missing": missing, "invalid_postcode": invalid_plz}
