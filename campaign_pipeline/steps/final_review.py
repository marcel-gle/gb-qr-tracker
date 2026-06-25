from __future__ import annotations

import csv
import json
import logging
import re
from pathlib import Path
from typing import Iterable, List, Optional

from ..config import CampaignConfig
from ..io.readers import load_rows_as_business
from ..io.writers import write_business_rows, write_final_business_rows
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


HOUSE_TOKEN_PATTERN = re.compile(r"^\d{1,4}[A-Za-z]?(?:[-/]\d{1,4}[A-Za-z]?)?$")


def _compact_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _looks_like_house_number(token: str) -> bool:
    return bool(HOUSE_TOKEN_PATTERN.match(token))


def normalize_street_and_house(street: str | None, house_number: str | None) -> tuple[str, str]:
    street_clean = _compact_space(street)
    house_clean = _compact_space(house_number)
    if not street_clean:
        return "", house_clean

    parts = street_clean.split(" ")
    if len(parts) >= 2 and _looks_like_house_number(parts[-1]):
        tail = parts[-1]
        prev = parts[-2]

        if _looks_like_house_number(prev) and prev.lower() == tail.lower():
            # Example: "Danziger Str. 1 1" -> "Danziger Str.", "1"
            return " ".join(parts[:-2]).strip(), house_clean or tail

        if house_clean and house_clean.lower() == tail.lower():
            # Example: street="Musterstr. 12", house_number="12" -> remove duplication in street.
            return " ".join(parts[:-1]).strip(), house_clean

        if not house_clean:
            # Example: street="Musterstr. 12", house_number="" -> split into separate fields.
            return " ".join(parts[:-1]).strip(), tail

    return street_clean, house_clean


def _has_usable_address(row: BusinessRow) -> bool:
    return bool((row.street or "").strip()) and bool((row.city or "").strip()) and _postcode_valid(row.postcode)


def build_clean_list(
    config: CampaignConfig,
    *,
    drop_missing_address: bool = True,
    source_path: Path | None = None,
) -> tuple[Path, dict]:
    """Create the cleaned intermediate list from the (untouched) imprint CSV.

    Normalizes street/house number on every row and optionally drops rows with no
    usable address. Writes the result to {base}_cleaned.csv and leaves the original
    imprint file unchanged.
    """
    src = source_path or stage_path(config.campaign_dir, config.base_name, "imprint")
    output = stage_path(config.campaign_dir, config.base_name, "cleaned")
    stats = {
        "input": 0,
        "normalized": 0,
        "dropped_missing_address": 0,
        "kept": 0,
        "output_path": str(output),
        "missing_imprint": False,
    }
    if not src.exists():
        stats["missing_imprint"] = True
        return output, stats

    rows = load_rows_as_business(src, max_directors=config.max_directors)
    stats["input"] = len(rows)

    kept: List[BusinessRow] = []
    for row in rows:
        new_street, new_house = normalize_street_and_house(row.street, row.house_number)
        if (new_street, new_house) != (row.street or "", row.house_number or ""):
            row.street, row.house_number = new_street, new_house
            stats["normalized"] += 1
        if drop_missing_address and not _has_usable_address(row):
            stats["dropped_missing_address"] += 1
            continue
        kept.append(row)

    stats["kept"] = len(kept)
    write_business_rows(output, kept, max_directors=config.max_directors)
    logger.info("Clean list: %s", {k: v for k, v in stats.items() if k != "output_path"})
    return output, stats


def cleaned_or_imprint_path(config: CampaignConfig) -> Path:
    cleaned = stage_path(config.campaign_dir, config.base_name, "cleaned")
    if cleaned.exists():
        return cleaned
    return stage_path(config.campaign_dir, config.base_name, "imprint")


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


def _compute_final_review(
    rows: List[BusinessRow],
    config: CampaignConfig,
    *,
    min_score: float,
    drop_missing_address: bool,
    flagged_indices: set[int],
    keep_overrides: set[int],
    registry: Optional[PipelineRegistry] = None,
) -> tuple[List[BusinessRow], dict]:
    kept: List[BusinessRow] = []
    stats = {
        "input": len(rows),
        "removed_score": 0,
        "removed_missing_address": 0,
        "removed_required": 0,
        "removed_postcode": 0,
        "removed_issues": 0,
        "kept": 0,
        "flagged_for_review": len(flagged_indices),
        "kept_despite_flag": 0,
        "normalized_address_rows": 0,
    }

    for idx, row in enumerate(rows, start=1):
        before_street = _compact_space(row.street)
        before_house = _compact_space(row.house_number)
        row.street, row.house_number = normalize_street_and_house(row.street, row.house_number)
        if (row.street, row.house_number) != (before_street, before_house):
            stats["normalized_address_rows"] += 1

        score = row.match_score
        if score is None or score < min_score:
            stats["removed_score"] += 1
            if registry:
                registry.record_drop(row.domain, "failed_final_score")
            continue
        if drop_missing_address:
            if not row.street or not row.city or not _postcode_valid(row.postcode):
                stats["removed_missing_address"] += 1
                if registry:
                    registry.record_drop(row.domain, "missing_address")
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

    stats["kept"] = len(kept)
    return kept, stats


def preview_final_review(
    config: CampaignConfig,
    input_path: Path | None = None,
    *,
    min_score: float | None = None,
    drop_missing_address: bool = False,
    review_decisions: dict[int, str] | None = None,
) -> dict:
    path = input_path or cleaned_or_imprint_path(config)
    if not path.exists():
        return {"input": 0, "kept": 0, "missing_imprint": True}

    rows = load_rows_as_business(path, max_directors=config.max_directors)
    threshold = min_score if min_score is not None else config.score_config.pass_threshold

    issues_path = review_issues_path(config.campaign_dir, config.base_name)
    if review_decisions is None:
        decisions = load_review_decisions(
            review_decisions_path(config.campaign_dir, config.base_name)
        )
    else:
        decisions = review_decisions
    flagged_indices = load_flagged_indices(issues_path)
    keep_overrides = {idx for idx, choice in decisions.items() if choice == "keep"}

    _, stats = _compute_final_review(
        rows,
        config,
        min_score=threshold,
        drop_missing_address=drop_missing_address,
        flagged_indices=flagged_indices,
        keep_overrides=keep_overrides,
    )
    return stats


def run_final_review(
    config: CampaignConfig,
    input_path: Path | None = None,
    *,
    min_score: float | None = None,
    drop_missing_address: bool = False,
    registry: Optional[PipelineRegistry] = None,
) -> tuple[Path, dict]:
    path = input_path or cleaned_or_imprint_path(config)
    rows = load_rows_as_business(path, max_directors=config.max_directors)
    threshold = min_score if min_score is not None else config.score_config.pass_threshold

    issues_path = review_issues_path(config.campaign_dir, config.base_name)
    decisions_path = review_decisions_path(config.campaign_dir, config.base_name)
    flagged_indices = load_flagged_indices(issues_path)
    decisions = load_review_decisions(decisions_path)
    keep_overrides = {idx for idx, choice in decisions.items() if choice == "keep"}

    kept, stats = _compute_final_review(
        rows,
        config,
        min_score=threshold,
        drop_missing_address=drop_missing_address,
        flagged_indices=flagged_indices,
        keep_overrides=keep_overrides,
        registry=registry,
    )

    output = stage_path(config.campaign_dir, config.base_name, "final")
    write_final_business_rows(output, kept, max_directors=config.max_directors)
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
