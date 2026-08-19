from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Optional

from list_processing.llm.base import LLMClient

from ..config import CampaignConfig
from ..io.readers import load_rows_as_business
from ..io.writers import write_business_rows
from ..models import BusinessRow
from ..naming import cache_path, stage_path
from ..registry import PipelineRegistry
from ..imprint.extract import ImprintExtractor, run_imprint_scrape

logger = logging.getLogger(__name__)


def row_is_missing_core_fields(row: BusinessRow) -> bool:
    """True when a row lacks a street or any named managing director.

    Used by the ``only_missing_fields`` re-scrape mode to target rows that a
    previous imprint run left incomplete.
    """
    if not (row.street or "").strip():
        return True
    for d in row.directors or []:
        if (d or {}).get("first_name") or (d or {}).get("last_name"):
            return False
    return True


def resolve_imprint_input_path(config: CampaignConfig) -> Path:
    """Prefer scored output; fall back to raw_deduped / raw when scoring was skipped."""
    scored = stage_path(config.campaign_dir, config.base_name, "scored")
    if scored.exists():
        return scored
    deduped = stage_path(config.campaign_dir, config.base_name, "raw_deduped")
    if deduped.exists():
        return deduped
    return stage_path(config.campaign_dir, config.base_name, "raw")


def _is_scored_stage_path(config: CampaignConfig, path: Path) -> bool:
    scored = stage_path(config.campaign_dir, config.base_name, "scored")
    try:
        return path.resolve() == scored.resolve()
    except OSError:
        return path.name == scored.name


def run_imprint_step(
    config: CampaignConfig,
    llm: LLMClient,
    input_path: Path | None = None,
    *,
    registry: Optional[PipelineRegistry] = None,
    only_domains: set[str] | None = None,
    skip_score_filter: bool = False,
    only_missing_fields: bool = False,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
) -> tuple[Path, dict]:
    imprint_output = stage_path(config.campaign_dir, config.base_name, "imprint")

    if only_missing_fields:
        # Re-scrape rows from a previous imprint run that are still missing a
        # street or a named managing director. Reads the imprint output so
        # already-complete rows are preserved, and forces a fresh fetch
        # (bypassing the per-domain cache) so improved fetch logic can run.
        path = input_path or imprint_output
        if not path.exists():
            raise FileNotFoundError(
                f"Imprint output not found: {path}. Run a full imprint scrape first."
            )
    else:
        path = input_path or resolve_imprint_input_path(config)
        if not path.exists():
            raise FileNotFoundError(
                f"Imprint input not found: {path}. Run merge (and optionally score) first."
            )

    using_scored = _is_scored_stage_path(config, path)
    apply_pass_filter = (
        config.pass_score_filter
        and using_scored
        and not skip_score_filter
        and not only_missing_fields
    )
    if only_missing_fields:
        logger.info("Imprint re-scrape: only rows missing street/name from %s.", path.name)
    elif not using_scored:
        logger.info(
            "Imprint using %s (no _scored.csv) — score pass filter disabled.",
            path.name,
        )
    elif skip_score_filter:
        logger.info("Imprint score pass filter skipped by request.")

    rows = load_rows_as_business(path, max_directors=config.max_directors)
    input_count = len(rows)

    if apply_pass_filter:
        rows = [r for r in rows if r.passed_score_filter is True]

    if only_missing_fields:
        to_scrape = [
            r
            for r in rows
            if (only_domains is None or r.domain in only_domains)
            and row_is_missing_core_fields(r)
        ]
    else:
        to_scrape = [
            r
            for r in rows
            if (only_domains is None or r.domain in only_domains)
            and (registry is None or registry.should_process(r.domain, "imprint"))
        ]

    cache = cache_path(config.campaign_dir, "imprint")
    extractor = ImprintExtractor(
        llm,
        cache_path=cache,
        enable_northdata=config.enable_northdata_fallback,
    )
    run_imprint_scrape(
        to_scrape,
        extractor,
        max_workers=config.max_workers_http,
        force=only_missing_fields,
        progress_callback=progress_callback,
    )

    for row in to_scrape:
        if registry:
            registry.mark(row.domain, "imprint")

    write_business_rows(imprint_output, rows, max_directors=config.max_directors)
    stats = {
        "input": input_count,
        "after_score_filter": len(rows),
        "scraped": len(to_scrape),
        "output": len(rows),
        "input_path": str(path),
        "used_scored": using_scored,
        "score_filter_applied": apply_pass_filter,
        "only_missing_fields": only_missing_fields,
    }
    logger.info("Imprint scrape complete: %s", stats)
    return imprint_output, stats
