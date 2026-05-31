from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Optional

from list_processing.llm.base import LLMClient
from scripts.business.prompt_manager import get_prompt

from ..config import CampaignConfig, ScoreConfig
from ..io.readers import load_rows_as_business
from ..io.writers import write_business_rows
from ..naming import stage_path
from ..registry import PipelineRegistry
from .scoring import DomainScoringService, apply_analysis_flat

logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover

    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable


def _load_score_config(prompt_name: str, override: ScoreConfig) -> ScoreConfig:
    prompt = get_prompt(prompt_name)
    if prompt and isinstance(prompt._raw_data, dict):
        cfg = ScoreConfig.from_prompt_data(prompt._raw_data)
        if override.pass_threshold != ScoreConfig().pass_threshold:
            cfg.pass_threshold = override.pass_threshold
        return cfg
    return override


def run_scoring(
    config: CampaignConfig,
    llm: LLMClient,
    input_path: Path | None = None,
    *,
    registry: Optional[PipelineRegistry] = None,
    only_domains: set[str] | None = None,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
) -> tuple[Path, dict]:
    if input_path is None:
        deduped = stage_path(config.campaign_dir, config.base_name, "raw_deduped")
        path = deduped if deduped.exists() else stage_path(config.campaign_dir, config.base_name, "raw")
    else:
        path = input_path
    rows = load_rows_as_business(path, max_directors=config.max_directors)

    score_config = _load_score_config(config.scoring_prompt_name, config.score_config)
    config.score_config = score_config

    prompt = get_prompt(config.scoring_prompt_name)
    if prompt is None:
        raise RuntimeError(f"Scoring prompt '{config.scoring_prompt_name}' not found")

    service = DomainScoringService(llm, prompt, score_config)

    to_score = [
        r
        for r in rows
        if (only_domains is None or r.domain in only_domains)
        and (registry is None or registry.should_process(r.domain, "scored"))
    ]

    total = len(to_score)
    if progress_callback and total == 0:
        progress_callback(0, 0, 0.0, "")

    def _worker(row):
        try:
            service.score_row(row)
            apply_analysis_flat(row)
        except Exception as exc:
            logger.warning("Scoring failed for %s: %s", row.domain, exc)

    started = time.monotonic()
    completed = 0
    with ThreadPoolExecutor(max_workers=config.max_workers_http) as pool:
        futures = {pool.submit(_worker, r): r for r in to_score}
        iterator = as_completed(futures)
        if progress_callback is None:
            iterator = tqdm(iterator, total=total, desc="Scoring")
        for fut in iterator:
            row = futures[fut]
            completed += 1
            if progress_callback is not None:
                progress_callback(completed, total, time.monotonic() - started, row.domain)

    output_rows: list = []
    passed = 0
    failed = 0
    for row in rows:
        if row.passed_score_filter is False and not config.keep_failed_scores:
            failed += 1
            if registry:
                registry.record_drop(row.domain, "failed_score_filter")
            continue
        if row.passed_score_filter:
            passed += 1
        if registry and row.passed_score_filter:
            registry.mark(row.domain, "scored")
        output_rows.append(row)

    output = stage_path(config.campaign_dir, config.base_name, "scored")
    write_business_rows(output, output_rows, max_directors=config.max_directors)
    stats = {
        "input": len(rows),
        "scored": len(to_score),
        "output": len(output_rows),
        "passed": passed,
        "failed_filter": failed,
        "score_scale": score_config.scale,
        "pass_threshold": score_config.pass_threshold,
    }
    logger.info("Scoring complete: %s", stats)
    return output, stats
