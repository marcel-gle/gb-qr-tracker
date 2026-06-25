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
from ..naming import scoring_cache_path, stage_path
from ..imprint.fetch import close_browser_pool
from ..registry import PipelineRegistry
from ..scoring.cache import ScoringResultCache, score_payload_from_row
from .scoring import DomainScoringService, apply_analysis_flat

logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover

    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable


def _default_scoring_input_path(config: CampaignConfig) -> Path:
    deduped = stage_path(config.campaign_dir, config.base_name, "raw_deduped")
    if deduped.exists():
        return deduped
    return stage_path(config.campaign_dir, config.base_name, "raw")


def _resolve_scoring_input_path(config: CampaignConfig, *, only_missing: bool) -> Path:
    if only_missing:
        scored = stage_path(config.campaign_dir, config.base_name, "scored")
        if scored.exists():
            return scored
        logger.warning(
            "Score only missing: %s not found — falling back to raw input.",
            scored.name,
        )
    return _default_scoring_input_path(config)


def _load_score_config(prompt_name: str, override: ScoreConfig) -> ScoreConfig:
    prompt = get_prompt(prompt_name)
    if prompt and isinstance(prompt._raw_data, dict):
        cfg = ScoreConfig.from_prompt_data(prompt._raw_data)
        if override.pass_threshold != ScoreConfig().pass_threshold:
            cfg.pass_threshold = override.pass_threshold
        return cfg
    return override


def _atomic_write_business_rows(path: Path, rows: list, max_directors: int) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    write_business_rows(tmp, rows, max_directors=max_directors)
    tmp.replace(path)


def run_scoring(
    config: CampaignConfig,
    llm: LLMClient,
    input_path: Path | None = None,
    *,
    registry: Optional[PipelineRegistry] = None,
    only_domains: set[str] | None = None,
    only_missing: bool = False,
    limit: int | None = None,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
) -> tuple[Path, dict]:
    path = input_path or _resolve_scoring_input_path(config, only_missing=only_missing)
    rows = load_rows_as_business(path, max_directors=config.max_directors)

    score_config = _load_score_config(config.scoring_prompt_name, config.score_config)
    config.score_config = score_config

    prompt = get_prompt(config.scoring_prompt_name)
    if prompt is None:
        raise RuntimeError(f"Scoring prompt '{config.scoring_prompt_name}' not found")

    service = DomainScoringService(llm, prompt, score_config)

    # Durable per-domain cache: restore any previously computed results onto the
    # freshly loaded rows so an interrupted run resumes losslessly, regardless of
    # which input file we read from or which flags are set.
    cache = ScoringResultCache(scoring_cache_path(config.campaign_dir))
    for row in rows:
        if not row.has_score_result() and cache.apply_to_row(row):
            apply_analysis_flat(row)

    missing_before = sum(1 for r in rows if not r.has_score_result())

    to_score: list = []
    for row in rows:
        if only_domains is not None and row.domain not in only_domains:
            continue
        # Already scored (loaded from the CSV or restored from cache): never redo.
        if row.has_score_result():
            continue
        if only_missing:
            to_score.append(row)
            continue
        if registry is None or registry.should_process(row.domain, "scored"):
            to_score.append(row)

    if limit is not None and limit > 0:
        to_score = to_score[:limit]

    total = len(to_score)
    if progress_callback and total == 0:
        progress_callback(0, 0, 0.0, "")
    output = stage_path(config.campaign_dir, config.base_name, "scored")
    checkpoint_every = max(1, int(getattr(config, "checkpoint_every_rows", 25) or 25))

    def _checkpoint_domain(row) -> None:
        if registry is None or not row.has_score_result():
            return
        if row.passed_score_filter is False and not config.keep_failed_scores:
            state = registry.domains.get(row.domain)
            if not state or state.stage != "dropped":
                registry.record_drop(row.domain, "failed_score_filter")
            return
        registry.mark(row.domain, "scored")

    def _flush_partial() -> None:
        """Persist everything done so far. Safe to call mid-run / on interruption."""
        _atomic_write_business_rows(output, rows, max_directors=config.max_directors)
        if registry:
            registry.save()

    def _worker(row):
        try:
            service.score_row(row)
            apply_analysis_flat(row)
            if row.has_score_result():
                cache.put(row.domain, score_payload_from_row(row))
        except Exception as exc:
            logger.warning("Scoring failed for %s: %s", row.domain, exc)

    started = time.monotonic()
    completed = 0
    try:
        with ThreadPoolExecutor(max_workers=config.max_workers_http) as pool:
            futures = {pool.submit(_worker, r): r for r in to_score}
            iterator = as_completed(futures)
            if progress_callback is None:
                iterator = tqdm(iterator, total=total, desc="Scoring")
            for fut in iterator:
                row = futures[fut]
                completed += 1
                _checkpoint_domain(row)
                if completed % checkpoint_every == 0 or completed == total:
                    _flush_partial()
                if progress_callback is not None:
                    progress_callback(completed, total, time.monotonic() - started, row.domain)
    except BaseException:
        # Interrupted (e.g. KeyboardInterrupt, laptop sleep killing the run):
        # flush whatever finished so the next run resumes from here.
        _flush_partial()
        raise
    finally:
        if prompt.content_extraction_browser_fallback:
            close_browser_pool()

    output_rows: list = []
    passed = 0
    failed = 0
    for row in rows:
        if row.passed_score_filter is False and not config.keep_failed_scores:
            failed += 1
            if registry:
                state = registry.domains.get(row.domain)
                if not state or state.stage != "dropped":
                    registry.record_drop(row.domain, "failed_score_filter")
            continue
        if row.passed_score_filter:
            passed += 1
        if registry and row.has_score_result():
            registry.mark(row.domain, "scored")
        output_rows.append(row)

    _atomic_write_business_rows(output, output_rows, max_directors=config.max_directors)
    missing_after = sum(1 for r in output_rows if not r.has_score_result())
    stats = {
        "input": len(rows),
        "scored": len(to_score),
        "output": len(output_rows),
        "passed": passed,
        "failed_filter": failed,
        "missing_before": missing_before,
        "missing_after": missing_after,
        "only_missing": only_missing,
        "limit": limit if limit and limit > 0 else None,
        "input_path": str(path),
        "score_scale": score_config.scale,
        "pass_threshold": score_config.pass_threshold,
        **service.stats,
    }
    logger.info("Scoring complete: %s", stats)
    return output, stats
