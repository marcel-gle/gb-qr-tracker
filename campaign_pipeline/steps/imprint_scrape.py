from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Optional

from list_processing.llm.base import LLMClient

from ..config import CampaignConfig
from ..io.readers import load_rows_as_business
from ..io.writers import write_business_rows
from ..naming import cache_path, stage_path
from ..registry import PipelineRegistry
from ..imprint.extract import ImprintExtractor, run_imprint_scrape

logger = logging.getLogger(__name__)


def _atomic_write_business_rows(path: Path, rows: list, max_directors: int) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    write_business_rows(tmp, rows, max_directors=max_directors)
    tmp.replace(path)


_ENRICHMENT_FIELDS = (
    "full_address",
    "street",
    "house_number",
    "postcode",
    "city",
    "email",
    "phone",
    "legal_name",
)


def _overlay_prior_enrichment(rows: list, prior_path: Path, max_directors: int) -> None:
    """Restore imprint enrichment from a previous (partial) output so a resumed
    run never loses already-scraped rows."""
    try:
        prior_rows = load_rows_as_business(prior_path, max_directors=max_directors)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not read prior imprint output %s: %s", prior_path, exc)
        return
    prior_by_domain = {r.domain: r for r in prior_rows}
    for row in rows:
        prev = prior_by_domain.get(row.domain)
        if prev is None:
            continue
        for field in _ENRICHMENT_FIELDS:
            value = getattr(prev, field, None)
            if value and not getattr(row, field, None):
                setattr(row, field, value)
        if prev.directors and any(d for d in prev.directors if any(d.values())):
            if not (row.directors and any(d for d in row.directors if any(d.values()))):
                row.directors = prev.directors


def run_imprint_step(
    config: CampaignConfig,
    llm: LLMClient,
    input_path: Path | None = None,
    *,
    registry: Optional[PipelineRegistry] = None,
    only_domains: set[str] | None = None,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
) -> tuple[Path, dict]:
    path = input_path or stage_path(config.campaign_dir, config.base_name, "scored")
    rows = load_rows_as_business(path, max_directors=config.max_directors)

    if config.pass_score_filter:
        rows = [r for r in rows if r.passed_score_filter is True]

    output = stage_path(config.campaign_dir, config.base_name, "imprint")

    # Resume losslessly: restore enrichment from a previous (partial) imprint run
    # so already-scraped rows keep their data even though they are skipped below.
    if output.exists():
        _overlay_prior_enrichment(rows, output, config.max_directors)

    to_scrape = [
        r
        for r in rows
        if (only_domains is None or r.domain in only_domains)
        and (registry is None or registry.should_process(r.domain, "imprint"))
    ]

    checkpoint_every = max(1, int(getattr(config, "checkpoint_every_rows", 25) or 25))

    cache = cache_path(config.campaign_dir, "imprint")
    extractor = ImprintExtractor(llm, cache_path=cache)

    def _flush() -> None:
        """Persist everything done so far. Safe on interruption."""
        extractor.save_cache()
        _atomic_write_business_rows(output, rows, max_directors=config.max_directors)
        if registry:
            registry.save()

    def _checkpoint(row, completed: int, total: int, elapsed: float) -> None:
        if registry:
            registry.mark(row.domain, "imprint")
        if completed % checkpoint_every != 0 and completed != total:
            return
        _flush()

    try:
        run_imprint_scrape(
            to_scrape,
            extractor,
            max_workers=config.max_workers_http,
            progress_callback=progress_callback,
            checkpoint_callback=_checkpoint,
        )
    except BaseException:
        # Interrupted (KeyboardInterrupt, laptop sleep, ...): flush partial work
        # so the next run resumes from where we left off.
        _flush()
        raise

    for row in to_scrape:
        if registry:
            registry.mark(row.domain, "imprint")

    _flush()
    stats = {"input": len(rows), "scraped": len(to_scrape), "output": len(rows)}
    logger.info("Imprint scrape complete: %s", stats)
    return output, stats
