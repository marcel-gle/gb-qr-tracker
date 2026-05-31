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

    to_scrape = [
        r
        for r in rows
        if (only_domains is None or r.domain in only_domains)
        and (registry is None or registry.should_process(r.domain, "imprint"))
    ]

    cache = cache_path(config.campaign_dir, "imprint")
    extractor = ImprintExtractor(llm, cache_path=cache)
    run_imprint_scrape(
        to_scrape,
        extractor,
        max_workers=config.max_workers_http,
        progress_callback=progress_callback,
    )

    for row in to_scrape:
        if registry:
            registry.mark(row.domain, "imprint")

    output = stage_path(config.campaign_dir, config.base_name, "imprint")
    write_business_rows(output, rows, max_directors=config.max_directors)
    stats = {"input": len(rows), "scraped": len(to_scrape), "output": len(rows)}
    logger.info("Imprint scrape complete: %s", stats)
    return output, stats
