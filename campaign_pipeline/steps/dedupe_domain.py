from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

from ..config import CampaignConfig
from ..io.readers import load_rows_as_business
from ..io.writers import write_business_rows
from ..models import BusinessRow
from ..naming import stage_path
from ..registry import PipelineRegistry

logger = logging.getLogger(__name__)


def _atomic_write_business_rows(path: Path, rows: list[BusinessRow], *, max_directors: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(suffix=".csv", dir=path.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        write_business_rows(tmp_path, rows, max_directors=max_directors)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    if not path.is_file():
        raise RuntimeError(f"Dedupe output was not written: {path}")


def dedupe_by_domain(
    config: CampaignConfig,
    input_path: Path | None = None,
    *,
    registry: Optional[PipelineRegistry] = None,
) -> tuple[Path, dict]:
    raw_path = stage_path(config.campaign_dir, config.base_name, "raw").resolve()
    deduped_path = stage_path(config.campaign_dir, config.base_name, "raw_deduped").resolve()
    if input_path is not None:
        path = input_path
    elif raw_path.exists():
        path = raw_path
    elif deduped_path.exists():
        path = deduped_path
    else:
        raise FileNotFoundError(
            f"No raw list to dedupe. Expected `{raw_path.name}` in `{raw_path.parent}`."
        )

    rows = load_rows_as_business(path, max_directors=config.max_directors)

    seen: set[str] = set()
    kept: list[BusinessRow] = []
    removed = 0

    for row in rows:
        if row.domain in seen:
            removed += 1
            if registry:
                registry.record_drop(row.domain, "dropped_domain_dup")
            continue
        seen.add(row.domain)
        kept.append(row)
        if registry:
            registry.mark(row.domain, "raw")

    _atomic_write_business_rows(deduped_path, kept, max_directors=config.max_directors)

    if raw_path.is_file() and raw_path != deduped_path:
        raw_path.unlink()

    if not deduped_path.is_file():
        raise RuntimeError(f"Dedupe output missing after write: {deduped_path}")

    stats = {
        "input": len(rows),
        "kept": len(kept),
        "removed": removed,
        "input_path": str(path.resolve()),
        "output_path": str(deduped_path),
    }
    logger.info("Domain dedupe: %s", stats)
    return deduped_path, stats
