from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from ..config import CampaignConfig
from ..io.readers import load_rows_as_business
from ..io.writers import write_business_rows
from ..models import BusinessRow, normalize_postcode
from ..naming import stage_path
from ..registry import PipelineRegistry

logger = logging.getLogger(__name__)

_LEGAL_SUFFIX = re.compile(
    r"\b(gmbh|ug|ag|kg|gbr|ohg|kgaa|se|e\.k\.?)\b",
    re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _normalize_company_name(name: Optional[str]) -> str:
    if not name:
        return ""
    s = name.lower()
    s = _LEGAL_SUFFIX.sub(" ", s)
    s = _NON_ALNUM.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def _address_key(row: BusinessRow) -> tuple[str, str, str, str] | None:
    street = (row.street or "").strip().lower()
    house = (row.house_number or "").strip().lower()
    pc = normalize_postcode(row.postcode)
    postcode = (pc or "").strip().lower()
    company = _normalize_company_name(row.company_name or row.legal_name)
    if not street or not house or not postcode or not company:
        return None
    return (company, street, house, postcode)


def dedupe_by_address(
    config: CampaignConfig,
    input_path: Path | None = None,
    *,
    registry: Optional[PipelineRegistry] = None,
) -> tuple[Path, dict]:
    path = input_path or stage_path(config.campaign_dir, config.base_name, "imprint")
    rows = load_rows_as_business(path, max_directors=config.max_directors)

    seen: set[tuple[str, str, str, str]] = set()
    kept: list[BusinessRow] = []
    removed = 0
    missing_key = 0

    for row in rows:
        key = _address_key(row)
        if key is None:
            kept.append(row)
            missing_key += 1
            if registry:
                registry.mark(row.domain, "imprint")
            continue
        if key in seen:
            removed += 1
            if registry:
                registry.record_drop(row.domain, "dropped_address_dup")
            continue
        seen.add(key)
        kept.append(row)
        if registry:
            registry.mark(row.domain, "imprint")

    output = stage_path(config.campaign_dir, config.base_name, "imprint")
    write_business_rows(output, kept, max_directors=config.max_directors)
    stats = {"input": len(rows), "kept": len(kept), "removed": removed, "missing_key": missing_key}
    logger.info("Address dedupe: %s", stats)
    return output, stats
