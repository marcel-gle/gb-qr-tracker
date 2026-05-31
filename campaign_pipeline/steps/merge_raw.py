from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional, Set

from ..config import CampaignConfig
from ..io.mappers import map_row_to_canonical
from ..io.readers import load_csv_rows
from ..io.writers import write_csv_rows
from ..models import BusinessRow, normalize_domain
from ..naming import stage_path
from ..registry import PipelineRegistry

logger = logging.getLogger(__name__)


def merge_raw_lists(
    config: CampaignConfig,
    source_files: List[Path],
    *,
    append_only_new: bool = False,
    registry: Optional[PipelineRegistry] = None,
) -> tuple[Path, dict]:
    """
    Merge CSV source files into {base}_raw.csv.
    Returns (output_path, stats).
    """
    output = stage_path(config.campaign_dir, config.base_name, "raw")
    existing_domains: Set[str] = set()
    merged_rows: List[dict] = []
    header_order: List[str] = []

    if append_only_new and output.exists():
        existing_rows, existing_headers, _ = load_csv_rows(output)
        header_order = list(existing_headers)
        for row in existing_rows:
            domain = normalize_domain(row.get("domain") or row.get("Domain"))
            if domain:
                existing_domains.add(domain)
                merged_rows.append(row)

    files_merged = 0
    rows_added = 0
    rows_skipped_no_domain = 0
    rows_skipped_known = 0

    for fp in source_files:
        if not fp.exists():
            logger.warning("Source file not found: %s", fp)
            continue
        raw_rows, _, _ = load_csv_rows(fp)
        files_merged += 1
        for row_idx, raw in enumerate(raw_rows, start=1):
            canonical = map_row_to_canonical(raw)
            domain = canonical.get("domain")
            if not domain:
                rows_skipped_no_domain += 1
                continue
            domain = str(domain)
            if append_only_new and domain in existing_domains:
                rows_skipped_known += 1
                continue
            if append_only_new and registry and not registry.is_new_domain(domain):
                rows_skipped_known += 1
                continue

            row_out = dict(raw)
            row_out["domain"] = domain
            if canonical.get("company_name"):
                row_out["company_name"] = canonical["company_name"]
            row_out["source_file"] = fp.name
            row_out["source_row"] = str(row_idx)
            for key, val in canonical.items():
                if key not in row_out and val:
                    row_out[key] = val

            merged_rows.append(row_out)
            existing_domains.add(domain)
            rows_added += 1
            if registry:
                registry.mark(domain, "raw")

            for key in row_out.keys():
                if key not in header_order:
                    header_order.append(key)

    if "domain" not in header_order:
        header_order.insert(0, "domain")
    else:
        header_order = ["domain"] + [k for k in header_order if k != "domain"]

    write_csv_rows(output, merged_rows, fieldnames=header_order)
    stats = {
        "files_merged": files_merged,
        "rows_total": len(merged_rows),
        "rows_added": rows_added,
        "rows_skipped_no_domain": rows_skipped_no_domain,
        "rows_skipped_known": rows_skipped_known,
    }
    logger.info("Merged raw list: %s", stats)
    return output, stats
