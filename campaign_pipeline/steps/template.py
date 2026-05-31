from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple

from ..io.readers import detect_delimiter, load_csv_rows
from ..io.writers import write_csv_rows

logger = logging.getLogger(__name__)


def add_template_column(
    input_csv: Path,
    templates_dir: Path,
    output_csv: Path,
    *,
    single_template: str | None = None,
    split_specs: List[Tuple[str, int | None]] | None = None,
) -> dict:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {input_csv}")
    if not templates_dir.exists():
        raise FileNotFoundError(f"Templates directory does not exist: {templates_dir}")

    template_files = sorted(p.name for p in templates_dir.glob("*.pdf"))
    if not template_files:
        raise ValueError(f"No PDF templates in {templates_dir}")

    rows, headers, delimiter = load_csv_rows(input_csv)
    template_header = next((h for h in headers if h.lower() == "template"), "Template")
    if template_header not in headers:
        headers = list(headers) + [template_header]

    n_rows = len(rows)
    if split_specs:
        idx = 0
        for tpl_name, count in split_specs:
            if tpl_name not in template_files:
                raise ValueError(f"Template '{tpl_name}' not found. Available: {template_files}")
            end = n_rows if count is None else min(idx + count, n_rows)
            for r in range(idx, end):
                rows[r][template_header] = tpl_name
            idx = end
            if idx >= n_rows:
                break
        for r in range(idx, n_rows):
            if not rows[r].get(template_header):
                rows[r][template_header] = split_specs[-1][0]
    elif single_template:
        if single_template not in template_files:
            raise ValueError(f"Template '{single_template}' not found. Available: {template_files}")
        for row in rows:
            row[template_header] = single_template
    else:
        default = template_files[0]
        for row in rows:
            row.setdefault(template_header, default)

    write_csv_rows(output_csv, rows, fieldnames=headers, delimiter=delimiter)
    return {"rows": n_rows, "template": single_template or "split"}
