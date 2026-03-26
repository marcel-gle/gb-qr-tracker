"""
Add or overwrite the Template column in a CSV so every row has a PDF filename
from the templates directory. Used before Firestore upload (local_process_upload).

Usage:
  # Same template for all rows:
  python scripts/list_processing/add_template_column.py \\
    --input-csv path/to/filtered.csv --templates-dir path/to/templates \\
    --output-csv path/to/output.csv --template template-A.pdf

  # Split by row counts (first 100 get A, next 200 get B, rest get C):
  python scripts/list_processing/add_template_column.py \\
    --input-csv path/to/filtered.csv --templates-dir path/to/templates \\
    --output-csv path/to/output.csv \\
    --split "template-A.pdf:100" "template-B.pdf:200" "template-C.pdf"
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Dict, Tuple


def _detect_delimiter(sample: str) -> str:
    if not sample:
        return ";"
    first_line = ""
    for line in sample.splitlines():
        if line.strip():
            first_line = line
            break
    if not first_line:
        return ";"
    best_delimiter = ";"
    best_count = 1
    for delim in (";", "\t", ",", "|"):
        count = len(first_line.split(delim))
        if count > best_count:
            best_count = count
            best_delimiter = delim
    return best_delimiter


def _read_rows(path: Path) -> tuple[List[Dict[str, str]], List[str], str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = _detect_delimiter(sample)
        reader = csv.DictReader(f, delimiter=delimiter, restkey="_extra", restval="")
        rows: List[Dict[str, str]] = []
        for r in reader:
            r = {(k if isinstance(k, str) else str(k)): (v or "") for k, v in r.items()}
            r.pop("_extra", None)
            rows.append(r)
        headers = list(reader.fieldnames or [])
    return rows, headers, delimiter


def _write_rows(path: Path, rows: List[Dict[str, str]], fieldnames: List[str], delimiter: str) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Add or set Template column in a CSV from a templates directory."
    )
    parser.add_argument("--input-csv", required=True, type=Path, help="Path to input CSV.")
    parser.add_argument("--templates-dir", required=True, type=Path, help="Directory containing PDF templates.")
    parser.add_argument("--output-csv", required=True, type=Path, help="Path to output CSV.")
    parser.add_argument(
        "--template",
        default=None,
        help="Use this template filename for all rows (must be a PDF in templates-dir). Ignored if --split is used.",
    )
    parser.add_argument(
        "--split",
        action="append",
        default=None,
        metavar="TEMPLATE:COUNT",
        help=(
            "Assign templates by row count. E.g. 'A.pdf:100' 'B.pdf:200' 'C.pdf' (no count = rest). "
            "Can be passed multiple times. Overrides --template."
        ),
    )
    args = parser.parse_args()

    input_path = args.input_csv
    templates_dir = args.templates_dir
    output_path = args.output_csv

    if not input_path.exists():
        print(f"❌ Input CSV does not exist: {input_path}")
        return 1
    if not templates_dir.exists() or not templates_dir.is_dir():
        print(f"❌ Templates directory does not exist or is not a directory: {templates_dir}")
        return 1

    template_files = sorted(p.name for p in templates_dir.glob("*.pdf"))
    if not template_files:
        print(f"❌ No PDF files found in {templates_dir}")
        return 1

    rows, headers, delimiter = _read_rows(input_path)
    n_rows = len(rows)

    template_header = "Template"
    if template_header not in headers:
        headers = list(headers) + [template_header]

    split_specs: List[Tuple[str, int | None]] = []  # (template_name, count or None for rest)
    if args.split:
        for s in args.split:
            s = (s or "").strip()
            if ":" in s:
                name, count_str = s.split(":", 1)
                name, count_str = name.strip(), count_str.strip()
                count = int(count_str) if count_str else None
            else:
                name, count = s.strip(), None
            if not name:
                continue
            if name not in template_files:
                print(f"❌ Template '{name}' not found. Available: {template_files}")
                return 1
            split_specs.append((name, count))
    single_template = (args.template or "").strip() if not split_specs else None
    if single_template and single_template not in template_files:
        print(f"❌ Template '{single_template}' not found in {templates_dir}. Available: {template_files}")
        return 1

    if split_specs:
        idx = 0
        for tpl_name, count in split_specs:
            if count is None:
                end = n_rows
            else:
                end = min(idx + count, n_rows)
            for r in range(idx, end):
                rows[r][template_header] = tpl_name
            idx = end
            if idx >= n_rows:
                break
        for r in range(idx, n_rows):
            if not rows[r].get(template_header):
                rows[r][template_header] = split_specs[-1][0] if split_specs else template_files[0]
    else:
        for row in rows:
            if single_template:
                row[template_header] = single_template
            else:
                row.setdefault(template_header, template_files[0] if template_files else "")

    _write_rows(output_path, rows, headers, delimiter)
    print(f"Written {len(rows)} rows to {output_path}")
    if single_template:
        print(f"Template column set to '{single_template}' for all rows.")
    elif split_specs:
        for tpl_name, count in split_specs:
            c = f"{count}" if count is not None else "rest"
            print(f"  {tpl_name}: {c} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
