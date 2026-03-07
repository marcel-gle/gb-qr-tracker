from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Set


REQUIRED_FIELDS: List[str] = [
    "first_name",
    "last_name",
    "salutation",
    "street",
    "house_number",
    "postcode",
    "city",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter a list_processing output CSV by score, required fields, and "
            "rows flagged in the final_review_issues.csv."
        )
    )
    parser.add_argument(
        "input_csv",
        help="Path to the pipeline output CSV (internal_enriched or similar).",
    )
    parser.add_argument(
        "--output-csv",
        help=(
            "Path to write the filtered CSV. "
            "Defaults to '<input_stem>__filtered.csv' next to the input."
        ),
    )
    parser.add_argument(
        "--issues-csv",
        help=(
            "Path to the final_review_issues CSV. "
            "Defaults to '<input_stem>.final_review_issues.csv' if present."
        ),
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=4.0,
        help="Minimum required match_score to keep a row (default: 4.0).",
    )
    parser.add_argument(
        "--branchencode",
        action="append",
        dest="wanted_branchencodes",
        help=(
            "Branchencode prefix to keep; can be passed multiple times. "
            "For example, '--branchencode 61 --branchencode 62' keeps rows whose "
            "branchencode starts with '61' or '62' (e.g. '61.01', '62.01'). "
            "If omitted, no branchencode filtering is applied."
        ),
    )
    return parser.parse_args()


def load_flagged_indices(issues_path: Path) -> Set[int]:
    if not issues_path.exists():
        return set()

    flagged: Set[int] = set()
    with issues_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_idx = (row.get("row_index") or "").strip()
            if not raw_idx:
                continue
            try:
                idx = int(raw_idx)
            except ValueError:
                continue
            flagged.add(idx)
    return flagged


def has_required_fields(row: Dict[str, str]) -> bool:
    for field in REQUIRED_FIELDS:
        value = (row.get(field) or "").strip()
        if not value:
            return False
    return True


def parse_score(row: Dict[str, str]) -> float | None:
    # Prefer match_score, fall back to domain_match_score if present.
    for key in ("match_score", "domain_match_score"):
        raw = row.get(key)
        if raw is None:
            continue
        raw = raw.strip()
        if not raw:
            continue
        try:
            return float(raw.replace(",", "."))
        except ValueError:
            continue
    return None


def matches_branchencode(row: Dict[str, str], wanted_prefixes: List[str] | None) -> bool:
    """
    Return True if the row passes the branchencode filter.

    - If no wanted_prefixes are provided, always return True.
    - Otherwise, require that the 'branchencode' field starts with one of the
      given prefixes (after basic normalization).
    """
    if not wanted_prefixes:
        return True

    raw = (row.get("branchencode") or "").strip()
    if not raw:
        return False

    # Try to extract the leading code part before any non-digit/dot characters.
    m = re.match(r"^[0-9.]+", raw)
    code = m.group(0) if m else raw
    code = code.strip()
    if not code:
        return False

    for prefix in wanted_prefixes:
        p = (prefix or "").strip()
        if not p:
            continue
        if code.startswith(p):
            return True
    return False


def main() -> int:
    args = parse_args()

    input_path = Path(args.input_csv)
    if not input_path.exists():
        print(f"❌ Input CSV does not exist: {input_path}")
        return 1

    output_path = Path(args.output_csv) if args.output_csv else input_path.with_name(
        f"{input_path.stem}__filtered.csv"
    )

    if args.issues_csv:
        issues_path = Path(args.issues_csv)
    else:
        issues_path = input_path.with_name(f"{input_path.stem}.final_review_issues.csv")

    flagged_indices = load_flagged_indices(issues_path)

    # Normalize branchencode prefixes (if provided).
    wanted_branchencodes: List[str] | None = None
    if args.wanted_branchencodes:
        wanted_branchencodes = [s.strip() for s in args.wanted_branchencodes if s and s.strip()]

    total_rows = 0
    kept_rows = 0
    removed_by_score = 0
    removed_by_required = 0
    removed_by_issues = 0
    removed_by_branchencode = 0

    with input_path.open("r", encoding="utf-8-sig", newline="") as f_in, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as f_out:
        reader = csv.DictReader(f_in, delimiter=";")
        fieldnames = reader.fieldnames or []
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()

        for row_index, row in enumerate(reader, start=1):
            total_rows += 1

            score = parse_score(row)
            if score is None or score < args.min_score:
                removed_by_score += 1
                continue

            if not has_required_fields(row):
                removed_by_required += 1
                continue

            if row_index in flagged_indices:
                removed_by_issues += 1
                continue

            if not matches_branchencode(row, wanted_branchencodes):
                removed_by_branchencode += 1
                continue

            writer.writerow(row)
            kept_rows += 1

    removed_rows = total_rows - kept_rows
    print(f"Total input rows:                        {total_rows}")
    print(f"Removed (score cutoff):                  {removed_by_score}")
    print(f"Removed (missing required fields):       {removed_by_required}")
    print(f"Removed (final_review_issues flagged):   {removed_by_issues}")
    print(f"Removed (branchencode filter):           {removed_by_branchencode}")
    print(f"Removed rows (total filtered):           {removed_rows}")
    print(f"Fully compliant rows:                    {kept_rows}")
    print(f"Filtered CSV written to:                 {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

