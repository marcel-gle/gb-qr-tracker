from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

from ..models import MAX_DIRECTORS_DEFAULT, BusinessRow, final_csv_fieldnames


def write_csv_rows(
    path: Path,
    rows: List[Dict[str, str]],
    fieldnames: List[str] | None = None,
    delimiter: str = ";",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    if fieldnames is None:
        seen: List[str] = []
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.append(key)
        fieldnames = seen

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_business_rows(path: Path, rows: List[BusinessRow], max_directors: int = MAX_DIRECTORS_DEFAULT) -> None:
    dict_rows = [r.to_dict(max_directors=max_directors) for r in rows]
    preferred = [
        "domain",
        "company_name",
        "match_score",
        "score_raw",
        "score_scale",
        "passed_score_filter",
        "salutation_1",
        "first_name_1",
        "last_name_1",
        "full_address",
        "street",
        "house_number",
        "postcode",
        "city",
        "email",
        "phone",
        "legal_name",
        "template",
        "source_file",
        "source_row",
        "gegenstand",
        "branchencode",
    ]
    all_keys: List[str] = []
    for row in dict_rows:
        for key in row.keys():
            if key not in all_keys:
                all_keys.append(key)
    fieldnames = [k for k in preferred if k in all_keys]
    fieldnames.extend(k for k in all_keys if k not in fieldnames)
    write_csv_rows(path, dict_rows, fieldnames=fieldnames)


def write_final_business_rows(path: Path, rows: List[BusinessRow], max_directors: int = MAX_DIRECTORS_DEFAULT) -> None:
    """Write letter-ready final CSV with a fixed column set only."""
    fieldnames = final_csv_fieldnames(max_directors)
    dict_rows = [r.to_final_dict(max_directors=max_directors) for r in rows]
    write_csv_rows(path, dict_rows, fieldnames=fieldnames)
