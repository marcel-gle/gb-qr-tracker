from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Dict, List, Tuple

from ..models import MAX_DIRECTORS_DEFAULT, BusinessRow

logger = logging.getLogger(__name__)

CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


def detect_delimiter(sample: str) -> str:
    for line in sample.splitlines():
        if line.strip():
            if line.count(";") >= line.count(","):
                return ";"
            return ","
    return ";"


def _read_text(path: Path) -> str:
    last_error: Exception | None = None
    for enc in CSV_ENCODINGS:
        try:
            return path.read_text(encoding=enc)
        except (UnicodeDecodeError, LookupError) as exc:
            last_error = exc
    raise ValueError(f"Could not decode {path}: {last_error}")


def load_csv_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str], str]:
    content = _read_text(path)
    delim = detect_delimiter(content)
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim, restkey="_extra", restval="")
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            row.pop("_extra", None)
            clean = {(k if k else ""): (v or "") for k, v in row.items()}
            rows.append(clean)
    return rows, fieldnames, delim


def load_rows_as_business(path: Path, max_directors: int = MAX_DIRECTORS_DEFAULT) -> List[BusinessRow]:
    rows, _, _ = load_csv_rows(path)
    out: List[BusinessRow] = []
    for idx, row in enumerate(rows, start=1):
        try:
            out.append(BusinessRow.from_dict(row, max_directors=max_directors))
        except ValueError as exc:
            logger.warning("Skipping row %d in %s: %s", idx, path, exc)
    return out
