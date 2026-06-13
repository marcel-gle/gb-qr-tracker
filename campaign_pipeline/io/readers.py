from __future__ import annotations

import csv
import io
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


def _decode_bytes(raw: bytes) -> tuple[str, str]:
    if raw.startswith(b"\xff\xfe") and not raw.startswith(b"\xff\xfe\x00\x00"):
        return raw.decode("utf-16-le"), "utf-16-le"
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be"), "utf-16-be"
    last_error: UnicodeDecodeError | None = None
    for enc in CSV_ENCODINGS:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise ValueError(f"Could not decode CSV bytes: {last_error}") from last_error
    return "", "utf-8"


def _read_text(path: Path) -> str:
    text, _ = _decode_bytes(path.read_bytes())
    return text


def load_csv_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str], str]:
    raw = path.read_bytes()
    content, encoding = _decode_bytes(raw)
    delim = detect_delimiter(content)
    rows: List[Dict[str, str]] = []
    with io.TextIOWrapper(io.BytesIO(raw), encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delim, restkey="_extra", restval="")
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            row.pop("_extra", None)
            clean = {(k if k else ""): (v or "") for k, v in row.items()}
            rows.append(clean)
    logger.debug("Loaded %d rows from %s (%s)", len(rows), path.name, encoding)
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
