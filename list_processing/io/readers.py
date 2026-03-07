from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Iterable, List, Sequence

from ..models import LeadRecord
from .mappers import InputMapper, guess_mapper_from_header, get_builtin_mapper

logger = logging.getLogger(__name__)


def _detect_delimiter(path: Path) -> str:
    """Detect CSV delimiter by sampling the first chunk of the file."""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
            return dialect.delimiter
        except Exception:
            # Fallback: pick the most frequent candidate, default to comma.
            counts = {d: sample.count(d) for d in [",", ";", "\t", "|"]}
            return max(counts, key=counts.get) if max(counts.values()) > 0 else ","


def load_lead_records_from_csv(
    path: Path,
    mapper: InputMapper | None = None,
) -> List[LeadRecord]:
    """
    Load a list of LeadRecord instances from a CSV file.

    If no mapper is provided, a heuristic mapper is derived from the header
    and then refined with the built-in default mapping.
    """
    delimiter = _detect_delimiter(path)
    logger.info("Loading CSV %s with delimiter '%s'", path, delimiter)

    records: List[LeadRecord] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        header: Sequence[str] = list(reader.fieldnames or [])

        if mapper is None:
            guessed = guess_mapper_from_header(header)
            # Merge guessed mapping with built-in default to cover both
            # heuristic and explicit mappings.
            base = get_builtin_mapper("default")
            combined_map = dict(base.source_to_canonical)
            combined_map.update(guessed.source_to_canonical)
            mapper = InputMapper(combined_map)

        for idx, row in enumerate(reader, start=1):
            try:
                canonical = mapper.map_row(row)
                record = LeadRecord.from_canonical_dict(canonical)
                records.append(record)
            except ValueError as exc:
                logger.warning(
                    "Skipping row %s in %s due to missing required fields: %s",
                    idx,
                    path,
                    exc,
                )

    logger.info("Loaded %d LeadRecord objects from %s", len(records), path)
    return records

