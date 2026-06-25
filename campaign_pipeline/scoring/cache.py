from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from ..models import BusinessRow

logger = logging.getLogger(__name__)

# Fields written to / restored from the durable scoring cache. These are exactly
# the attributes that DomainScoringService.score_row sets on a BusinessRow.
_PAYLOAD_FIELDS = (
    "domain_analysis_raw",
    "score_field",
    "score_scale",
    "match_score",
    "score_raw",
    "passed_score_filter",
)


def score_payload_from_row(row: BusinessRow) -> Dict[str, Any]:
    return {field: getattr(row, field) for field in _PAYLOAD_FIELDS}


def apply_score_payload(row: BusinessRow, payload: Dict[str, Any]) -> None:
    for field in _PAYLOAD_FIELDS:
        if field in payload:
            setattr(row, field, payload[field])


class ScoringResultCache:
    """Durable, append-only per-domain cache of scoring results.

    Each completed row is flushed to disk immediately as a single JSON line, so
    an interruption (crash, laptop sleep, Streamlit disconnect) never loses more
    than the rows currently in flight. On resume, cached results are re-applied
    to freshly loaded rows and those domains are skipped instead of re-scored.
    """

    def __init__(self, path: Optional[Path]) -> None:
        self._path = path
        self._lock = Lock()
        self._results: Dict[str, Dict[str, Any]] = {}
        if path is not None:
            self._load()

    def _load(self) -> None:
        if self._path is None or not self._path.exists():
            return
        try:
            with self._path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    domain = entry.get("domain")
                    payload = entry.get("result")
                    if isinstance(domain, str) and isinstance(payload, dict):
                        # Last write wins for a given domain.
                        self._results[domain] = payload
        except OSError as exc:
            logger.warning("Failed to load scoring cache %s: %s", self._path, exc)

    def __len__(self) -> int:
        with self._lock:
            return len(self._results)

    def has(self, domain: str) -> bool:
        with self._lock:
            return domain in self._results

    def get(self, domain: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            payload = self._results.get(domain)
            return dict(payload) if payload is not None else None

    def put(self, domain: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._results[domain] = payload
            if self._path is None:
                return
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"domain": domain, "result": payload}, ensure_ascii=False))
                    f.write("\n")
                    f.flush()
            except OSError as exc:
                logger.warning("Failed to append scoring cache for %s: %s", domain, exc)

    def apply_to_row(self, row: BusinessRow) -> bool:
        """Restore a cached result onto a row. Returns True if applied."""
        payload = self.get(row.domain)
        if payload is None:
            return False
        apply_score_payload(row, payload)
        return True
