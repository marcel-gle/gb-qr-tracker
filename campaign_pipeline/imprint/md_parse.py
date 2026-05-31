"""Wrap legacy imprint_md_extract for optional plaintext fallback."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT / "scripts" / "business") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "scripts" / "business"))

try:
    from imprint_md_extract import extract_managing_director_from_imprint_plaintext
except ImportError:  # pragma: no cover

    def extract_managing_director_from_imprint_plaintext(text: str) -> list[str]:  # type: ignore[misc]
        return []


__all__ = ["extract_managing_director_from_imprint_plaintext"]
