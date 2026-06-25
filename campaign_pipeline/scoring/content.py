from __future__ import annotations

from dataclasses import dataclass

from .extract import TechnicalSignals


@dataclass
class ScoringPageContent:
    visible_text: str
    raw_html: str = ""
    response_headers: dict[str, str] | None = None
    final_url: str = ""
    fetch_url: str = ""
    technical_signals: TechnicalSignals | None = None
    fetch_source: str = "http"

    def for_llm(self, mode: str) -> str:
        """Return visible text only — never raw HTML or technical signal prose."""
        _ = mode
        return self.visible_text or ""
