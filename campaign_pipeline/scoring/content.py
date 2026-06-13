from __future__ import annotations

from dataclasses import dataclass

MAX_VISIBLE_TEXT_CHARS = 20_000
MAX_TECHNICAL_SIGNALS_CHARS = 3_000


@dataclass
class ScoringPageContent:
    visible_text: str
    technical_signals: str | None = None
    fetch_source: str = "http"

    def for_llm(self, mode: str) -> str:
        visible = (self.visible_text or "")[:MAX_VISIBLE_TEXT_CHARS]
        if mode != "text_and_technical" or not self.technical_signals:
            return visible
        tech = self.technical_signals[:MAX_TECHNICAL_SIGNALS_CHARS]
        return (
            "=== SICHTBARER WEBSEITEN-TEXT ===\n"
            f"{visible}\n\n"
            "=== TECHNISCHE SIGNALE (aus HTML-Quellcode) ===\n"
            f"{tech}"
        )
