from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional

BackendType = Literal["local", "openai"]
ScoreScale = Literal["0-5", "0-10", "binary"]


@dataclass
class ScoreConfig:
    field: str = "match_score"
    scale: ScoreScale = "0-5"
    pass_threshold: float = 4.0

    @classmethod
    def from_prompt_data(cls, data: dict) -> "ScoreConfig":
        raw = data.get("score_config") or {}
        scale = raw.get("scale", "0-5")
        if scale not in ("0-5", "0-10", "binary"):
            scale = "0-5"
        field_name = raw.get("field", "match_score")
        threshold = float(raw.get("pass_threshold", 4.0 if scale == "0-5" else 6.0 if scale == "0-10" else 1.0))
        return cls(field=field_name, scale=scale, pass_threshold=threshold)  # type: ignore[arg-type]


@dataclass
class CampaignConfig:
    campaign_dir: Path
    base_name: str

    backend: BackendType = "local"
    local_model: Optional[str] = None
    max_workers_http: int = 10
    max_workers_llm: int = 5

    scoring_prompt_name: str = "handwerk_analysis"
    score_config: ScoreConfig = field(default_factory=ScoreConfig)
    keep_failed_scores: bool = True

    pass_score_filter: bool = True
    enable_final_llm_review: bool = True
    enable_northdata_fallback: bool = True
    resume: bool = True

    max_directors: int = 3
    target_final_count: Optional[int] = None

    def lists_dir(self) -> Path:
        return self.campaign_dir / "lists"

    def incoming_dir(self) -> Path:
        return self.campaign_dir / "lists" / "incoming"

    def templates_dir(self) -> Path:
        return self.campaign_dir / "templates"

    def pdf_output_dir(self) -> Path:
        return self.campaign_dir / "pdf_output"

    def pipeline_dir(self) -> Path:
        return self.campaign_dir / ".pipeline"
