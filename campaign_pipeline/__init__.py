"""Campaign list processing pipeline with staged CSV outputs."""

from .config import CampaignConfig, ScoreConfig
from .models import BusinessRow

__all__ = ["CampaignConfig", "ScoreConfig", "BusinessRow", "CampaignPipeline"]


def __getattr__(name: str):
    if name == "CampaignPipeline":
        from .pipeline import CampaignPipeline

        return CampaignPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
