from __future__ import annotations

from typing import TYPE_CHECKING

from list_processing.llm.base import LLMClient

from .config import CampaignConfig

if TYPE_CHECKING:
    pass


def create_llm(config: CampaignConfig) -> LLMClient:
    if config.backend == "local":
        from list_processing.llm.local_mlstudio import LocalMLStudioClient

        return LocalMLStudioClient(
            max_concurrent_requests=config.max_workers_llm,
            model=config.local_model,
        )
    if config.backend == "openai":
        from list_processing.llm.openai_client import OpenAIClient

        return OpenAIClient(max_concurrent_requests=config.max_workers_llm)
    raise ValueError(f"Unknown backend: {config.backend}")
