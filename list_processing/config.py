from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional


BackendType = Literal["local", "openai"]
OutputSchemaType = Literal["internal_enriched", "lettershop"]


@dataclass
class ListProcessingConfig:
    """
    Configuration object for the list_processing pipeline.

    This is intentionally lightweight and can be instantiated directly from
    CLI arguments or a small JSON/YAML config file.
    """

    input_path: Path
    output_path: Path

    # Input mapping
    input_mapper_name: Optional[str] = None
    mapping_file: Optional[Path] = None

    # LLM backend selection
    backend: BackendType = "local"
    # Optional: explicit local model name (overrides LOCAL_MODEL env var)
    local_model: Optional[str] = None

    # Concurrency
    max_workers_http: int = 10
    max_workers_llm: int = 5

    # Prompt configuration (names are resolved via prompt_manager or inline defaults)
    enrichment_prompt_name: str = "imprint_enrichment"
    salutation_prompt_name: str = "salutation_inference"
    scoring_prompt_name: str = "handwerk_analysis"

    # Step toggles
    enable_enrichment: bool = True
    enable_salutation: bool = True
    enable_scoring: bool = True
    enable_final_llm_check: bool = True

    # Caching / restartability
    resume: bool = True
    cache_dir: Optional[Path] = None

    # Output schema
    output_schema: OutputSchemaType = "lettershop"

    def resolve_cache_dir(self) -> Path:
        """
        Determine where to store pipeline checkpoints and caches.

        Defaults to a sibling directory next to the output CSV, e.g.:
        output.csv -> output.list_processing_cache/
        """
        if self.cache_dir is not None:
            return self.cache_dir

        base = self.output_path.with_suffix("")
        return base.parent / f"{base.name}.list_processing_cache"

