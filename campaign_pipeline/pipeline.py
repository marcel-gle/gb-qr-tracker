from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, List, Optional

from list_processing.llm.base import LLMClient

from .config import CampaignConfig, ScoreConfig
from .llm import create_llm
from .naming import CAMPAIGN_SUBFOLDERS, incoming_dir, review_decisions_path, review_issues_path, stage_path
from .registry import PipelineRegistry
from .steps.dedupe_address import dedupe_by_address
from .steps.dedupe_domain import dedupe_by_domain
from .steps.final_review import (
    load_review_decisions,
    load_review_issues,
    output_check_stats,
    run_final_review,
    save_review_decisions,
)
from .steps.imprint_scrape import run_imprint_step
from .steps.merge_raw import merge_raw_lists
from .steps.run_scoring import run_scoring
from .steps.template import add_template_column

logger = logging.getLogger(__name__)


class CampaignPipeline:
    def __init__(self, config: CampaignConfig) -> None:
        self.config = config
        self.registry = PipelineRegistry.load(config.campaign_dir)
        self.registry.target_final_count = config.target_final_count
        self._llm: LLMClient | None = None

    @property
    def llm(self) -> LLMClient:
        if self._llm is None:
            self._llm = create_llm(self.config)
        return self._llm

    def ensure_campaign_dirs(self) -> None:
        for sub in CAMPAIGN_SUBFOLDERS:
            (self.config.campaign_dir / sub).mkdir(parents=True, exist_ok=True)
        self.config.pipeline_dir().mkdir(parents=True, exist_ok=True)

    def save_registry(self) -> None:
        self.registry.save()
        self._update_manifest()

    def _update_manifest(self) -> None:
        counts = {}
        for stage in ("raw", "raw_deduped", "scored", "imprint", "final"):
            path = stage_path(self.config.campaign_dir, self.config.base_name, stage)
            if path.exists():
                from .io.readers import load_csv_rows

                rows, _, _ = load_csv_rows(path)
                counts[stage] = len(rows)
            else:
                counts[stage] = 0
        self.registry.update_manifest(
            raw=counts.get("raw", 0),
            scored=counts.get("scored", 0),
            imprint=counts.get("imprint", 0),
            final=counts.get("final", 0),
        )

    def merge_raw(
        self,
        source_files: List[Path],
        *,
        append_only_new: bool = False,
    ) -> dict:
        _, stats = merge_raw_lists(
            self.config,
            source_files,
            append_only_new=append_only_new,
            registry=self.registry,
        )
        self.save_registry()
        return stats

    def dedupe_domain(self) -> dict:
        output_path, stats = dedupe_by_domain(self.config, registry=self.registry)
        stats = {**stats, "output_path": str(output_path)}
        self.save_registry()
        return stats

    def score(
        self,
        *,
        only_new: bool = False,
        only_missing: bool = False,
        limit: int | None = None,
        progress_callback: Callable[[int, int, float, str], None] | None = None,
    ) -> dict:
        only_domains = None
        if only_new and not only_missing:
            only_domains = {d for d, s in self.registry.domains.items() if s.stage == "raw"}
        _, stats = run_scoring(
            self.config,
            self.llm,
            registry=self.registry,
            only_domains=only_domains,
            only_missing=only_missing,
            limit=limit,
            progress_callback=progress_callback,
        )
        self.save_registry()
        return stats

    def imprint(
        self,
        *,
        only_new: bool = False,
        progress_callback: Callable[[int, int, float, str], None] | None = None,
    ) -> dict:
        only_domains = None
        if only_new:
            only_domains = {d for d, s in self.registry.domains.items() if s.stage == "scored"}
        _, stats = run_imprint_step(
            self.config,
            self.llm,
            registry=self.registry,
            only_domains=only_domains,
            progress_callback=progress_callback,
        )
        self.save_registry()
        return stats

    def dedupe_address(self) -> dict:
        _, stats = dedupe_by_address(self.config, registry=self.registry)
        self.save_registry()
        return stats

    def add_template(
        self,
        templates_dir: Path,
        *,
        single_template: str | None = None,
        split_specs: list[tuple[str, int | None]] | None = None,
    ) -> dict:
        input_path = stage_path(self.config.campaign_dir, self.config.base_name, "imprint")
        stats = add_template_column(
            input_path,
            templates_dir,
            input_path,
            single_template=single_template,
            split_specs=split_specs,
        )
        return stats

    def build_clean_list(self, *, drop_missing_address: bool = True) -> dict:
        """Build {base}_cleaned.csv from the imprint file (imprint left untouched)."""
        from .steps.final_review import build_clean_list

        _, stats = build_clean_list(
            self.config,
            drop_missing_address=drop_missing_address,
        )
        return stats

    def run_llm_quality_check(
        self,
        *,
        drop_missing_address: bool = True,
        progress_callback: Callable[[int, int, float], None] | None = None,
    ) -> dict:
        from .io.readers import load_rows_as_business
        from .steps.final_llm_review import run_final_llm_review
        from .steps.final_review import build_clean_list

        # Build the cleaned intermediate list first (normalize + drop empty addresses)
        # so those rows never reach the LLM or the manual review list. The original
        # imprint CSV is left unchanged.
        cleaned_path, clean_stats = build_clean_list(
            self.config,
            drop_missing_address=drop_missing_address,
        )
        rows = load_rows_as_business(cleaned_path, max_directors=self.config.max_directors)

        issues_path = review_issues_path(self.config.campaign_dir, self.config.base_name)
        stats = run_final_llm_review(
            rows,
            self.llm,
            issues_path,
            progress_callback=progress_callback,
        )
        stats["clean"] = clean_stats
        save_review_decisions(review_decisions_path(self.config.campaign_dir, self.config.base_name), {})
        return stats

    def get_review_issues(self) -> list[dict]:
        return load_review_issues(review_issues_path(self.config.campaign_dir, self.config.base_name))

    def get_review_decisions(self) -> dict[int, str]:
        return load_review_decisions(
            review_decisions_path(self.config.campaign_dir, self.config.base_name)
        )

    def save_review_decisions(self, decisions: dict[int, str]) -> None:
        save_review_decisions(
            review_decisions_path(self.config.campaign_dir, self.config.base_name),
            decisions,
        )

    def preview_final_review(
        self,
        *,
        min_score: float | None = None,
        drop_missing_address: bool = False,
        review_decisions: dict[int, str] | None = None,
    ) -> dict:
        from .steps.final_review import preview_final_review

        return preview_final_review(
            self.config,
            min_score=min_score,
            drop_missing_address=drop_missing_address,
            review_decisions=review_decisions,
        )

    def final_review(
        self,
        *,
        min_score: float | None = None,
        drop_missing_address: bool = False,
    ) -> dict:
        from .io.readers import load_rows_as_business

        _, stats = run_final_review(
            self.config,
            registry=self.registry,
            min_score=min_score,
            drop_missing_address=drop_missing_address,
        )
        final_path = stage_path(self.config.campaign_dir, self.config.base_name, "final")
        check = output_check_stats(
            load_rows_as_business(final_path, max_directors=self.config.max_directors)
        )
        stats["output_check"] = check
        self.save_registry()
        return stats

    def continue_pipeline(self, from_step: str = "scoring") -> dict:
        """Run remaining steps for newly added domains only."""
        results = {}
        if from_step in ("scoring", "merge_raw", "dedupe_domain"):
            results["scoring"] = self.score(only_new=True)
        if from_step in ("scoring", "imprint", "merge_raw", "dedupe_domain"):
            results["imprint"] = self.imprint(only_new=True)
        results["dedupe_address"] = self.dedupe_address()
        return results

    def funnel_status(self) -> dict:
        summary = self.registry.funnel_summary()
        summary["paths"] = {
            stage: str(stage_path(self.config.campaign_dir, self.config.base_name, stage))
            for stage in ("raw", "raw_deduped", "scored", "imprint", "final")
        }
        return summary

    def list_incoming_csvs(self) -> List[Path]:
        inc = incoming_dir(self.config.campaign_dir)
        if not inc.exists():
            return []
        return sorted(inc.glob("*.csv"))
