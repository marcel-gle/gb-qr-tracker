from __future__ import annotations

import json
import logging
import pickle
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from scripts.business.prompt_manager import get_prompt

from .config import ListProcessingConfig
from .io import (
    InputMapper,
    get_builtin_mapper,
    guess_mapper_from_header,
    load_lead_records_from_csv,
    write_internal_csv,
    write_lettershop_csv,
)
from .llm.base import LLMClient
from .llm.local_mlstudio import LocalMLStudioClient
from .llm.openai_client import OpenAIClient
from .logging_config import log_step_banner
from .models import LeadRecord
from .steps.enrichment import EnrichmentService, run_enrichment
from .steps.final_evaluation import run_final_llm_evaluation
from .steps.output_check import run_output_checks
from .steps.salutation import SalutationService, apply_salutation
from .steps.scoring import DomainScoringService, score_records
from .steps.address_cleanup import cleanup_addresses

logger = logging.getLogger(__name__)


class ListProcessingPipeline:
    """
    High-level orchestration for the new list processing flow.

    This class is deliberately minimal and focuses on sequencing, logging,
    and checkpointing. Heavy lifting is delegated to the step-specific
    service classes.
    """

    def __init__(self, config: ListProcessingConfig, llm: LLMClient) -> None:
        self.config = config
        self.llm = llm

    # ------------------------------------------------------------------
    # Checkpoint helpers
    # ------------------------------------------------------------------

    def _checkpoint_paths(self) -> Tuple[Path, Path, Path, Path]:
        cache_dir = self.config.resolve_cache_dir()
        return (
            cache_dir / "records.normalized.pkl",
            cache_dir / "records.enriched.pkl",
            cache_dir / "records.salutation.pkl",
            cache_dir / "records.scored.pkl",
        )

    def _load_latest_checkpoint(self) -> Tuple[str, List[LeadRecord]]:
        """
        Load the most advanced checkpoint if resume is enabled.

        Returns (stage_name, records). stage_name is one of:
        - \"none\"
        - \"normalized\"
        - \"enriched\"
        - \"salutation\"
        - \"scored\"
        """
        if not self.config.resume:
            return "none", []

        normalized_path, enriched_path, salutation_path, scored_path = self._checkpoint_paths()

        for stage_name, path in [
            ("scored", scored_path),
            ("salutation", salutation_path),
            ("enriched", enriched_path),
            ("normalized", normalized_path),
        ]:
            if path.exists():
                try:
                    with path.open("rb") as f:
                        records = pickle.load(f)
                    logger.info("Resuming pipeline from %s checkpoint at %s", stage_name, path)
                    return stage_name, records
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("Failed to load checkpoint %s: %s", path, exc)
                    break

        return "none", []

    def _save_checkpoint(self, stage_name: str, records: List[LeadRecord]) -> None:
        normalized_path, enriched_path, salutation_path, scored_path = self._checkpoint_paths()
        path_lookup = {
            "normalized": normalized_path,
            "enriched": enriched_path,
            "salutation": salutation_path,
            "scored": scored_path,
        }
        path = path_lookup.get(stage_name)
        if not path:
            return

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("wb") as f:
                pickle.dump(records, f)
            logger.info("Saved %s checkpoint to %s", stage_name, path)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to save %s checkpoint to %s: %s", stage_name, path, exc)

    # ------------------------------------------------------------------
    # LLM factory
    # ------------------------------------------------------------------

    @staticmethod
    def create_llm_from_config(config: ListProcessingConfig) -> LLMClient:
        if config.backend == "local":
            return LocalMLStudioClient(
                max_concurrent_requests=config.max_workers_llm,
                model=config.local_model,
            )
        if config.backend == "openai":
            return OpenAIClient(max_concurrent_requests=config.max_workers_llm)
        raise ValueError(f"Unknown LLM backend: {config.backend}")

    # ------------------------------------------------------------------
    # Main entrypoint
    # ------------------------------------------------------------------

    def run(self) -> None:
        overall_start = time.monotonic()
        step_timings: dict[str, float] = {}
        step_status: dict[str, str] = {}
        address_cleanup_stats: dict[str, int] | None = None
        final_llm_review_stats: dict[str, object] | None = None

        cache_dir = self.config.resolve_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Load checkpoint or start from scratch
        stage, records = self._load_latest_checkpoint()

        # 1) Normalization / input loading
        if stage == "none":
            log_step_banner("STEP 1: Input & Normalization")
            t0 = time.monotonic()
            records = load_lead_records_from_csv(self.config.input_path)
            logger.info("Loaded %d records from input CSV", len(records))
            self._save_checkpoint("normalized", records)
            step_timings["step1_input"] = time.monotonic() - t0
            step_status["step1_input"] = "completed"
            stage = "normalized"
        else:
            step_status["step1_input"] = f"skipped (resumed from stage={stage})"

        # 2) Enrichment
        if self.config.enable_enrichment and stage in {"none", "normalized"}:
            log_step_banner("STEP 2: Enrichment (Imprint)")
            t0 = time.monotonic()
            _, enriched_path, _, _ = self._checkpoint_paths()
            enrichment_cache_path = cache_dir / "enrichment_domain_cache.pkl"
            enrichment_service = EnrichmentService(
                self.llm,
                cache_path=enrichment_cache_path,
                prompt_name=self.config.enrichment_prompt_name,
            )
            run_enrichment(
                records,
                enrichment_service,
                max_workers_http=self.config.max_workers_http,
            )
            self._save_checkpoint("enriched", records)
            step_timings["step2_enrichment"] = time.monotonic() - t0
            step_status["step2_enrichment"] = "completed"
            stage = "enriched"
        else:
            reason = []
            if not self.config.enable_enrichment:
                reason.append("disabled")
            if stage not in {"none", "normalized"}:
                reason.append(f"resumed from stage={stage}")
            step_status["step2_enrichment"] = "skipped (" + ", ".join(reason) + ")"

        # 3) Salutation
        if self.config.enable_salutation and stage in {"none", "normalized", "enriched"}:
            log_step_banner("STEP 3: Salutation Inference")
            t0 = time.monotonic()
            sal_service = SalutationService(
                self.llm,
                prompt_name=self.config.salutation_prompt_name,
            )
            apply_salutation(records, sal_service)
            self._save_checkpoint("salutation", records)
            step_timings["step3_salutation"] = time.monotonic() - t0
            step_status["step3_salutation"] = "completed"
            stage = "salutation"
        else:
            reason = []
            if not self.config.enable_salutation:
                reason.append("disabled")
            if stage not in {"none", "normalized", "enriched"}:
                reason.append(f"resumed from stage={stage}")
            step_status["step3_salutation"] = "skipped (" + ", ".join(reason) + ")"

        # 3b) Address & duplicate cleanup
        if stage in {"normalized", "enriched", "salutation"}:
            log_step_banner("STEP 3b: Address & Duplicate Cleanup")
            t0 = time.monotonic()
            address_cleanup_stats = cleanup_addresses(records)

            step_timings["step3b_address_cleanup"] = time.monotonic() - t0
            step_status["step3b_address_cleanup"] = "completed"
        else:
            step_status["step3b_address_cleanup"] = f"skipped (stage={stage})"

        # 4) Domain scoring
        if self.config.enable_scoring and stage in {
            "none",
            "normalized",
            "enriched",
            "salutation",
        }:
            log_step_banner("STEP 4: Domain Scoring")
            t0 = time.monotonic()
            prompt = get_prompt(self.config.scoring_prompt_name)
            if prompt is None:
                raise RuntimeError(
                    f"Scoring prompt '{self.config.scoring_prompt_name}' not found. "
                    "Use scripts/business/prompts.json to configure prompts."
                )

            scoring_service = DomainScoringService(self.llm, prompt)
            score_records(
                records,
                scoring_service,
                max_workers_http=self.config.max_workers_http,
            )
            self._save_checkpoint("scored", records)
            step_timings["step4_scoring"] = time.monotonic() - t0
            step_status["step4_scoring"] = "completed"
            stage = "scored"
        else:
            reason = []
            if not self.config.enable_scoring:
                reason.append("disabled")
            if stage not in {"none", "normalized", "enriched", "salutation"}:
                reason.append(f"resumed from stage={stage}")
            step_status["step4_scoring"] = "skipped (" + ", ".join(reason) + ")"

        # 5) Output
        log_step_banner("STEP 5: Output")
        t0 = time.monotonic()
        if self.config.output_schema == "internal_enriched":
            write_internal_csv(self.config.output_path, records)
        elif self.config.output_schema == "lettershop":
            write_lettershop_csv(self.config.output_path, records)
        else:
            raise ValueError(f"Unknown output schema: {self.config.output_schema}")
        step_timings["step5_output"] = time.monotonic() - t0
        step_status["step5_output"] = "completed"

        logger.info("Pipeline complete. Wrote output to %s", self.config.output_path)

        # 6) Final data check
        log_step_banner("STEP 6: Output Data Check")
        t0 = time.monotonic()
        run_output_checks(records, self.config.output_path)
        step_timings["step6_output_check"] = time.monotonic() - t0
        step_status["step6_output_check"] = "completed"

        # 7) LLM-based final sanity check over the entire list
        if self.config.enable_final_llm_check:
            log_step_banner("STEP 7: LLM Final Data Review")
            t0 = time.monotonic()
            final_llm_review_stats = run_final_llm_evaluation(records, self.llm, self.config.output_path)
            step_timings["step7_llm_review"] = time.monotonic() - t0
            step_status["step7_llm_review"] = "completed"
        else:
            step_status["step7_llm_review"] = "skipped (disabled)"

        total_runtime = time.monotonic() - overall_start
        logger.info("Pipeline timing summary (seconds):")
        summary_order = [
            ("step1_input", "STEP 1: Input & Normalization"),
            ("step2_enrichment", "STEP 2: Enrichment (Imprint)"),
            ("step3_salutation", "STEP 3: Salutation Inference"),
            ("step3b_address_cleanup", "STEP 3b: Address & Duplicate Cleanup"),
            ("step4_scoring", "STEP 4: Domain Scoring"),
            ("step5_output", "STEP 5: Output"),
            ("step6_output_check", "STEP 6: Output Data Check"),
            ("step7_llm_review", "STEP 7: LLM Final Data Review"),
        ]
        for key, label in summary_order:
            status = step_status.get(key, "skipped")
            if status == "completed":
                duration = step_timings.get(key, 0.0)
                logger.info("  %s: %.2fs", label, duration)
            else:
                logger.info("  %s: %s", label, status)
        logger.info("  Total pipeline runtime: %.2fs", total_runtime)

        # Optional: aggregate token usage statistics from the LLM client.
        try:
            total_tokens = getattr(self.llm, "total_tokens", 0)
            prompt_tokens = getattr(self.llm, "total_prompt_tokens", 0)
            completion_tokens = getattr(self.llm, "total_completion_tokens", 0)
            total_calls = getattr(self.llm, "total_calls", 0)

            num_rows = len(records)
            avg_tokens_per_row = (total_tokens / num_rows) if num_rows else 0.0
            avg_tokens_per_call = (total_tokens / total_calls) if total_calls else 0.0

            # Overall throughput: seconds per 1000 tokens across the whole run.
            seconds_per_1k_tokens = (
                (total_runtime * 1000.0 / total_tokens) if total_tokens > 0 else 0.0
            )

            logger.info("Pipeline LLM usage summary:")
            logger.info("  Total LLM calls: %d", total_calls)
            logger.info(
                "  Total tokens: %d (prompt=%d, completion=%d)",
                total_tokens,
                prompt_tokens,
                completion_tokens,
            )
            logger.info("  Average tokens per LLM call: %.2f", avg_tokens_per_call)
            logger.info("  Average tokens per row: %.2f (based on %d rows)", avg_tokens_per_row, num_rows)
            logger.info("  Approx. seconds per 1000 tokens: %.3f", seconds_per_1k_tokens)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Failed to compute LLM usage summary: %s", exc)

        # Persist a structured JSON summary of the run for downstream analysis.
        try:
            summary_path = self.config.output_path.with_suffix("").with_name(
                f"{self.config.output_path.stem}.run_summary.json"
            )

            # Recompute LLM usage with graceful fallbacks.
            total_tokens = getattr(self.llm, "total_tokens", 0)
            prompt_tokens = getattr(self.llm, "total_prompt_tokens", 0)
            completion_tokens = getattr(self.llm, "total_completion_tokens", 0)
            total_calls = getattr(self.llm, "total_calls", 0)
            num_rows = len(records)
            avg_tokens_per_row = (total_tokens / num_rows) if num_rows else 0.0
            avg_tokens_per_call = (total_tokens / total_calls) if total_calls else 0.0
            seconds_per_1k_tokens = (
                (total_runtime * 1000.0 / total_tokens) if total_tokens > 0 else 0.0
            )

            run_summary: dict[str, object] = {
                "timestamp_utc": datetime.utcnow().isoformat() + "Z",
                "input": {
                    "input_path": str(self.config.input_path),
                    "output_path": str(self.config.output_path),
                    "backend": self.config.backend,
                    "local_model": getattr(self.config, "local_model", None),
                    "max_workers_http": self.config.max_workers_http,
                    "max_workers_llm": self.config.max_workers_llm,
                    "enrichment_prompt_name": self.config.enrichment_prompt_name,
                    "salutation_prompt_name": self.config.salutation_prompt_name,
                    "scoring_prompt_name": self.config.scoring_prompt_name,
                    "enable_enrichment": self.config.enable_enrichment,
                    "enable_salutation": self.config.enable_salutation,
                    "enable_scoring": self.config.enable_scoring,
                    "enable_final_llm_check": self.config.enable_final_llm_check,
                    "resume": self.config.resume,
                    "output_schema": self.config.output_schema,
                    "cache_dir": str(self.config.resolve_cache_dir()),
                },
                "timing": {
                    "total_runtime_seconds": total_runtime,
                    "per_step_seconds": step_timings,
                    "per_step_status": step_status,
                },
                "records": {
                    "final_count": len(records),
                },
                "llm_usage": {
                    "total_calls": total_calls,
                    "total_tokens": total_tokens,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "avg_tokens_per_call": avg_tokens_per_call,
                    "avg_tokens_per_row": avg_tokens_per_row,
                    "seconds_per_1000_tokens": seconds_per_1k_tokens,
                    "backend": type(self.llm).__name__,
                    "model_name": getattr(self.llm, "model_name", None),
                },
                "address_cleanup": address_cleanup_stats,
                "final_llm_review": final_llm_review_stats,
            }

            with summary_path.open("w", encoding="utf-8") as f:
                json.dump(run_summary, f, ensure_ascii=False, indent=2)

            logger.info("Pipeline run summary written to %s", summary_path)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to write pipeline run summary JSON: %s", exc)

