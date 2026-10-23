from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Callable, Optional

from list_processing.llm.base import LLMClient

from ..config import CampaignConfig
from ..email_fill import fill_rows_email
from ..io.readers import load_rows_as_business
from ..io.writers import write_business_rows
from ..models import BusinessRow
from ..naming import cache_path, stage_path
from ..registry import PipelineRegistry
from ..imprint.apollo import (
    DEFAULT_POLL_TIMEOUT_S,
    ApolloPhoneClient,
    count_apollo_candidates,
    get_api_key,
    match_and_reveal_for_row,
    poll_apollo_pending,
)
from ..imprint.extract import ImprintExtractor, run_imprint_scrape
from .dedupe_address import address_key

logger = logging.getLogger(__name__)


def apollo_target_rows(all_rows: list[BusinessRow], scraped: list[BusinessRow]) -> list[BusinessRow]:
    """Scraped rows that survive the later address dedupe (first occurrence wins)."""
    scraped_ids = {id(r) for r in scraped}
    seen: set = set()
    targets: list[BusinessRow] = []
    for row in all_rows:
        key = address_key(row)
        duplicate = key is not None and key in seen
        if key is not None:
            seen.add(key)
        if id(row) in scraped_ids and not duplicate:
            targets.append(row)
    return targets


def run_apollo_enrichment(
    rows: list[BusinessRow],
    client: ApolloPhoneClient,
    *,
    max_directors: int,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
    poll_timeout_s: float = DEFAULT_POLL_TIMEOUT_S,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict:
    """Match + verify, reveal verified people, then poll. Returns ``apollo_*`` stats."""
    totals: dict = {}
    total = count_apollo_candidates(rows, max_directors=max_directors)
    started = time.monotonic()
    done = 0
    if progress_callback is not None:
        progress_callback(0, max(total, 1), 0.0, "apollo_match")
    for row in rows:
        part = match_and_reveal_for_row(row, client, max_directors=max_directors)
        for key, value in part.items():
            totals[key] = totals.get(key, 0) + value
        done += sum(part.get(k, 0) for k in ("rejected", "revealed", "resumed", "cached", "error"))
        if part.get("matched") or part.get("rejected") or part.get("revealed"):
            # Persist paid results immediately so an interrupted run never pays twice.
            client.save_cache()
        if progress_callback is not None:
            progress_callback(min(done, total), max(total, 1), time.monotonic() - started, "apollo_match")
    client.save_cache()

    def on_poll(poll_done: int, poll_total: int, elapsed: float) -> None:
        if progress_callback is not None:
            progress_callback(poll_done, poll_total, elapsed, "apollo_poll")

    poll_kwargs: dict = {"max_directors": max_directors, "timeout_s": poll_timeout_s, "progress_callback": on_poll}
    if sleep_fn is not None:
        poll_kwargs["sleep_fn"] = sleep_fn
    poll_totals = poll_apollo_pending(rows, client, **poll_kwargs)
    client.save_cache()

    # Final counts come from the slots so cached and resumed results are included.
    status_counts = {"ok": 0, "none": 0, "rejected": 0, "error": 0, "pending": 0}
    rejected_by_reason: dict = {}
    for row in rows:
        for slot in (row.apollo or [])[:max_directors]:
            status = (slot or {}).get("status")
            if status in status_counts:
                status_counts[status] += 1
            if status == "rejected":
                reason = slot.get("reject_reason") or "unknown"
                rejected_by_reason[reason] = rejected_by_reason.get(reason, 0) + 1

    stats = {
        "apollo_candidates": total,
        "apollo_matched": totals.get("matched", 0),
        "apollo_revealed": totals.get("revealed", 0),
        "apollo_cached": totals.get("cached", 0),
        "apollo_timeout": poll_totals.get("timeout", 0),
    }
    if client.fatal_error:
        stats["apollo_fatal_error"] = client.fatal_error
    stats.update({f"apollo_{k}": v for k, v in status_counts.items()})
    stats.update({f"apollo_rejected_{k}": v for k, v in rejected_by_reason.items()})
    logger.info("Apollo enrichment: %s", stats)
    return stats


_SCORE_ATTRS = (
    "match_score",
    "score_raw",
    "score_scale",
    "score_field",
    "passed_score_filter",
    "domain_analysis_raw",
)


def keep_previous_imprint_rows(
    rows: list[BusinessRow],
    to_scrape: list[BusinessRow],
    imprint_output: Path,
    *,
    registry: Optional[PipelineRegistry] = None,
    max_directors: int,
) -> list[BusinessRow]:
    """Rows that are not scraped again keep their earlier imprint results.

    ``rows`` is reloaded from the scored/raw input, which has no imprint data.
    Without this, re-running the step would overwrite ``_imprint.csv`` with
    empty address/director/Apollo fields for every already-processed domain.
    Scores are taken from the fresh input; rows the registry dropped (e.g. by
    the address dedupe) stay out.
    """
    if not imprint_output.exists():
        return rows
    previous = {
        r.domain: r for r in load_rows_as_business(imprint_output, max_directors=max_directors)
    }
    scraped = {id(r) for r in to_scrape}
    out: list[BusinessRow] = []
    for row in rows:
        if id(row) in scraped:
            out.append(row)
            continue
        prev = previous.get(row.domain)
        if prev is not None:
            for attr in _SCORE_ATTRS:
                setattr(prev, attr, getattr(row, attr))
            out.append(prev)
            continue
        state = registry.domains.get(row.domain) if registry else None
        if state is not None and state.stage == "dropped":
            continue
        out.append(row)
    return out


def row_is_missing_core_fields(row: BusinessRow) -> bool:
    """True when a row lacks a street or any named managing director.

    Used by the ``only_missing_fields`` re-scrape mode to target rows that a
    previous imprint run left incomplete.
    """
    if not (row.street or "").strip():
        return True
    for d in row.directors or []:
        if (d or {}).get("first_name") or (d or {}).get("last_name"):
            return False
    return True


def resolve_imprint_input_path(config: CampaignConfig) -> Path:
    """Prefer scored output; fall back to raw_deduped / raw when scoring was skipped."""
    scored = stage_path(config.campaign_dir, config.base_name, "scored")
    if scored.exists():
        return scored
    deduped = stage_path(config.campaign_dir, config.base_name, "raw_deduped")
    if deduped.exists():
        return deduped
    return stage_path(config.campaign_dir, config.base_name, "raw")


def _is_scored_stage_path(config: CampaignConfig, path: Path) -> bool:
    scored = stage_path(config.campaign_dir, config.base_name, "scored")
    try:
        return path.resolve() == scored.resolve()
    except OSError:
        return path.name == scored.name


def run_imprint_step(
    config: CampaignConfig,
    llm: LLMClient,
    input_path: Path | None = None,
    *,
    registry: Optional[PipelineRegistry] = None,
    only_domains: set[str] | None = None,
    skip_score_filter: bool = False,
    only_missing_fields: bool = False,
    force_rescrape: bool = False,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
) -> tuple[Path, dict]:
    imprint_output = stage_path(config.campaign_dir, config.base_name, "imprint")
    if force_rescrape and only_missing_fields:
        raise ValueError("force_rescrape and only_missing_fields cannot be combined")

    if only_missing_fields:
        # Re-scrape rows from a previous imprint run that are still missing a
        # street or a named managing director. Reads the imprint output so
        # already-complete rows are preserved, and forces a fresh fetch
        # (bypassing the per-domain cache) so improved fetch logic can run.
        path = input_path or imprint_output
        if not path.exists():
            raise FileNotFoundError(
                f"Imprint output not found: {path}. Run a full imprint scrape first."
            )
    else:
        path = input_path or resolve_imprint_input_path(config)
        if not path.exists():
            raise FileNotFoundError(
                f"Imprint input not found: {path}. Run merge (and optionally score) first."
            )

    using_scored = _is_scored_stage_path(config, path)
    apply_pass_filter = (
        config.pass_score_filter
        and using_scored
        and not skip_score_filter
        and not only_missing_fields
    )
    if only_missing_fields:
        logger.info("Imprint re-scrape: only rows missing street/name from %s.", path.name)
    elif force_rescrape:
        logger.info(
            "Imprint force re-scrape of all selected rows from %s "
            "(imprint HTTP cache bypassed; Apollo cache kept).",
            path.name,
        )
    elif not using_scored:
        logger.info(
            "Imprint using %s (no _scored.csv) — score pass filter disabled.",
            path.name,
        )
    elif skip_score_filter:
        logger.info("Imprint score pass filter skipped by request.")

    rows = load_rows_as_business(path, max_directors=config.max_directors)
    input_count = len(rows)

    if apply_pass_filter:
        rows = [r for r in rows if r.passed_score_filter is True]

    if only_missing_fields:
        to_scrape = [
            r
            for r in rows
            if (only_domains is None or r.domain in only_domains)
            and row_is_missing_core_fields(r)
        ]
    elif force_rescrape:
        # Re-fetch every selected row; ignore registry stage so already-imprinted
        # domains are scraped again. Apollo enrichment afterwards reuses
        # .pipeline/apollo_phones.cache.pkl and does not re-bill finished people.
        to_scrape = [
            r for r in rows if (only_domains is None or r.domain in only_domains)
        ]
    else:
        to_scrape = [
            r
            for r in rows
            if (only_domains is None or r.domain in only_domains)
            and (registry is None or registry.should_process(r.domain, "imprint"))
        ]
        rows = keep_previous_imprint_rows(
            rows,
            to_scrape,
            imprint_output,
            registry=registry,
            max_directors=config.max_directors,
        )

    cache = cache_path(config.campaign_dir, "imprint")
    extractor = ImprintExtractor(
        llm,
        cache_path=cache,
        enable_northdata=config.enable_northdata_fallback,
    )
    run_imprint_scrape(
        to_scrape,
        extractor,
        max_workers=config.max_workers_http,
        force=only_missing_fields or force_rescrape,
        progress_callback=progress_callback,
    )

    apollo_stats: dict = {"apollo_enabled": bool(config.enable_apollo_phones)}
    if config.enable_apollo_phones:
        if not get_api_key():
            logger.warning(
                "Apollo phones enabled but APOLLO_API_KEY is not set; skipping enrichment."
            )
            apollo_stats["apollo_skipped_no_key"] = 1
        else:
            client = ApolloPhoneClient(
                cache_path=cache_path(config.campaign_dir, "apollo_phones")
            )
            # Already-scraped rows are included so Apollo can be enabled on a later
            # run; finished people come from the slot/cache without new credits.
            pool = to_scrape if only_domains is not None else rows
            targets = apollo_target_rows(rows, pool)
            apollo_stats["apollo_skipped_address_dup"] = len(pool) - len(targets)
            apollo_stats.update(
                run_apollo_enrichment(
                    targets,
                    client,
                    max_directors=config.max_directors,
                    progress_callback=progress_callback,
                )
            )

    apollo_stats["email_filled"] = fill_rows_email(rows)

    for row in to_scrape:
        if registry:
            registry.mark(row.domain, "imprint")

    write_business_rows(imprint_output, rows, max_directors=config.max_directors)
    stats = {
        "input": input_count,
        "after_score_filter": len(rows),
        "scraped": len(to_scrape),
        "output": len(rows),
        "input_path": str(path),
        "used_scored": using_scored,
        "score_filter_applied": apply_pass_filter,
        "only_missing_fields": only_missing_fields,
        "force_rescrape": force_rescrape,
        **apollo_stats,
    }
    logger.info("Imprint scrape complete: %s", stats)
    return imprint_output, stats


def run_apollo_only_step(
    config: CampaignConfig,
    *,
    input_path: Path | None = None,
    output_path: Path | None = None,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
    poll_timeout_s: float = DEFAULT_POLL_TIMEOUT_S,
    sleep_fn: Callable[[float], None] | None = None,
) -> tuple[Path, dict]:
    """Enrich a CSV with Apollo phones — no imprint re-scrape.

    Defaults to ``{base}_imprint.csv`` for both read and write. Optional
    ``input_path`` / ``output_path`` let you enrich another stage file in place
    or write to a different path. Uses the campaign
    ``.pipeline/apollo_phones.cache.pkl`` so already matched/revealed people
    are never billed twice.
    """
    source = Path(input_path) if input_path is not None else stage_path(
        config.campaign_dir, config.base_name, "imprint"
    )
    dest = Path(output_path) if output_path is not None else source
    if not source.exists():
        raise FileNotFoundError(
            f"Apollo input not found: {source}. Run imprint scrape first or pick another CSV."
        )

    rows = load_rows_as_business(source, max_directors=config.max_directors)
    stats: dict = {
        "apollo_only": True,
        "apollo_enabled": True,
        "input": len(rows),
        "output": len(rows),
        "input_path": str(source),
        "output_path": str(dest),
        "scraped": 0,
    }

    if not get_api_key():
        logger.warning(
            "Apollo phones requested but APOLLO_API_KEY is not set; skipping enrichment."
        )
        stats["apollo_skipped_no_key"] = 1
        return dest, stats

    client_kwargs: dict = {"cache_path": cache_path(config.campaign_dir, "apollo_phones")}
    if sleep_fn is not None:
        client_kwargs["sleep_fn"] = sleep_fn
    client = ApolloPhoneClient(**client_kwargs)
    targets = apollo_target_rows(rows, rows)
    stats["apollo_skipped_address_dup"] = len(rows) - len(targets)
    enrich_kwargs: dict = {
        "max_directors": config.max_directors,
        "progress_callback": progress_callback,
        "poll_timeout_s": poll_timeout_s,
    }
    if sleep_fn is not None:
        enrich_kwargs["sleep_fn"] = sleep_fn
    stats.update(run_apollo_enrichment(targets, client, **enrich_kwargs))
    stats["email_filled"] = fill_rows_email(rows)
    write_business_rows(dest, rows, max_directors=config.max_directors)
    logger.info("Apollo-only enrichment complete: %s", stats)
    return dest, stats
