from __future__ import annotations

import json
import logging
import re
import threading
from typing import Any, Dict, Mapping, Optional

from scripts.business.prompt_manager import Prompt, get_prompt

from list_processing.llm.base import LLMClient

from ..config import ScoreConfig
from ..models import BusinessRow, flatten_analysis
from ..scoring.compute import compute_veraltung_score
from ..scoring.extract import TechnicalSignals
from ..scoring.fetch import fetch_scoring_content
from ..scoring.truncate import (
    DEFAULT_LLM_INPUT_FLOOR,
    PROMPT_OVERHEAD_CHARS,
    estimate_prompt_chars,
    fits_context,
    is_context_length_error,
    prepare_llm_visible_text,
    resolve_max_chars_for_call,
    shrink_max_chars_steps,
    word_count,
)

logger = logging.getLogger(__name__)


def extract_homepage_text(domain: str) -> Optional[str]:
    """Backward-compatible wrapper: text-only extraction."""
    content = fetch_scoring_content(domain, mode="text_only", browser_fallback=False)
    if not content:
        return None
    return content.for_llm("text_only") or None


def _extract_json_from_response(content: str) -> Optional[Dict[str, object]]:
    content = (content or "").strip()
    if not content:
        return None
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", content, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return None


def _response_format_from_prompt(prompt: Prompt) -> Optional[Dict[str, str]]:
    fmt = prompt.output_format or {}
    if fmt.get("type") == "json":
        return {"type": "json_object"}
    return None


def normalize_score(raw_value: Any, score_config: ScoreConfig) -> tuple[Optional[float], Optional[float], bool]:
    if raw_value is None:
        return None, None, False

    if score_config.scale == "binary":
        if isinstance(raw_value, bool):
            normalized = 1.0 if raw_value else 0.0
            return normalized, normalized, normalized >= score_config.pass_threshold
        if isinstance(raw_value, (int, float)):
            normalized = 1.0 if float(raw_value) >= 1 else 0.0
            return normalized, float(raw_value), normalized >= score_config.pass_threshold
        s = str(raw_value).strip().lower()
        if s in ("true", "yes", "1"):
            return 1.0, 1.0, True
        return 0.0, 0.0, False

    try:
        raw = float(str(raw_value).replace(",", "."))
    except (TypeError, ValueError):
        return None, None, False

    return raw, raw, raw >= score_config.pass_threshold


def extract_score_from_result(result: Dict[str, object], score_config: ScoreConfig) -> tuple[Optional[float], Optional[float], bool]:
    field = score_config.field

    if score_config.scale == "binary" and ("verkauft" in result or "installiert" in result):
        solar_match = bool(result.get("verkauft")) or bool(result.get("installiert"))
        if field in ("score", "verkauft", "match_score"):
            if field == "score" and result.get("score") is not None:
                return normalize_score(result.get("score"), score_config)
            return normalize_score(solar_match, score_config)

    raw = result.get(field)
    if raw is None and field == "match_score":
        raw = result.get("score")
    if raw is None and score_config.scale == "binary":
        raw = result.get("is_match") or result.get("match")
    return normalize_score(raw, score_config)


def _coerce_bool(value: object) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "yes", "1"):
            return True
        if s in ("false", "no", "0"):
            return False
    return None


def evaluate_pass(
    result: Dict[str, object],
    score_config: ScoreConfig,
    pass_rules: Mapping[str, Any] | None = None,
) -> tuple[Optional[float], Optional[float], bool]:
    normalized, raw, score_passed = extract_score_from_result(result, score_config)
    if not score_passed:
        return normalized, raw, False

    require_boolean = (pass_rules or {}).get("require_boolean") if pass_rules else None
    if isinstance(require_boolean, dict):
        for field, expected in require_boolean.items():
            actual = _coerce_bool(result.get(field))
            if actual is None or actual is not expected:
                return normalized, raw, False

    return normalized, raw, True


def _prepare_visible_for_call(
    visible_text: str,
    prompt: Prompt,
    *,
    call_name: str,
) -> tuple[str, dict]:
    llm_input = prompt.llm_input
    max_chars = resolve_max_chars_for_call(llm_input, call_name)
    keywords = llm_input.get("keywords")
    max_primary_lines = int(llm_input.get("max_primary_lines", 80))
    prepared, meta = prepare_llm_visible_text(
        visible_text,
        max_chars=max_chars,
        max_primary_lines=max_primary_lines,
        keywords=keywords if isinstance(keywords, list) else None,
    )
    meta["call"] = call_name
    return prepared, meta


def _fit_visible_text_to_budget(
    visible_text: str,
    prompt: Prompt,
    *,
    call_name: str,
    system_prompt: str,
    user_template_kwargs: dict[str, str],
) -> tuple[str, dict]:
    llm_input = prompt.llm_input
    base_max = resolve_max_chars_for_call(llm_input, call_name)
    keywords = llm_input.get("keywords")
    max_primary_lines = int(llm_input.get("max_primary_lines", 80))

    for max_chars in [base_max, *shrink_max_chars_steps(base_max)]:
        prepared, meta = prepare_llm_visible_text(
            visible_text,
            max_chars=max_chars,
            max_primary_lines=max_primary_lines,
            keywords=keywords if isinstance(keywords, list) else None,
        )
        user_kwargs = {**user_template_kwargs, "homepage_text": prepared}
        user_prompt = prompt.format_user_prompt(**user_kwargs)
        budget = max_chars + len(system_prompt or "") + PROMPT_OVERHEAD_CHARS
        if fits_context(estimate_prompt_chars(system_prompt, user_prompt), budget):
            meta["call"] = call_name
            meta["max_chars_used"] = max_chars
            return prepared, meta

    prepared, meta = prepare_llm_visible_text(
        visible_text,
        max_chars=DEFAULT_LLM_INPUT_FLOOR,
        max_primary_lines=max_primary_lines,
        keywords=keywords if isinstance(keywords, list) else None,
    )
    meta["call"] = call_name
    meta["max_chars_used"] = DEFAULT_LLM_INPUT_FLOOR
    meta["floor"] = True
    return prepared, meta


def _chat_with_context_retry(
    llm: LLMClient,
    *,
    prompt: Prompt,
    system_prompt: str,
    user_template_kwargs: dict[str, str],
    visible_text: str,
    call_name: str,
) -> tuple[Optional[Dict[str, object]], dict, bool]:
    prepared, meta = _fit_visible_text_to_budget(
        visible_text,
        prompt,
        call_name=call_name,
        system_prompt=system_prompt,
        user_template_kwargs=user_template_kwargs,
    )
    user_kwargs = {**user_template_kwargs, "homepage_text": prepared}
    user_prompt = prompt.format_user_prompt(**user_kwargs)
    response_format = _response_format_from_prompt(prompt)
    context_error = False

    def _call(user_text: str) -> Optional[Dict[str, object]]:
        kwargs = {**user_template_kwargs, "homepage_text": user_text}
        up = prompt.format_user_prompt(**kwargs)
        content = llm.chat(
            system_prompt=system_prompt,
            user_prompt=up,
            response_format=response_format,
            temperature=0,
        )
        return _extract_json_from_response(content)

    try:
        result = _call(prepared)
        meta["sent_chars"] = len(prepared)
        return result, meta, context_error
    except Exception as exc:
        if not is_context_length_error(exc):
            raise
        context_error = True
        retry_max = max(DEFAULT_LLM_INPUT_FLOOR, int(meta.get("max_chars_used", resolve_max_chars_for_call(prompt.llm_input, call_name)) * 0.5))
        retry_prepared, retry_meta = prepare_llm_visible_text(
            visible_text,
            max_chars=retry_max,
            max_primary_lines=int(prompt.llm_input.get("max_primary_lines", 80)),
            keywords=prompt.llm_input.get("keywords") if isinstance(prompt.llm_input.get("keywords"), list) else None,
        )
        meta.update(retry_meta)
        meta["retry"] = True
        try:
            result = _call(retry_prepared)
            meta["sent_chars"] = len(retry_prepared)
            return result, meta, context_error
        except Exception as retry_exc:
            if is_context_length_error(retry_exc):
                meta["sent_chars"] = len(retry_prepared)
                return None, meta, True
            raise


def analyze_domain_with_llm(
    domain: str,
    homepage_text: str,
    prompt: Prompt,
    llm: LLMClient,
    gegenstand: str = "",
    *,
    call_name: str = "default",
) -> tuple[Optional[Dict[str, object]], dict]:
    llm_input = prompt.llm_input
    max_chars = resolve_max_chars_for_call(llm_input, call_name)
    prepared, meta = _prepare_visible_for_call(homepage_text, prompt, call_name=call_name)

    fallback = (
        "Analysiere diese Homepage:\n\n"
        "Unternehmensgegenstand: {gegenstand}\n\n"
        "Domain: {domain}\n\n"
        "Homepage-Inhalt:\n\"\"\"\n{homepage_text}\n\"\"\"\n\n"
        'Antworte ausschließlich mit JSON.'
    )
    template_kwargs = {
        "domain": domain,
        "homepage_text": prepared,
        "gegenstand": gegenstand or "(nicht angegeben)",
    }
    if (prompt.user_prompt_template or "").strip():
        user_prompt = prompt.format_user_prompt(**template_kwargs)
    else:
        user_prompt = fallback.format(**template_kwargs)

    system_prompt = prompt.system_prompt
    response_format = _response_format_from_prompt(prompt)

    def _do_call(text: str) -> Optional[Dict[str, object]]:
        kwargs = {**template_kwargs, "homepage_text": text}
        up = prompt.format_user_prompt(**kwargs) if (prompt.user_prompt_template or "").strip() else fallback.format(**kwargs)
        content = llm.chat(
            system_prompt=system_prompt,
            user_prompt=up,
            response_format=response_format,
            temperature=0,
        )
        return _extract_json_from_response(content)

    try:
        result = _do_call(prepared)
        meta["sent_chars"] = len(prepared)
        return result, meta
    except Exception as exc:
        if is_context_length_error(exc):
            retry_max = max(DEFAULT_LLM_INPUT_FLOOR, int(max_chars * 0.5))
            retry_prepared, retry_meta = prepare_llm_visible_text(
                homepage_text,
                max_chars=retry_max,
                max_primary_lines=int(llm_input.get("max_primary_lines", 80)),
                keywords=llm_input.get("keywords") if isinstance(llm_input.get("keywords"), list) else None,
            )
            meta.update(retry_meta)
            meta["retry"] = True
            result = _do_call(retry_prepared)
            meta["sent_chars"] = len(retry_prepared)
            return result, meta
        raise


class DomainScoringService:
    def __init__(self, llm: LLMClient, prompt: Prompt, score_config: ScoreConfig) -> None:
        self._llm = llm
        self._prompt = prompt
        self._score_config = score_config
        self._extraction_mode = prompt.content_extraction_mode
        self._browser_fallback = prompt.content_extraction_browser_fallback
        self._stats = {
            "truncated": 0,
            "degraded": 0,
            "context_errors": 0,
        }
        self._stats_lock = threading.Lock()

    def _inc_stat(self, key: str, amount: int = 1) -> None:
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + amount

    @property
    def stats(self) -> dict[str, int]:
        with self._stats_lock:
            return dict(self._stats)

    def score_row(self, row: BusinessRow) -> bool:
        if self._prompt.scoring_strategy == "weighted_signals":
            return self._score_row_weighted(row)
        return self._score_row_single(row)

    def _score_row_single(self, row: BusinessRow) -> bool:
        page_content = fetch_scoring_content(
            row.domain,
            mode=self._extraction_mode,
            browser_fallback=self._browser_fallback,
        )
        if not page_content:
            logger.info("No homepage content for %s", row.domain)
            return False

        visible = page_content.visible_text
        if not visible.strip():
            logger.info("Empty LLM input for %s", row.domain)
            return False

        result, meta = analyze_domain_with_llm(
            row.domain,
            visible,
            self._prompt,
            self._llm,
            gegenstand=row.gegenstand or "",
        )
        if meta.get("truncated"):
            self._inc_stat("truncated")
        if not result:
            return False
        if meta.get("retry"):
            self._inc_stat("context_errors")

        row.domain_analysis_raw = result
        row.score_field = self._score_config.field
        row.score_scale = self._score_config.scale
        normalized, raw, passed = evaluate_pass(result, self._score_config, self._prompt.pass_rules)
        row.match_score = normalized
        row.score_raw = raw
        row.passed_score_filter = passed
        return True

    def _score_row_weighted(self, row: BusinessRow) -> bool:
        signal_weights = self._prompt.signal_weights
        enabled_keys = list(signal_weights.keys())

        page_content = fetch_scoring_content(
            row.domain,
            mode=self._extraction_mode,
            browser_fallback=self._browser_fallback,
            enabled_signal_keys=enabled_keys,
        )
        if not page_content:
            logger.info("No homepage content for %s", row.domain)
            return False

        visible = page_content.visible_text
        if not visible.strip():
            logger.info("Empty LLM input for %s", row.domain)
            return False

        sub = self._prompt.sub_prompts
        class_prompt = get_prompt(sub.get("classification", ""))
        visual_prompt = get_prompt(sub.get("visual_age", ""))
        if class_prompt is None or visual_prompt is None:
            logger.error("Missing sub-prompts for weighted scoring on %s", self._prompt.name)
            return False

        llm_input_meta: dict[str, dict] = {}
        llm_degraded = False

        class_kwargs = {
            "domain": row.domain,
            "gegenstand": row.gegenstand or "(nicht angegeben)",
        }
        class_result, class_meta, class_ctx_err = _chat_with_context_retry(
            self._llm,
            prompt=class_prompt,
            system_prompt=class_prompt.system_prompt,
            user_template_kwargs=class_kwargs,
            visible_text=visible,
            call_name="classification",
        )
        llm_input_meta["classification"] = class_meta
        if class_meta.get("truncated"):
            self._inc_stat("truncated")
        if class_ctx_err:
            self._inc_stat("context_errors")

        if not class_result:
            logger.warning("Classification failed for %s", row.domain)
            return False

        visual_age_bonus = 0
        visuelle_signale: list[str] = []
        visual_meta: dict = {"skipped": False}

        if word_count(visible) < 30:
            visual_meta["skipped"] = True
            visual_meta["reason"] = "too_few_words"
            llm_input_meta["visual_age"] = visual_meta
        else:
            visual_kwargs = {"domain": row.domain, "gegenstand": ""}
            visual_result, visual_meta, visual_ctx_err = _chat_with_context_retry(
                self._llm,
                prompt=visual_prompt,
                system_prompt=visual_prompt.system_prompt,
                user_template_kwargs=visual_kwargs,
                visible_text=visible,
                call_name="visual_age",
            )
            llm_input_meta["visual_age"] = visual_meta
            if visual_meta.get("truncated"):
                self._inc_stat("truncated")
            if visual_ctx_err:
                self._inc_stat("context_errors")

            if visual_result:
                try:
                    visual_age_bonus = int(visual_result.get("visual_age_bonus", 0))
                except (TypeError, ValueError):
                    visual_age_bonus = 0
                raw_signals = visual_result.get("visuelle_signale")
                if isinstance(raw_signals, list):
                    visuelle_signale = [str(s) for s in raw_signals]
            else:
                llm_degraded = True
                self._inc_stat("degraded")
                if visual_ctx_err:
                    visual_meta["degraded"] = True

        signals: TechnicalSignals = page_content.technical_signals or TechnicalSignals()
        technical_total, technical_sum, scored_labels, all_detected = compute_veraltung_score(
            signals,
            visual_age_bonus,
            signal_weights,
        )

        merged: Dict[str, object] = {
            "makler": class_result.get("makler"),
            "begruendung": class_result.get("begruendung", ""),
            "score": int(round(technical_total)),
            "veraltung_signale": scored_labels,
            "technical_signals_detected": all_detected,
            "technical_score": technical_sum,
            "visual_age_bonus": max(0, min(3, visual_age_bonus)),
            "visuelle_signale": visuelle_signale,
            "llm_input_meta": llm_input_meta,
            "llm_degraded": llm_degraded,
        }

        row.domain_analysis_raw = merged
        row.score_field = self._score_config.field
        row.score_scale = self._score_config.scale
        normalized, raw, passed = evaluate_pass(merged, self._score_config, self._prompt.pass_rules)
        row.match_score = normalized
        row.score_raw = raw
        row.passed_score_filter = passed
        return True


def apply_analysis_flat(row: BusinessRow) -> None:
    if isinstance(row.domain_analysis_raw, dict):
        row.extra.update({k: v for k, v in flatten_analysis(row.domain_analysis_raw).items() if k not in row.extra})
