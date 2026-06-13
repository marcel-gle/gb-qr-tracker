from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Mapping, Optional

from scripts.business.prompt_manager import Prompt

from list_processing.llm.base import LLMClient

from ..config import ScoreConfig
from ..models import BusinessRow, flatten_analysis
from ..scoring.fetch import fetch_scoring_content

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


def normalize_score(raw_value: Any, score_config: ScoreConfig) -> tuple[Optional[float], Optional[float], bool]:
    """
    Returns (match_score_normalized, score_raw, passed).
    For binary: match_score is 0 or 1.
    For 0-5 / 0-10: match_score equals raw numeric score.
    """
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

    # Solar-style prompts: pass if verkauft OR installiert (score 1), else 0.
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


def analyze_domain_with_llm(
    domain: str,
    homepage_text: str,
    prompt: Prompt,
    llm: LLMClient,
    gegenstand: str = "",
) -> Optional[Dict[str, object]]:
    fallback = (
        "Analysiere diese Homepage:\n\n"
        "Unternehmensgegenstand: {gegenstand}\n\n"
        "Domain: {domain}\n\n"
        "Homepage-Inhalt:\n\"\"\"\n{homepage_text}\n\"\"\"\n\n"
        'Antworte ausschließlich mit JSON.'
    )
    user_prompt = (
        prompt.format_user_prompt(domain=domain, homepage_text=homepage_text, gegenstand=gegenstand or "(nicht angegeben)")
        if (prompt.user_prompt_template or "").strip()
        else fallback.format(domain=domain, homepage_text=homepage_text, gegenstand=gegenstand or "(nicht angegeben)")
    )
    content = llm.chat(
        system_prompt=prompt.system_prompt,
        user_prompt=user_prompt,
        response_format=None,
        temperature=0,
    )
    return _extract_json_from_response(content)


class DomainScoringService:
    def __init__(self, llm: LLMClient, prompt: Prompt, score_config: ScoreConfig) -> None:
        self._llm = llm
        self._prompt = prompt
        self._score_config = score_config
        self._extraction_mode = prompt.content_extraction_mode
        self._browser_fallback = prompt.content_extraction_browser_fallback

    def score_row(self, row: BusinessRow) -> bool:
        page_content = fetch_scoring_content(
            row.domain,
            mode=self._extraction_mode,
            browser_fallback=self._browser_fallback,
        )
        if not page_content:
            logger.info("No homepage content for %s", row.domain)
            return False

        llm_input = page_content.for_llm(self._extraction_mode)
        if not llm_input.strip():
            logger.info("Empty LLM input for %s", row.domain)
            return False

        result = analyze_domain_with_llm(
            row.domain,
            llm_input,
            self._prompt,
            self._llm,
            gegenstand=row.gegenstand or "",
        )
        if not result:
            return False
        row.domain_analysis_raw = result
        row.score_field = self._score_config.field
        row.score_scale = self._score_config.scale
        normalized, raw, passed = evaluate_pass(result, self._score_config, self._prompt.pass_rules)
        row.match_score = normalized
        row.score_raw = raw
        row.passed_score_filter = passed
        return True


def apply_analysis_flat(row: BusinessRow) -> None:
    if isinstance(row.domain_analysis_raw, dict):
        row.extra.update({k: v for k, v in flatten_analysis(row.domain_analysis_raw).items() if k not in row.extra})
