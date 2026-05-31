from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional

from scripts.business.prompt_manager import Prompt

from list_processing.llm.base import LLMClient

from ..config import ScoreConfig
from ..models import BusinessRow, flatten_analysis
from ..imprint.fetch import normalize_domain_to_base_url

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10


def _fetch_url(url: str) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            return resp
    except requests.RequestException:
        return None
    return None


def extract_homepage_text(domain: str) -> Optional[str]:
    base_url = normalize_domain_to_base_url(domain)
    if not base_url:
        return None
    resp = _fetch_url(base_url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines)[:20_000]


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

    def score_row(self, row: BusinessRow) -> bool:
        homepage_text = extract_homepage_text(row.domain)
        if not homepage_text:
            logger.info("No homepage text for %s", row.domain)
            return False
        result = analyze_domain_with_llm(
            row.domain,
            homepage_text,
            self._prompt,
            self._llm,
            gegenstand=row.gegenstand or "",
        )
        if not result:
            return False
        row.domain_analysis_raw = result
        row.score_field = self._score_config.field
        row.score_scale = self._score_config.scale
        normalized, raw, passed = extract_score_from_result(result, self._score_config)
        row.match_score = normalized
        row.score_raw = raw
        row.passed_score_filter = passed
        return True


def apply_analysis_flat(row: BusinessRow) -> None:
    if isinstance(row.domain_analysis_raw, dict):
        row.extra.update({k: v for k, v in flatten_analysis(row.domain_analysis_raw).items() if k not in row.extra})
