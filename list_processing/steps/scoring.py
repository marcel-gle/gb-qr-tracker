from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency
    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable

from scripts.business.prompt_manager import Prompt

from ..llm.base import LLMClient
from ..models import LeadRecord, PromptUsage

logger = logging.getLogger(__name__)


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10


def _fetch_url(url: str) -> Optional[requests.Response]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            return resp
    except requests.RequestException:
        return None
    return None


def _normalize_domain(domain: str) -> Optional[str]:
    domain = (domain or "").strip()
    if not domain:
        return None

    if domain.startswith("http://"):
        domain = domain[len("http://") :]
    elif domain.startswith("https://"):
        domain = domain[len("https://") :]

    domain = domain.split("/")[0].rstrip("/")

    for scheme in ("https://", "http://"):
        url = scheme + domain
        resp = _fetch_url(url)
        if resp:
            return resp.url
    return None


def extract_homepage_text(domain: str) -> Optional[str]:
    base_url = _normalize_domain(domain)
    if not base_url:
        logger.warning("Could not normalize domain: %s", domain)
        return None

    resp = _fetch_url(base_url)
    if not resp:
        logger.warning("Could not fetch homepage for: %s", domain)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    for script in soup(["script", "style"]):
        script.decompose()

    raw_text = soup.get_text(separator="\n")

    # Normalize into individual non-empty text lines.
    lines = (line.strip() for line in raw_text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    all_lines = [chunk for chunk in chunks if chunk]
    if not all_lines:
        return ""

    # Heuristic selection:
    # - Keep the first N lines (hero, nav, main claims).
    # - Additionally keep lines containing important keywords related to IT/digital
    #   services, as long as we stay under a global character cap.
    max_chars = 6000
    max_primary_lines = 80
    keywords = [
        "leistung",
        "leistungen",
        "service",
        "services",
        "lösung",
        "lösungen",
        "solution",
        "solutions",
        "angebot",
        "angebote",
        "produkte",
        "product",
        "products",
        "über uns",
        "about us",
        "referenzen",
        "case study",
        "case studies",
        "cases",
        "branchen",
        "industries",
        "it",
        "software",
        "systemhaus",
        "digitalisierung",
        "cloud",
        "managed",
        "consulting",
        "beratung",
    ]

    selected: list[str] = []
    selected_idx: set[int] = set()
    char_count = 0

    # 1) Take the first max_primary_lines lines in order.
    for idx, line in enumerate(all_lines):
        if idx >= max_primary_lines:
            break
        if char_count + len(line) + 1 > max_chars:
            break
        selected.append(line)
        selected_idx.add(idx)
        char_count += len(line) + 1

    # 2) Add additional lines containing important keywords, if space allows.
    if char_count < max_chars:
        for idx, line in enumerate(all_lines):
            if idx in selected_idx:
                continue
            lower = line.lower()
            if not any(kw in lower for kw in keywords):
                continue
            if char_count + len(line) + 1 > max_chars:
                break
            selected.append(line)
            selected_idx.add(idx)
            char_count += len(line) + 1

    # Fallback: if somehow nothing was selected (e.g. extremely short/odd page),
    # just take the first lines up to the char cap.
    if not selected:
        char_count = 0
        for line in all_lines:
            if char_count + len(line) + 1 > max_chars:
                break
            selected.append(line)
            char_count += len(line) + 1

    text = "\n".join(selected)
    if len(text) > max_chars:
        text = text[:max_chars]

    return text


def _extract_json_from_response(content: str) -> Optional[Dict[str, object]]:
    content_clean = content.strip()

    if content_clean.startswith("```json"):
        content_clean = content_clean[7:]
    elif content_clean.startswith("```"):
        content_clean = content_clean[3:]

    if content_clean.endswith("```"):
        content_clean = content_clean[:-3]

    content_clean = content_clean.strip()

    start_idx = content_clean.find("{")
    end_idx = content_clean.rfind("}")

    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
        json_str = content_clean[start_idx : end_idx + 1]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None
    return None


def analyze_domain_with_llm(
    domain: str,
    homepage_text: str,
    prompt: Prompt,
    llm: LLMClient,
    gegenstand: str = "",
) -> Optional[Dict[str, object]]:
    """
    Call the LLM for domain scoring, with automatic retries if the context
    exceeds the model's maximum length.

    On context-length errors, we iteratively shrink the homepage_text by
    approximately 1000 tokens (about 4000 characters) and retry, up to a
    small number of attempts.
    """
    # Start from the full homepage text and shrink on demand.
    approx_chars_per_1000_tokens = 4000
    current_text = homepage_text
    max_attempts = 3
    fallback_user_prompt_template = (
        "Analysiere diese Homepage:\n\n"
        "Unternehmensgegenstand: {gegenstand}\n\n"
        "Domain: {domain}\n\n"
        "Homepage-Inhalt:\n"
        "\"\"\"\n"
        "{homepage_text}\n"
        "\"\"\"\n\n"
        "Antworte ausschließlich mit:\n"
        '{{"match_score": SCORE}}'
    )

    for attempt in range(1, max_attempts + 1):
        if (prompt.user_prompt_template or "").strip():
            user_prompt = prompt.format_user_prompt(
                domain=domain,
                homepage_text=current_text,
                gegenstand=gegenstand or "(nicht angegeben)",
            )
        else:
            logger.warning(
                "Scoring prompt '%s' has no user_prompt_template; using fallback template.",
                prompt.name,
            )
            user_prompt = fallback_user_prompt_template.format(
                domain=domain,
                homepage_text=current_text,
                gegenstand=gegenstand or "(nicht angegeben)",
            )
        logger.debug("Scoring prompt %s (attempt %d)", prompt.name, attempt)
        logger.debug("System prompt:\n%s", prompt.system_prompt)
        logger.debug("User prompt:\n%s", user_prompt)

        try:
            content = llm.chat(
                system_prompt=prompt.system_prompt,
                user_prompt=user_prompt,
                response_format=None,
                temperature=0,
            )
        except Exception as exc:
            msg = str(exc).lower()
            # Heuristic detection of context-length / token-limit errors.
            if (
                "context" in msg and "length" in msg
            ) or "max context" in msg or "token limit" in msg or "maximum context" in msg:
                logger.warning(
                    "Context too long for domain scoring of %s (attempt %d): %s",
                    domain,
                    attempt,
                    exc,
                )
                # If the text is already very small, give up.
                if len(current_text) <= approx_chars_per_1000_tokens:
                    logger.warning(
                        "Homepage text for %s is still too long after truncation; "
                        "skipping scoring.",
                        domain,
                    )
                    return None
                # Shrink by ~1000 tokens and retry.
                current_text = current_text[:-approx_chars_per_1000_tokens]
                logger.info(
                    "Retrying domain scoring for %s with shorter homepage text "
                    "(len=%d chars).",
                    domain,
                    len(current_text),
                )
                continue

            # For non-context-related errors, propagate so callers can handle/log.
            raise

        logger.debug("LLM raw content for %s (attempt %d): %s", domain, attempt, content)
        data = _extract_json_from_response(content)
        logger.debug("Parsed scoring result for %s (attempt %d): %s", domain, attempt, data)
        return data

    # All attempts exhausted.
    return None


class DomainScoringService:
    """
    Wraps domain scoring logic so it can be used on LeadRecord instances.
    """

    def __init__(self, llm: LLMClient, prompt: Prompt) -> None:
        self._llm = llm
        self._prompt = prompt

    def score_record(self, record: LeadRecord) -> bool:
        domain = (record.website or "").strip()
        if not domain:
            return False

        homepage_text = extract_homepage_text(domain)
        if not homepage_text:
            logger.info("Skipping scoring for %s; homepage text missing.", domain)
            return False

        result = analyze_domain_with_llm(
            domain,
            homepage_text,
            self._prompt,
            self._llm,
            gegenstand=record.gegenstand or "",
        )
        if result is None:
            logger.warning("LLM scoring failed for %s", domain)
            return False

        record.domain_analysis_raw = result
        score_value = result.get("match_score")
        if score_value is None:
            score_value = result.get("score")
        if score_value is not None:
            try:
                record.domain_match_score = int(score_value)
            except (TypeError, ValueError):
                pass

        record.prompts_used.append(
            PromptUsage(
                step="domain_scoring",
                prompt_name=self._prompt.name,
                version=self._prompt.version,
                backend=type(self._llm).__name__,
                model=getattr(self._llm, "model_name", None),
            )
        )

        return True


def score_records(
    records: List[LeadRecord],
    service: DomainScoringService,
    *,
    max_workers_http: int = 10,
) -> None:
    """
    Score all records concurrently.
    """
    if not records:
        return

    logger.info(
        "Starting domain scoring for %d records (max_workers_http=%d)",
        len(records),
        max_workers_http,
    )

    def _worker(rec: LeadRecord) -> None:
        try:
            service.score_record(rec)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Error scoring domain %s (%s): %s",
                rec.company_name,
                rec.website,
                exc,
            )

    with ThreadPoolExecutor(max_workers=max_workers_http) as executor:
        futures = [executor.submit(_worker, rec) for rec in records]
        for _ in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Scoring",
            unit="record",
        ):
            pass

    logger.info("Domain scoring finished")

