from __future__ import annotations

import os
from typing import Iterable

DEFAULT_LLM_MAX_CHARS = 6_000
DEFAULT_MAX_PRIMARY_LINES = 80
DEFAULT_LLM_INPUT_FLOOR = 500
PROMPT_OVERHEAD_CHARS = 500

DEFAULT_KEYWORDS = (
    "immobilien",
    "makler",
    "vermittlung",
    "vermietung",
    "exposé",
    "objekt",
    "wohnung",
    "haus",
    "gewerbe",
    "leistungen",
)


def resolve_max_chars(prompt_llm_input: dict | None) -> int:
    if prompt_llm_input and prompt_llm_input.get("max_chars") is not None:
        return int(prompt_llm_input["max_chars"])
    env_val = os.environ.get("SCORING_LLM_MAX_CHARS")
    if env_val:
        try:
            return int(env_val)
        except ValueError:
            pass
    return DEFAULT_LLM_MAX_CHARS


def resolve_max_chars_for_call(
    prompt_llm_input: dict | None,
    call_name: str,
) -> int:
    prompt_llm_input = prompt_llm_input or {}
    by_call = prompt_llm_input.get("max_chars_by_call") or {}
    if isinstance(by_call, dict) and call_name in by_call:
        return int(by_call[call_name])
    return resolve_max_chars(prompt_llm_input)


def prepare_llm_visible_text(
    text: str,
    *,
    max_chars: int,
    max_primary_lines: int = DEFAULT_MAX_PRIMARY_LINES,
    keywords: Iterable[str] | None = None,
) -> tuple[str, dict]:
    original_chars = len(text or "")
    keywords = tuple(keywords or DEFAULT_KEYWORDS)

    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return "", {
            "original_chars": original_chars,
            "sent_chars": 0,
            "lines_kept": 0,
            "truncated": bool(original_chars),
        }

    selected: list[str] = []
    selected_idx: set[int] = set()
    char_count = 0

    for idx, line in enumerate(lines):
        if idx >= max_primary_lines:
            break
        if char_count + len(line) + 1 > max_chars:
            break
        selected.append(line)
        selected_idx.add(idx)
        char_count += len(line) + 1

    if char_count < max_chars:
        for idx, line in enumerate(lines):
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

    if not selected:
        char_count = 0
        for line in lines:
            if char_count + len(line) + 1 > max_chars:
                break
            selected.append(line)
            char_count += len(line) + 1

    result = "\n".join(selected)
    if len(result) > max_chars:
        result = result[:max_chars]

    sent_chars = len(result)
    return result, {
        "original_chars": original_chars,
        "sent_chars": sent_chars,
        "lines_kept": len(selected),
        "truncated": sent_chars < original_chars,
    }


def estimate_prompt_chars(system_prompt: str, user_prompt: str) -> int:
    return len(system_prompt or "") + len(user_prompt or "")


def fits_context(estimate: int, budget: int) -> bool:
    return estimate <= budget


def shrink_max_chars_steps(max_chars: int) -> list[int]:
    steps = []
    for ratio in (0.75, 0.5, 0.25):
        steps.append(max(1, int(max_chars * ratio)))
    steps.append(DEFAULT_LLM_INPUT_FLOOR)
    seen: set[int] = set()
    ordered: list[int] = []
    for value in steps:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def word_count(text: str) -> int:
    return len([w for w in (text or "").split() if w.strip()])


def is_context_length_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    needles = (
        "context length",
        "number of tokens",
        "maximum context",
        "too many tokens",
        "error code: 400",
    )
    return any(n in msg for n in needles)
