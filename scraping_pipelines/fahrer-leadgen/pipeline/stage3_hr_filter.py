"""
Stage 3 - HR-department exclusion (the negative filter).

Scrapes impressum/team/kontakt/karriere pages, does a cheap deterministic
keyword pass, then hands the combined text to a local LLM for a structured
verdict. Companies WITH a dedicated HR function are excluded.

Also derives employee_estimate from an explicit headcount mention on the
company's own site (e.g. "45 Mitarbeiter"), reusing the same page text --
North Data's employee figure is unreliable, so this is the authoritative
source for the sweet-spot scoring bonus in stage 4.

Adds to each row:
    has_hr: bool | None          # None = uncertain -> manual review
    hr_confidence: float
    hr_evidence: str
    direct_to_gf: bool           # applications routed to owner/GF
    review_flag: bool            # medium-confidence -> human check
    employee_estimate: int | None  # from website text, None if not stated
"""
from __future__ import annotations

import json
import re

import httpx

from config import (HR_PROBE_PATHS, HR_ROLE_SIGNALS, HR_MAILBOX_HINTS,
                    HR_CONF_KEEP, LLM_API_URL, LLM_MODEL, LLM_TIMEOUT)
from utils import fetch, visible_text, extract_employee_count

# ---------------------------------------------------------------------------
# Deterministic pre-pass: cheap, runs before spending an LLM call.
# ---------------------------------------------------------------------------
def collect_pages(domain: str) -> tuple[str, int, int]:
    """Returns (combined_text, pages_reached, pages_probed). Per-URL fetch
    errors are suppressed here (quiet=True) -- the caller reports one
    aggregated summary line per company instead."""
    probed = 1 + len(HR_PROBE_PATHS)
    if not domain:
        return "", 0, probed
    chunks: list[str] = []
    reached = 0
    home = fetch(f"https://{domain}/", expect_json=False, quiet=True)
    if home:
        reached += 1
        chunks.append(visible_text(home)[:4000])
    for path in HR_PROBE_PATHS:
        page = fetch(f"https://{domain}/{path}", expect_json=False, quiet=True)
        if page:
            reached += 1
            chunks.append(f"[{path}] " + visible_text(page)[:4000])
    return "\n\n".join(chunks), reached, probed


def keyword_hr_hits(text: str) -> list[str]:
    low = text.lower()
    return [sig for sig in HR_ROLE_SIGNALS if sig in low]


def mailbox_hits(text: str) -> list[str]:
    low = text.lower()
    return [m for m in HR_MAILBOX_HINTS if m in low]


# ---------------------------------------------------------------------------
# LLM classifier. Swap `call_llm` for your local endpoint (Ollama/vLLM/etc.).
# Kept separate so the deterministic pipeline is testable without a model.
# ---------------------------------------------------------------------------
LLM_SYSTEM_PROMPT = """Du bist ein Analyst, der deutsche Firmenwebsites prüft.
Aufgabe: Entscheide, ob das Unternehmen eine EIGENE Personalabteilung oder eine
dedizierte HR-Person hat.

Als "hat HR" gilt: benannte Person mit Titel wie Personalleiter/in,
Personalreferent/in, Personalsachbearbeiter/in, HR Manager, Leiter Personal,
People & Culture, Recruiter, Talent Acquisition; oder ein eigener
Personalbereich/Personalabteilung.

Als "keine HR" gilt: Bewerbungen gehen direkt an Geschäftsführer/Inhaber oder an
eine generische info@-Adresse; keine benannte Personalfunktion auffindbar;
kleines, inhabergeführtes Unternehmen ohne Personalbereich.

Antworte AUSSCHLIESSLICH mit JSON, ohne Vorwort, ohne Markdown:
{"has_hr_department": true|false,
 "hr_evidence": "kurze Begründung mit gefundenem Beleg oder 'kein Beleg'",
 "direct_to_gf": true|false,
 "confidence": 0.0-1.0}"""


def _parse_llm_json(content: str) -> dict:
    """Small local models don't always obey 'JSON only, no markdown' -- pull
    out the first {...} block instead of assuming the whole reply is JSON."""
    match = re.search(r"\{.*\}", content, re.S)
    if not match:
        raise ValueError(f"no JSON object in LLM output: {content[:200]!r}")
    return json.loads(match.group(0))


def call_llm(text: str) -> dict:
    """Calls a local OpenAI-compatible chat endpoint (LM Studio, Ollama's
    /v1 shim, vLLM, ...) configured via LLM_API_URL / LLM_MODEL in config.py.

    Never raises: any request/parse failure falls back to an "uncertain"
    verdict, so one bad response routes that company to the review queue
    instead of crashing the whole run.
    """
    try:
        r = httpx.post(LLM_API_URL, json={
            "model": LLM_MODEL,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": LLM_SYSTEM_PROMPT},
                {"role": "user", "content": text[:12000]},
            ],
        }, timeout=LLM_TIMEOUT)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        return _parse_llm_json(content)
    except Exception as e:  # noqa: BLE001 - one bad LLM call shouldn't kill the run
        print(f"  [llm] {type(e).__name__}: {e}")
        return {"has_hr_department": None, "hr_evidence": f"LLM-Fehler: {e}",
                "direct_to_gf": False, "confidence": 0.0}


def classify(row: dict) -> dict:
    text, pages_reached, pages_probed = collect_pages(row.get("domain", ""))
    row["employee_estimate"] = extract_employee_count(text) if text else None
    kw = keyword_hr_hits(text)
    mail = mailbox_hits(text)

    verdict = call_llm(text) if text else {
        "has_hr_department": None, "hr_evidence": "no site text",
        "direct_to_gf": False, "confidence": 0.0}

    has_hr = verdict.get("has_hr_department")
    conf = float(verdict.get("confidence") or 0.0)
    evidence = verdict.get("hr_evidence", "")

    # Deterministic override: a clear role-title hit is strong on its own.
    if kw:
        has_hr = True
        conf = max(conf, 0.85)
        evidence = f"Rollen-Signal: {', '.join(kw[:3])}. {evidence}".strip()
    elif has_hr is None and mail and not kw:
        # Generic HR mailbox but no named person: weak; leave for review.
        evidence = f"HR-Postfach ({', '.join(mail)}) ohne benannte Person. " \
                   + evidence

    # Decide keep / drop / review.
    review = False
    if has_hr is True and conf >= HR_CONF_KEEP:
        keep_decision = "drop"
    elif has_hr is False and conf >= HR_CONF_KEEP:
        keep_decision = "keep"
    else:
        keep_decision = "review"
        review = True

    row["has_hr"] = has_hr
    row["hr_confidence"] = round(conf, 2)
    row["hr_evidence"] = evidence
    row["direct_to_gf"] = bool(verdict.get("direct_to_gf"))
    row["review_flag"] = review
    row["_hr_decision"] = keep_decision
    row["_pages_reached"] = pages_reached
    row["_pages_probed"] = pages_probed
    return row


def run(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Returns (kept_rows, review_rows). Dropped rows are discarded."""
    kept, review = [], []
    for i, row in enumerate(rows, 1):
        classify(row)
        d = row["_hr_decision"]
        tag = {"keep": "KEEP  ", "drop": "DROP  ", "review": "REVIEW"}[d]
        hr_state = ({"true": "HR found", "false": "no HR found"}
                    .get(str(row["has_hr"]).lower(), "uncertain"))
        pages = f"{row['_pages_reached']}/{row['_pages_probed']} pages reached"
        emp = row["employee_estimate"]
        emp_str = f"emp: {emp}" if emp is not None else "emp: n/a"
        print(f"  [{i}/{len(rows)}] {tag} {row['company_name'][:42]:<42} "
              f"{pages}, {emp_str}, {hr_state} (conf {row['hr_confidence']})")
        if d == "keep":
            kept.append(row)
        elif d == "review":
            review.append(row)
    print(f"  keep={len(kept)}  review={len(review)}  "
          f"drop={len(rows)-len(kept)-len(review)}")
    return kept, review
