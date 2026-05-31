from __future__ import annotations

import csv
import io
import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional

from list_processing.llm.base import LLMClient

from ..models import BusinessRow

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, float], None]


def format_business_row_for_llm(idx: int, row: BusinessRow) -> str:
    street = (row.street or "").strip()
    house = (row.house_number or "").strip()
    postcode = (row.postcode or "").strip()
    city = (row.city or "").strip()
    addr_main = " ".join(p for p in [street, house] if p)
    addr_loc = " ".join(p for p in [postcode, city] if p)
    address = ", ".join(p for p in [addr_main, addr_loc] if p)

    data = row.to_dict()
    director = " ".join(
        p
        for p in [
            data.get("salutation_1"),
            data.get("first_name_1"),
            data.get("last_name_1"),
        ]
        if p
    )

    return (
        f"{idx}. domain={row.domain!r}, "
        f"company_name={row.company_name or ''!r}, "
        f"legal_name={row.legal_name or ''!r}, "
        f"address={address!r}, "
        f"director={director!r}, "
        f"email={row.email or ''!r}, "
        f"phone={row.phone or ''!r}, "
        f"template={row.template or ''!r}, "
        f"match_score={row.match_score!r}"
    )


def _chunk_lines(lines: List[str], max_chunk_chars: int = 10_000) -> List[str]:
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    for line in lines:
        if current and current_len + len(line) + 1 > max_chunk_chars:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks


def _parse_flagged_csv(text: str) -> List[Dict[str, str]]:
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if "row_index" in line and "severity" in line:
            header_idx = i
            break
    if header_idx is None:
        return []

    csv_text = "\n".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(csv_text))
    flagged: List[Dict[str, str]] = []
    for row in reader:
        if not any(v and str(v).strip() for v in row.values()):
            continue
        flagged.append(
            {
                "row_index": (row.get("row_index") or "").strip(),
                "domain": (row.get("domain") or "").strip(),
                "company_name": (row.get("company_name") or "").strip(),
                "address": (row.get("address") or "").strip(),
                "issue": (row.get("issue") or "").strip(),
                "severity": (row.get("severity") or "").strip(),
                "details": (row.get("details") or "").strip(),
            }
        )
    return flagged


def run_final_llm_review(
    rows: List[BusinessRow],
    llm: LLMClient,
    issues_path: Path,
    *,
    progress_callback: ProgressCallback | None = None,
) -> dict:
    """
    LLM sanity check for final list rows. Writes suspicious rows to issues_path.
    Does not modify the input rows.
    """
    n = len(rows)
    if n == 0:
        return {"records_total": 0, "chunks_total": 0, "flagged_rows": 0, "issues_path": None}

    lines = [format_business_row_for_llm(idx, row) for idx, row in enumerate(rows, start=1)]
    chunks = _chunk_lines(lines)
    total_chunks = len(chunks)

    system_prompt = (
        "You are a meticulous data quality analyst reviewing German business leads "
        "before a physical letter campaign.\n\n"
        "Each line is one summarized row. The data is authoritative — do not invent facts.\n\n"
        "Flag rows that need MANUAL REVIEW before sending mail. Focus on:\n"
        "1) Address appears outside Germany (non-DE country, foreign postcode/city patterns).\n"
        "2) Address looks invalid or not deliverable (missing street/PLZ/city, placeholder text, "
        "PO box only without street, obviously fake values).\n"
        "3) Other odd or suspicious data (duplicate-looking entries, director/company mismatch, "
        "nonsense fields, clear scraping errors).\n\n"
        "Do NOT flag minor typos or capitalization. Ignore revenue/score outliers unless they "
        "indicate clearly wrong company data."
    )

    flagged_rows: List[Dict[str, str]] = []
    started_chunks = 0

    for idx, chunk in enumerate(chunks, start=1):
        user_prompt = (
            f"Below is chunk {idx} of {total_chunks}. Each line is one business record.\n\n"
            f"CHUNK {idx}/{total_chunks}:\n"
            f"{chunk}\n\n"
            "TASK:\n"
            "- Flag ONLY rows needing manual review for postal delivery or obvious data problems.\n"
            "- Output CSV with columns exactly:\n"
            "  row_index,domain,company_name,address,issue,severity,details\n"
            "- row_index = leading number before the dot on each line.\n"
            "- domain copied from the line when present.\n"
            "- issue = short code: non_german_address | invalid_address | suspicious_data | duplicate | other\n"
            "- severity = high | medium\n"
            "- details = one short sentence explaining the concern.\n"
            "- If no issues in this chunk, output header only.\n"
            "- Output ONLY CSV, no markdown or prose."
        )
        try:
            report = llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_format=None,
                temperature=0.0,
            )
            flagged_rows.extend(_parse_flagged_csv(str(report).strip()))
        except Exception as exc:
            logger.warning("Final LLM review failed for chunk %d/%d: %s", idx, total_chunks, exc)
        started_chunks += 1
        if progress_callback is not None:
            progress_callback(started_chunks, total_chunks, 0.0)

    fieldnames = ["row_index", "domain", "company_name", "address", "issue", "severity", "details"]
    issues_path.parent.mkdir(parents=True, exist_ok=True)
    with issues_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(flagged_rows)

    logger.info(
        "Final LLM review: %d rows, %d chunks, %d flagged -> %s",
        n,
        total_chunks,
        len(flagged_rows),
        issues_path,
    )
    return {
        "records_total": n,
        "chunks_total": total_chunks,
        "flagged_rows": len(flagged_rows),
        "issues_path": str(issues_path),
    }
