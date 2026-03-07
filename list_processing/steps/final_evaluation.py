from __future__ import annotations

import csv
import io
import logging
from pathlib import Path
from typing import Iterable, List, Dict

from ..llm.base import LLMClient
from ..models import LeadRecord

logger = logging.getLogger(__name__)


def _format_record_for_llm(idx: int, rec: LeadRecord) -> str:
    """
    Create a compact, human-readable summary line for LLM-based QA.
    """
    company = rec.company_name or ""
    legal = rec.legal_name or ""
    website = rec.website or ""

    street = (rec.street or "").strip()
    house = (rec.house_number or "").strip()
    postcode = (str(rec.postcode).strip() if rec.postcode is not None else "")
    city = (rec.city or "").strip()

    addr_main = " ".join(p for p in [street, house] if p).strip()
    addr_loc = " ".join(p for p in [postcode, city] if p).strip()
    address = ", ".join(p for p in [addr_main, addr_loc] if p).strip()

    gegenstand = (rec.gegenstand or "").strip()
    umsatz = (rec.umsatz or "").strip()

    return (
        f"{idx}. "
        f"company_name={company!r}, "
        f"legal_name={legal!r}, "
        f"website={website!r}, "
        f"address={address!r}, "
        f"gegenstand={gegenstand!r}, "
        f"umsatz={umsatz!r}"
    )


def run_final_llm_evaluation(
    records: Iterable[LeadRecord],
    llm: LLMClient,
    output_path: Path,
) -> Dict[str, object]:
    """
    Run an LLM-based sanity check over the final list-processing output.

    The LLM sees read-only summaries of each row (not the actual CSV file)
    and is asked to detect only *serious* issues that could impair sending a
    physical letter, such as:
    - obviously bad or incomplete addresses (missing key fields, impossible PLZ)
    - addresses that clearly appear to be outside of Germany
    - remaining duplicate records (same or extremely similar address/name/website)

    The function aggregates flagged rows across all chunks and writes a small
    CSV file listing rows that need manual review, including a brief reason and
    severity. Existing data is not modified.
    """
    records_list: List[LeadRecord] = list(records)
    n = len(records_list)
    if n == 0:
        logger.info("Final LLM evaluation skipped: no records")
        return {
            "records_total": 0,
            "chunks_total": 0,
            "flagged_rows": 0,
            "issues_csv_path": None,
        }

    logger.info("Starting final LLM evaluation for %d records (output: %s)", n, output_path)

    # Build compact text lines for each record.
    lines: List[str] = [
        _format_record_for_llm(idx, rec) for idx, rec in enumerate(records_list, start=1)
    ]
    if not lines:
        logger.info("Final LLM evaluation skipped: no formatted records")
        return {
            "records_total": n,
            "chunks_total": 0,
            "flagged_rows": 0,
            "issues_csv_path": None,
        }

    # Chunk into reasonably sized pieces so we stay well within model limits.
    max_chunk_chars = 10_000
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0

    for line in lines:
        # +1 for newline
        if current and current_len + len(line) + 1 > max_chunk_chars:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line) + 1

    if current:
        chunks.append("\n".join(current))

    total_chunks = len(chunks)

    system_prompt = (
        "You are a meticulous data quality analyst reviewing a CSV export of "
        "German business leads. Each line you see is a summarized row with "
        "company_name, legal_name, website, address, gegenstand, and umsatz.\n\n"
        "IMPORTANT RULES:\n"
        "- The CSV data is authoritative and MUST NOT be changed.\n"
        "- Do NOT invent or assume external facts beyond what is in the rows.\n"
        "- Focus ONLY on SERIOUS issues that could realistically prevent a physical "
        "letter from being delivered to the right business.\n\n"
        "TREAT AS SERIOUS (flag):\n"
        "- Clearly incomplete addresses (e.g. missing street, house number, postcode, or city).\n"
        "- Obviously invalid German postcodes (not 5 digits) when a postcode is present.\n"
        "- Addresses that strongly appear to be outside Germany based on city/country words.\n"
        "- Remaining duplicate records: same or nearly identical address combined with very "
        "similar company_name/legal_name or same website.\n\n"
        "IGNORE (do NOT flag):\n"
        "- Minor data quality issues (typos, capitalization, punctuation).\n"
        "- Outliers in revenue (umsatz) or gegenstand that do not affect postal delivery.\n"
        "- Small inconsistencies unless they clearly indicate a duplicate or unusable address.\n\n"
        "Your job is to read the rows carefully and produce a machine-readable list of ONLY the "
        "rows that need manual review because of SERIOUS delivery-relevant issues."
    )

    # Collect all flagged rows from all chunks.
    flagged_rows: List[Dict[str, str]] = []

    for idx, chunk in enumerate(chunks, start=1):
        header = (
            f"Below is chunk {idx} of {total_chunks} from a CSV export. "
            f"Each line is one business record.\n\n"
            f"CHUNK {idx}/{total_chunks}:\n"
        )
        user_prompt = (
            header
            + chunk
            + "\n\n"
            "TASK:\n"
            "- Consider ONLY SERIOUS issues that could prevent successful postal delivery, as "
            "described in the system prompt.\n"
            "- For THIS CHUNK ONLY, output a CSV table with the following columns in this exact order:\n"
            "  row_index,company_name,website,address,issue,severity\n"
            "- row_index MUST be the leading numeric index at the start of each line (before the dot).\n"
            "- company_name, website, and address should be copied from the line as best as possible.\n"
            "- issue should be a short keyword like 'invalid_address', 'non_german_address', 'duplicate'.\n"
            "- severity should be 'high' or 'medium' depending on how likely the issue is to break delivery.\n"
            "- DO NOT include rows that do not have serious issues.\n"
            "- If there are no serious issues in this chunk, output only the CSV header line with no data rows.\n\n"
            "IMPORTANT:\n"
            "- Output ONLY valid CSV text. Do not include explanations, comments or any other prose.\n"
        )

        try:
            report = llm.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_format=None,
                temperature=0.0,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Final LLM evaluation failed for chunk %d/%d: %s", idx, total_chunks, exc
            )
            continue

        text = str(report).strip()
        if not text:
            continue

        # Try to locate the CSV header line containing our expected columns.
        lines = text.splitlines()
        header_idx = None
        for i, line in enumerate(lines):
            if "row_index" in line and "severity" in line:
                header_idx = i
                break

        if header_idx is None:
            logger.warning(
                "Final LLM evaluation: could not find CSV header in chunk %d/%d response; skipping.",
                idx,
                total_chunks,
            )
            continue

        csv_text = "\n".join(lines[header_idx:])
        try:
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                # Skip completely empty rows (can happen if the model outputs only a header).
                if not any(v and str(v).strip() for v in row.values()):
                    continue
                flagged_rows.append(
                    {
                        "row_index": (row.get("row_index") or "").strip(),
                        "company_name": (row.get("company_name") or "").strip(),
                        "website": (row.get("website") or "").strip(),
                        "address": (row.get("address") or "").strip(),
                        "issue": (row.get("issue") or "").strip(),
                        "severity": (row.get("severity") or "").strip(),
                    }
                )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Final LLM evaluation: failed to parse CSV for chunk %d/%d: %s",
                idx,
                total_chunks,
                exc,
            )
            continue

    if not flagged_rows:
        logger.info(
            "Final LLM evaluation completed for %d records (in %d chunk(s)); "
            "no serious issues requiring review were flagged.",
            n,
            total_chunks,
        )
        return {
            "records_total": n,
            "chunks_total": total_chunks,
            "flagged_rows": 0,
            "issues_csv_path": None,
        }

    issues_path = output_path.with_suffix("").with_name(
        f"{output_path.stem}.final_review_issues.csv"
    )
    fieldnames = ["row_index", "company_name", "website", "address", "issue", "severity"]
    try:
        with issues_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flagged_rows)
        logger.info(
            "Final LLM evaluation completed for %d records (in %d chunk(s)); "
            "%d row(s) flagged for manual review and written to %s.",
            n,
            total_chunks,
            len(flagged_rows),
            issues_path,
        )
        return {
            "records_total": n,
            "chunks_total": total_chunks,
            "flagged_rows": len(flagged_rows),
            "issues_csv_path": str(issues_path),
        }
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(
            "Final LLM evaluation: failed to write issues CSV to %s: %s",
            issues_path,
            exc,
        )
        return {
            "records_total": n,
            "chunks_total": total_chunks,
            "flagged_rows": len(flagged_rows),
            "issues_csv_path": None,
        }

