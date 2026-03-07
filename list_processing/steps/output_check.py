"""
Final data check for list_processing output: postcode validation and missing-field statistics.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Iterable, List

from ..models import LeadRecord

logger = logging.getLogger(__name__)

# German postcodes are exactly 5 digits.
POSTCODE_PATTERN = re.compile(r"^\d{5}$")

def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and (not value or not value.strip()):
        return True
    return False


def _is_postcode_valid(postcode: str | None) -> bool:
    if not postcode:
        return True  # empty is not "invalid", just missing
    return bool(POSTCODE_PATTERN.match(str(postcode).strip()))


def run_output_checks(
    records: Iterable[LeadRecord],
    output_path: Path,
) -> None:
    """
    Run final data checks on the output records and print results.

    - Validates that all non-empty postcodes are valid 5-digit German PLZ.
    - Prints statistics about missing fields in the output file.
    """
    records_list: List[LeadRecord] = list(records)
    n = len(records_list)
    if n == 0:
        logger.info("Output check skipped: no records")
        return

    # --- Postcode validation ---
    invalid_postcodes: List[tuple[int, str, str]] = []
    for idx, rec in enumerate(records_list, start=1):
        pc = getattr(rec, "postcode", None)
        if pc is not None and str(pc).strip():
            if not _is_postcode_valid(pc):
                invalid_postcodes.append((idx, rec.company_name or "", str(pc)))

    # --- Missing-field statistics ---
    field_names = [
        "company_name",
        "website",
        "branchencode",
        "gegenstand",
        "umsatz",
        # "",  # intentionally not checked in summary
        "first_name",
        "last_name",
        "email",
        "phone",
        "street",
        "house_number",
        "postcode",
        "city",
        "managing_director_full",
        "managing_director_first",
        "managing_director_last",
        "salutation",
        "legal_name",
        "domain_match_score",
    ]
    missing_counts: List[tuple[str, int]] = []
    for field in field_names:
        count = sum(
            1
            for rec in records_list
            if _is_missing(getattr(rec, field, None))
        )
        missing_counts.append((field, count))

    # --- Print report ---
    logger.info("")
    logger.info("=== Output data check: %s ===", output_path.name)
    logger.info("Total records: %d", n)

    logger.info("")
    logger.info("Postcode validation (5 digits):")
    if not invalid_postcodes:
        logger.info("  All non-empty postcodes are valid (5 digits).")
    else:
        logger.info(
            "  Invalid postcodes: %d (of %d records with non-empty PLZ)",
            len(invalid_postcodes),
            sum(1 for r in records_list if getattr(r, "postcode", None) and str(getattr(r, "postcode", "")).strip()),
        )
        for idx, name, pc in invalid_postcodes[:10]:
            logger.info("    Row %d | %s | PLZ: %r", idx, name[:40], pc)
        if len(invalid_postcodes) > 10:
            logger.info("    ... and %d more.", len(invalid_postcodes) - 10)

    logger.info("")
    logger.info("Missing fields (output file):")
    for field, count in missing_counts:
        pct = (100 * count / n) if n else 0
        logger.info("  %s: %d missing (%.1f%%)", field, count, pct)

    # --- Additional consistency checks ---
    logger.info("")
    logger.info("Required fields coverage (company/address/contact):")
    required_fields = [
        "company_name",
        "street",
        "house_number",
        "postcode",
        "city",
        "first_name",
        "last_name",
        "salutation",
    ]
    for field in required_fields:
        missing = next((c for f, c in missing_counts if f == field), 0)
        pct = (100 * missing / n) if n else 0
        logger.info("  %s: %d missing (%.1f%%)", field, missing, pct)

    # Street name should not contain digits.
    street_with_digits: List[tuple[int, str, str]] = []
    for idx, rec in enumerate(records_list, start=1):
        street = getattr(rec, "street", "") or ""
        if any(ch.isdigit() for ch in str(street)):
            street_with_digits.append((idx, rec.company_name or "", str(street)))

    logger.info("")
    if not street_with_digits:
        logger.info("Street digit check: OK (no digits found in any street name).")
    else:
        logger.info(
            "Street digit check: %d record(s) have digits in the street field.",
            len(street_with_digits),
        )
        for idx, name, street in street_with_digits[:10]:
            logger.info("  Row %d | %s | street=%r", idx, name[:40], street)
        if len(street_with_digits) > 10:
            logger.info("  ... and %d more.", len(street_with_digits) - 10)

    logger.info("=== End of output data check ===")
    logger.info("")
