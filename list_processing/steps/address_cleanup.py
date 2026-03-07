from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

from ..models import LeadRecord, normalize_postcode

logger = logging.getLogger(__name__)


def _normalize_simple(text: Optional[str]) -> str:
    if not text:
        return ""
    s = str(text).strip().lower()
    # Replace commas and multiple whitespace with single spaces.
    s = re.sub(r"[,\s]+", " ", s)
    return s.strip()


def _address_key(record: LeadRecord) -> Optional[Tuple[str, str, str]]:
    street = _normalize_simple(record.street)
    house = _normalize_simple(record.house_number)
    pc_raw = normalize_postcode(record.postcode)
    postcode = _normalize_simple(pc_raw)

    if not street or not house or not postcode:
        return None
    return (street, house, postcode)


_LEGAL_SUFFIX_PATTERN = re.compile(
    r"\b(gmbh|ug|ag|kg|gbr|ohg|kgaa|kgaa|se|e\.k\.?|e\.kfm\.?|gmbh & co\. kg)\b",
    re.IGNORECASE,
)
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


def _normalize_company_name(name: Optional[str]) -> str:
    if not name:
        return ""
    s = name.lower()
    # Strip common legal forms.
    s = _LEGAL_SUFFIX_PATTERN.sub(" ", s)
    # Remove non-alphanumeric characters.
    s = _NON_ALNUM_PATTERN.sub(" ", s)
    # Collapse whitespace.
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _are_names_likely_duplicate(a: str, b: str) -> bool:
    if not a or not b:
        return False

    # Ensure a is the shorter string.
    if len(a) > len(b):
        a, b = b, a

    # If one normalized name is (almost) contained in the other and lengths are similar,
    # treat them as likely duplicates.
    if a in b:
        length_ratio = len(a) / len(b) if len(b) else 0.0
        if length_ratio >= 0.8:
            return True

    # Token-based overlap heuristic: if they share most tokens, consider them duplicates.
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a or not tokens_b:
        return False
    common = tokens_a & tokens_b
    overlap_ratio = len(common) / max(len(tokens_a), len(tokens_b))
    return overlap_ratio >= 0.7


_GERMAN_POSTCODE_PATTERN = re.compile(r"^\d{5}$")
_FOREIGN_HINTS = [
    "switzerland",
    "schweiz",
    "austria",
    "österreich",
    "australien",
    "france",
    "frankreich",
    "italy",
    "italien",
    "netherlands",
    "niederlande",
    "belgium",
    "belgien",
]


def _is_address_valid_germany(record: LeadRecord) -> bool:
    street = _normalize_simple(record.street)
    house = _normalize_simple(record.house_number)
    city = _normalize_simple(record.city)
    postcode_raw = normalize_postcode(record.postcode)
    postcode = _normalize_simple(postcode_raw)

    if not street or not house or not city or not postcode:
        return False
    if not _GERMAN_POSTCODE_PATTERN.fullmatch(postcode):
        return False

    context = " ".join(
        [
            city,
            _normalize_simple(record.raw_address),
            _normalize_simple(record.imprint_address),
        ]
    )
    for hint in _FOREIGN_HINTS:
        if hint in context:
            return False

    return True


def cleanup_addresses(records: List[LeadRecord]) -> Dict[str, int]:
    """
    In-place cleanup of duplicate and invalid addresses.

    - Remove exact duplicate addresses (same street, house_number, postcode).
    - Remove likely duplicate businesses based on similar names within the same
      postcode+city group.
    - Remove addresses that are invalid for German heuristics.

    This function mutates the input list in-place (records[:] = filtered).
    """
    n_initial = len(records)
    if n_initial == 0:
        logger.info("Address cleanup: no records to process.")
        return {
            "start": 0,
            "exact_dups_removed": 0,
            "fuzzy_dups_removed": 0,
            "invalid_removed": 0,
            "final": 0,
        }

    keep_flags = [True] * n_initial

    # 1) Exact duplicate addresses based on strict key.
    seen_exact: Dict[Tuple[str, str, str], int] = {}
    n_removed_exact = 0
    for idx, rec in enumerate(records):
        key = _address_key(rec)
        if key is None:
            continue
        if key in seen_exact:
            keep_flags[idx] = False
            n_removed_exact += 1
            if n_removed_exact <= 10:
                logger.info(
                    "Removing duplicate address row: company=%r, address_key=%r (keeping index %d)",
                    rec.company_name,
                    key,
                    seen_exact[key],
                )
        else:
            seen_exact[key] = idx

    # 2) Likely duplicate businesses (fuzzy name + shared postcode+city).
    groups: Dict[Tuple[str, str], List[int]] = {}
    for idx, rec in enumerate(records):
        if not keep_flags[idx]:
            continue
        pc_raw = normalize_postcode(rec.postcode)
        postcode = _normalize_simple(pc_raw)
        city = _normalize_simple(rec.city)
        if not postcode or not city:
            continue
        groups.setdefault((postcode, city), []).append(idx)

    n_removed_fuzzy = 0
    for (_postcode, _city), indices in groups.items():
        if len(indices) < 2:
            continue

        norm_names: Dict[int, str] = {}
        for idx in indices:
            rec = records[idx]
            base_name = rec.company_name or rec.legal_name or ""
            norm_names[idx] = _normalize_company_name(base_name)

        # Always keep the first record; compare subsequent ones to earlier kept ones.
        kept_in_group: List[int] = []
        for idx in sorted(indices):
            if not keep_flags[idx]:
                continue
            name_i = norm_names.get(idx, "")
            if not kept_in_group:
                kept_in_group.append(idx)
                continue

            is_dup = False
            for j in kept_in_group:
                name_j = norm_names.get(j, "")
                if _are_names_likely_duplicate(name_i, name_j):
                    keep_flags[idx] = False
                    n_removed_fuzzy += 1
                    is_dup = True
                    if n_removed_fuzzy <= 10:
                        logger.info(
                            "Removing likely duplicate business by name: %r ~ %r (postcode=%s, city=%s)",
                            records[idx].company_name,
                            records[j].company_name,
                            _postcode,
                            _city,
                        )
                    break

            if not is_dup:
                kept_in_group.append(idx)

    # 3) Address validity (German heuristic).
    n_removed_invalid = 0
    for idx, rec in enumerate(records):
        if not keep_flags[idx]:
            continue
        if not _is_address_valid_germany(rec):
            keep_flags[idx] = False
            n_removed_invalid += 1
            if n_removed_invalid <= 10:
                logger.info(
                    "Removing invalid address for company=%r (street=%r, house_number=%r, postcode=%r, city=%r)",
                    rec.company_name,
                    rec.street,
                    rec.house_number,
                    rec.postcode,
                    rec.city,
                )

    # Apply filters in-place.
    new_records = [rec for rec, keep in zip(records, keep_flags) if keep]
    records[:] = new_records

    logger.info(
        "Address cleanup: start=%d, exact_dups_removed=%d, fuzzy_dups_removed=%d, invalid_removed=%d, final=%d",
        n_initial,
        n_removed_exact,
        n_removed_fuzzy,
        n_removed_invalid,
        len(records),
    )

    return {
        "start": n_initial,
        "exact_dups_removed": n_removed_exact,
        "fuzzy_dups_removed": n_removed_fuzzy,
        "invalid_removed": n_removed_invalid,
        "final": len(records),
    }

