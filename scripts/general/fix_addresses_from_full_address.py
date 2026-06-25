#!/usr/bin/env python3
"""
Parse full_address with a local LLM and overwrite street/house_number/postcode/city.

Use when structured address fields disagree with full_address (e.g. register vs imprint).

Requires a running local ML Studio server (ML_STUDIO_BASE_URL, LOCAL_MODEL).

Example:
  PYTHONPATH=. python scripts/general/fix_addresses_from_full_address.py \\
    ~/Desktop/Briefversand/009-20260604-koenig-makler/lists/009-20260604-koenig-makler_imprint.csv \\
    -o ~/Desktop/Briefversand/009-20260604-koenig-makler/lists/009-20260604-koenig-makler_imprint.csv
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from campaign_pipeline.io.readers import load_csv_rows
from campaign_pipeline.io.writers import write_csv_rows
from campaign_pipeline.models import normalize_postcode
from list_processing.llm.local_mlstudio import LocalMLStudioClient

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    def tqdm(iterable, **kwargs):  # type: ignore[misc]
        return iterable

GERMAN_POSTCODE_RE = re.compile(r"^\d{5}$")
FOREIGN_HINTS = (
    "switzerland",
    "schweiz",
    "austria",
    "österreich",
    "france",
    "frankreich",
    "italy",
    "italien",
    "netherlands",
    "niederlande",
    "belgium",
    "belgien",
    "luxembourg",
    "luxemburg",
    "poland",
    "polen",
    "czech",
    "tschech",
    "spain",
    "spanien",
    "united kingdom",
    "vereinigtes königreich",
    "uk",
    "usa",
    "united states",
)

SYSTEM_PROMPT = """
You parse German postal addresses into structured fields for a letter campaign.

Rules:
- Input is one address string, usually German.
- Return JSON only with keys:
  street, house_number, postcode, city, is_german_address
- street: street name only (e.g. "Wulfers Weg", "Jakobistr.")
- house_number: number only (e.g. "35", "12a", "19-21")
- postcode: 5-digit German PLZ when present, else null
- city: city name only, no country
- is_german_address: true only if this is a deliverable German address
- Use null for unknown parts.
- Do not invent data not present in the input.
""".strip()


def _compact(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _extract_json(text: str) -> dict[str, Any] | None:
    raw = (text or "").strip()
    if not raw:
        return None
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", raw, flags=re.S)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def _looks_foreign(text: str) -> bool:
    lower = text.lower()
    return any(hint in lower for hint in FOREIGN_HINTS)


def _is_valid_german(parsed: dict[str, Any], full_address: str) -> bool:
    if parsed.get("is_german_address") is False:
        return False
    postcode = normalize_postcode(parsed.get("postcode"))
    street = _compact(parsed.get("street"))
    city = _compact(parsed.get("city"))
    house = _compact(parsed.get("house_number"))
    if not street or not city or not house or not postcode:
        return False
    if not GERMAN_POSTCODE_RE.fullmatch(str(postcode)):
        return False
    context = " ".join([full_address, city, street])
    if _looks_foreign(context):
        return False
    return True


def _compose_structured(street: str, house: str, postcode: str, city: str) -> str:
    main = " ".join(p for p in [_compact(street), _compact(house)] if p)
    loc = " ".join(p for p in [_compact(postcode), _compact(city)] if p)
    return ", ".join(p for p in [main, loc] if p)


def _addresses_mismatch(row: dict[str, str]) -> bool:
    full = _compact(row.get("full_address"))
    if not full:
        return False
    current = _compose_structured(
        row.get("street", ""),
        row.get("house_number", ""),
        row.get("postcode", ""),
        row.get("city", ""),
    )
    if not current:
        return True
    # Normalize for loose comparison: drop punctuation differences.
    norm = lambda s: re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    return norm(full) != norm(current)


def parse_address(llm: LocalMLStudioClient, full_address: str) -> dict[str, str] | None:
    user_prompt = (
        f'Parse this address:\n"{full_address}"\n\n'
        "Respond with one JSON object only."
    )
    raw = llm.chat(system_prompt=SYSTEM_PROMPT, user_prompt=user_prompt, temperature=0.0)
    data = _extract_json(raw)
    if not data:
        return None
    if not _is_valid_german(data, full_address):
        return None
    return {
        "street": _compact(data.get("street")),
        "house_number": _compact(data.get("house_number")),
        "postcode": normalize_postcode(data.get("postcode")) or "",
        "city": _compact(data.get("city")),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Parse full_address with local LLM and overwrite structured address fields."
    )
    parser.add_argument("input_csv", type=Path, help="Input CSV (must contain full_address column)")
    parser.add_argument("-o", "--output", type=Path, help="Output CSV (default: overwrite input)")
    parser.add_argument(
        "--only-mismatch",
        action="store_true",
        help="Only fix rows where full_address disagrees with street/house/postcode/city",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument("--limit", type=int, default=0, help="Process at most N candidate rows (0 = all)")
    parser.add_argument("--model", default=None, help="Override LOCAL_MODEL")
    parser.add_argument("--base-url", default=None, help="Override ML_STUDIO_BASE_URL")
    args = parser.parse_args(argv)

    rows, fieldnames, delimiter = load_csv_rows(args.input_csv)
    if "full_address" not in fieldnames:
        print("Error: input CSV has no full_address column", file=sys.stderr)
        return 1

    llm = LocalMLStudioClient(base_url=args.base_url, model=args.model)
    stats = {
        "rows_total": len(rows),
        "candidates": 0,
        "updated": 0,
        "skipped_no_full_address": 0,
        "skipped_match": 0,
        "failed_parse": 0,
    }

    candidates: list[dict[str, str]] = []
    for row in rows:
        full_address = _compact(row.get("full_address"))
        if not full_address:
            stats["skipped_no_full_address"] += 1
            continue
        if args.only_mismatch and not _addresses_mismatch(row):
            stats["skipped_match"] += 1
            continue
        candidates.append(row)

    stats["candidates"] = len(candidates)
    if args.limit:
        candidates = candidates[: args.limit]

    for row in tqdm(candidates, desc="Parsing addresses", unit="row"):
        full_address = _compact(row.get("full_address"))
        parsed = parse_address(llm, full_address)
        if not parsed:
            stats["failed_parse"] += 1
            tqdm.write(f"FAILED: {row.get('domain') or row.get('company_name') or '?'} — {full_address}")
            continue

        before = (
            row.get("street", ""),
            row.get("house_number", ""),
            row.get("postcode", ""),
            row.get("city", ""),
        )
        row["street"] = parsed["street"]
        row["house_number"] = parsed["house_number"]
        row["postcode"] = parsed["postcode"]
        row["city"] = parsed["city"]
        stats["updated"] += 1
        label = row.get("domain") or row.get("company_name") or "?"
        tqdm.write(f"UPDATED: {label}")
        tqdm.write(f"  full_address: {full_address}")
        tqdm.write(f"  before: street={before[0]!r} house={before[1]!r} plz={before[2]!r} city={before[3]!r}")
        tqdm.write(
            f"  after:  street={parsed['street']!r} house={parsed['house_number']!r} "
            f"plz={parsed['postcode']!r} city={parsed['city']!r}"
        )

    print("\nStats:", stats)
    if args.dry_run:
        print("Dry run — no file written.")
        return 0

    out = args.output or args.input_csv
    write_csv_rows(out, rows, fieldnames=fieldnames, delimiter=delimiter)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
