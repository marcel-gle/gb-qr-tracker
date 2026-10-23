"""
Stage 1 - Sourcing.

North Data (Power Search) and Ocean.io have no usable API for this, so this
stage ingests CSV *exports* you produce manually and normalizes them into one
master schema. Adjust COLUMN_MAP to match your actual export headers.

Master schema per row:
    company_name, domain, plz, ort, employee_estimate,
    employee_estimate_northdata, wz_code, source

employee_estimate is left None here -- North Data's headcount figures are
often stale/approximate, so it's no longer used for filtering or scoring.
Stage 3 fills it in from an explicit headcount mention on the company's own
website when it finds one (see utils.extract_employee_count). The raw North
Data number survives separately as employee_estimate_northdata, for
reference only.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

from utils import normalize_domain, dedup_rows


def _norm_header(name: str) -> str:
    """Lowercase + strip everything but letters/digits, so 'Mitarbeiterzahl',
    'MITARBEITER-ZAHL' and 'mitarbeiter_zahl' all match the same map entry."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


# Map your export's column headers -> master field. Extend as needed.
# Matching is done on the normalized header (see _norm_header), so exact
# case/punctuation doesn't need to match what's listed here.
COLUMN_MAP = {
    # North Data typical headers
    "Name": "company_name",
    "Firmenname": "company_name",
    "Website": "domain",
    "Homepage": "domain",
    "PLZ": "plz",
    "Postleitzahl": "plz",
    "Ort": "ort",
    "Mitarbeiter": "employee_estimate_northdata",
    "Beschäftigte": "employee_estimate_northdata",
    "Mitarbeiterzahl": "employee_estimate_northdata",
    "WZ-Code": "wz_code",
    "Branchencode": "wz_code",
    "branchencode": "wz_code",
    "Branche (NACE)": "wz_code",
    # Ocean.io typical headers
    "company_name": "company_name",
    "domain": "domain",
    "website": "domain",
    "employees": "employee_estimate_northdata",
    "employee_count": "employee_estimate_northdata",
    "zip": "plz",
    "city": "ort",
}
_COLUMN_MAP_NORM = {_norm_header(k): v for k, v in COLUMN_MAP.items()}


def _parse_employees(val: str | None) -> int | None:
    if not val:
        return None
    digits = "".join(c for c in str(val) if c.isdigit() or c == "-")
    # Handle ranges like "30-80": take the midpoint.
    if "-" in digits:
        parts = [p for p in digits.split("-") if p]
        nums = [int(p) for p in parts if p.isdigit()]
        if nums:
            return sum(nums) // len(nums)
        return None
    return int(digits) if digits.isdigit() else None


def load_csv(path: Path, source_name: str) -> list[dict]:
    rows: list[dict] = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        # sniff delimiter (German exports are often ';')
        sample = f.read(4096)
        f.seek(0)
        delim = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delim)
        for raw in reader:
            row = {"company_name": "", "domain": "", "plz": "", "ort": "",
                   "employee_estimate": None,
                   "employee_estimate_northdata": None, "wz_code": "",
                   "source": source_name}
            for col, val in raw.items():
                field = _COLUMN_MAP_NORM.get(_norm_header(col))
                if not field:
                    continue
                if field == "domain":
                    row[field] = normalize_domain(val)
                elif field == "employee_estimate_northdata":
                    row[field] = _parse_employees(val)
                else:
                    row[field] = (val or "").strip()
            if row["company_name"]:
                rows.append(row)
    return rows


def run(input_dir: Path) -> list[dict]:
    """Load every *.csv in input_dir. Filename prefix decides the source label:
    northdata_*.csv -> 'northdata', ocean_*.csv -> 'ocean', else 'other'.
    No employee-count filter here -- North Data's figure is unreliable and
    is no longer used for filtering; stage 3 derives the real headcount
    signal from the company's own website instead."""
    all_rows: list[dict] = []
    for path in sorted(input_dir.glob("*.csv")):
        name = path.stem.lower()
        src = ("northdata" if name.startswith("northdata")
               else "ocean" if name.startswith("ocean") else "other")
        rows = load_csv(path, src)
        print(f"  loaded {len(rows):>4} rows from {path.name} ({src})")
        all_rows.extend(rows)

    deduped = dedup_rows(all_rows)
    print(f"  {len(deduped)} rows after dedup")
    return deduped
