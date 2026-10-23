"""
Fahrer-Leadgen pipeline orchestrator.

Usage:
    python run.py                 # full run over data/input/*.csv
    python run.py --limit 20      # cap rows for a quick smoke test

Funnel: source -> hiring signal -> HR exclusion -> score.
Writes three CSVs to data/output/:
    qualified_leads.csv   (score >= threshold, no HR)
    review_queue.csv      (uncertain HR verdict -> manual check)
    full_run.csv          (everything that reached scoring, for audit)
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import stage1_source
import stage2_hiring
import stage3_hr_filter
import stage4_score

ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT / "data" / "input"
OUTPUT_DIR = ROOT / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FIELDS = [
    "company_name", "domain", "plz", "ort", "employee_estimate",
    "employee_estimate_northdata", "wz_code",
    "source", "hiring_signal", "posting_count", "hiring_evidence",
    "hiring_urls", "has_hr", "hr_confidence", "hr_evidence", "direct_to_gf",
    "review_flag", "score", "score_reasons", "qualified",
]


def _flatten(row: dict) -> dict:
    out = {}
    for k in FIELDS:
        v = row.get(k)
        out[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, list) else v
    return out


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(_flatten(r))
    print(f"  wrote {len(rows):>4} rows -> {path.relative_to(ROOT)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="cap number of sourced rows (smoke test)")
    args = ap.parse_args()

    print("Stage 1: sourcing")
    rows = stage1_source.run(INPUT_DIR)
    if args.limit:
        rows = rows[:args.limit]
        print(f"  limited to {len(rows)} rows")

    print("Stage 2: hiring signal")
    rows = stage2_hiring.run(rows)

    print("Stage 3: HR exclusion")
    kept, review = stage3_hr_filter.run(rows)

    print("Stage 4: scoring")
    scored = stage4_score.run(kept)
    review_scored = stage4_score.run(review) if review else []

    print("Output")
    qualified = [r for r in scored if r["qualified"]]
    write_csv(OUTPUT_DIR / "qualified_leads.csv", qualified)
    write_csv(OUTPUT_DIR / "review_queue.csv", review_scored)
    write_csv(OUTPUT_DIR / "full_run.csv", scored + review_scored)
    print("Done.")


if __name__ == "__main__":
    main()
