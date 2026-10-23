"""
Stage 4 - Composite scoring & ranking.

Turns the accumulated signals into a single 0-ish..95 score and a qualify flag.
"""
from __future__ import annotations

from config import (WEIGHTS, HR_DISQUALIFY, HR_CONF_KEEP, EMPLOYEE_SWEETSPOT,
                    SCORE_QUALIFY_MIN)


def score_row(row: dict) -> dict:
    score = 0
    reasons: list[str] = []

    if row.get("hiring_signal"):
        score += WEIGHTS["active_driver_posting"]
        reasons.append("active driver posting")
    if row.get("posting_count", 0) >= 2:
        score += WEIGHTS["multiple_or_recent"]
        reasons.append("multiple postings")

    emp = row.get("employee_estimate")
    lo, hi = EMPLOYEE_SWEETSPOT
    if emp is not None and lo <= emp <= hi:
        score += WEIGHTS["employee_sweetspot"]
        reasons.append("headcount sweet spot")

    if row.get("has_hr") is False and \
            row.get("hr_confidence", 0) >= HR_CONF_KEEP:
        score += WEIGHTS["no_hr_high_conf"]
        reasons.append("no HR (high conf)")
    if row.get("direct_to_gf"):
        score += WEIGHTS["direct_to_gf"]
        reasons.append("direct-to-GF application")

    if row.get("has_hr") is True and \
            row.get("hr_confidence", 0) >= HR_CONF_KEEP:
        score += HR_DISQUALIFY
        reasons.append("HR detected -> disqualify")

    row["score"] = score
    row["score_reasons"] = "; ".join(reasons)
    row["qualified"] = score >= SCORE_QUALIFY_MIN
    return row


def run(rows: list[dict]) -> list[dict]:
    for row in rows:
        score_row(row)
    rows.sort(key=lambda r: r["score"], reverse=True)
    q = sum(1 for r in rows if r["qualified"])
    print(f"  scored {len(rows)} rows; {q} qualified "
          f"(>= {SCORE_QUALIFY_MIN})")
    return rows
