#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dedupe_blacklist.py

Removes blacklist entries from Table1 (CSV or Excel),
keeping Table1’s original formatting and headers.
Outputs:
 - table1_cleaned.csv/xlsx (same columns, no blacklisted rows)
 - exact_matches.csv (original values)
 - close_matches.csv
"""

import argparse
import os
import re
import sys
import difflib
import pandas as pd

DEFAULT_CLOSE_THRESHOLD = 0.92


# ---------- Helper functions ----------
def is_excel(path: str) -> bool:
    return os.path.splitext(path)[1].lower() in {".xls", ".xlsx", ".xlsm", ".xlsb"}


def read_table(path: str, sheet: str = None) -> pd.DataFrame:
    if is_excel(path):
        return pd.read_excel(path, sheet_name=sheet)
    return pd.read_csv(path)


def write_table(df: pd.DataFrame, path: str):
    if is_excel(path):
        df.to_excel(path, index=False)
    else:
        df.to_csv(path, index=False)


def normalize_text(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().upper()
    s = (s.replace("Ä", "AE")
           .replace("Ö", "OE")
           .replace("Ü", "UE")
           .replace("ß".upper(), "SS"))
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def ensure_columns(df: pd.DataFrame, src: str) -> pd.DataFrame:
    col_map_candidates = {
        "PLZ": ["plz", "postleitzahl", "zip", "zip_code"],
        "Ort": ["ort", "stadt", "city", "gemeinde"],
        "Straße": ["straße", "strasse", "str", "str.", "street"],
        "Hausnummer": ["hausnummer", "hnr", "nr", "no", "number", "house_number"],
    }
    lower_to_actual = {c.lower(): c for c in df.columns}
    resolved = {}
    for canon, variants in col_map_candidates.items():
        found = None
        for v in [canon] + variants:
            if v.lower() in lower_to_actual:
                found = lower_to_actual[v.lower()]
                break
        if not found:
            raise ValueError(f"{src}: Missing required column '{canon}'. Found: {list(df.columns)}")
        resolved[canon] = found
    return df.rename(columns=resolved)


def normalize_for_matching(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["PLZ", "Ort", "Straße", "Hausnummer"]:
        out[c] = out[c].map(normalize_text)
    out["key_full"] = (
        out["PLZ"] + "|" + out["Ort"] + "|" + out["Straße"] + "|" + out["Hausnummer"]
    )
    out["key_city"] = out["PLZ"] + "|" + out["Ort"]
    out["key_streetnum"] = out["Straße"] + " " + out["Hausnummer"]
    return out


def find_close_matches(t1_unmatched: pd.DataFrame, bl: pd.DataFrame, threshold: float) -> pd.DataFrame:
    groups = {}
    for row in bl.itertuples(index=False):
        groups.setdefault(row.key_city, []).append(row.key_streetnum)

    records = []
    for r in t1_unmatched.itertuples(index=False):
        candidates = groups.get(r.key_city, [])
        if not candidates:
            continue
        best_score = -1.0
        best_cand = None
        for c in candidates:
            score = difflib.SequenceMatcher(None, r.key_streetnum, c).ratio()
            if score > best_score:
                best_score = score
                best_cand = c
        if best_score >= threshold:
            records.append({
                "PLZ": r.PLZ,
                "Ort": r.Ort,
                "Straße": r.Straße,
                "Hausnummer": r.Hausnummer,
                "Blacklist_candidate": best_cand,
                "similarity": round(best_score, 3),
            })
    return pd.DataFrame.from_records(records)


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Remove blacklist entries from a table, keeping original formatting.")
    ap.add_argument("--table1", required=True)
    ap.add_argument("--blacklist", required=True)
    ap.add_argument("--table1-sheet", default=None)
    ap.add_argument("--blacklist-sheet", default=None)
    ap.add_argument("--close-threshold", type=float, default=DEFAULT_CLOSE_THRESHOLD)
    ap.add_argument("--output-dir", default=".")
    ap.add_argument("--print-removed", type=int, default=50,
                    help="Print up to N removed addresses (default 50).")
    ap.add_argument("--print-all-removed", action="store_true",
                    help="Print ALL removed addresses (overrides --print-removed).")
    args = ap.parse_args()

    print("\n📄 Loading data...")
    df1_raw = read_table(args.table1, args.table1_sheet)
    df2_raw = read_table(args.blacklist, args.blacklist_sheet)

    # Keep the ORIGINAL (all columns, all formatting) for writing later
    df1_original = df1_raw.copy()

    # Canonical column view (original values under standard names)
    df1 = ensure_columns(df1_raw, "Table1")
    df2 = ensure_columns(df2_raw, "Blacklist")

    # Normalized copies for matching
    df1n = normalize_for_matching(df1)
    df2n = normalize_for_matching(df2)

    # Exact matches mask
    blacklist_keys = set(df2n["key_full"])
    df1n["is_blacklisted"] = df1n["key_full"].isin(blacklist_keys)

    exact_mask = df1n["is_blacklisted"]
    exact_matches_norm = df1n[exact_mask]
    unmatched = df1n[~exact_mask]

    # Close matches (info only)
    close_matches = find_close_matches(unmatched, df2n, args.close_threshold)

    # Cleaned = ORIGINAL rows excluding blacklisted
    cleaned_original = df1_original.loc[~exact_mask].copy()

    # Exact matches with ORIGINAL (pretty) values for output/printing
    exact_matches_original_cols = df1.loc[exact_mask, ["PLZ", "Ort", "Straße", "Hausnummer"]].copy()

    # Prepare output paths
    os.makedirs(args.output_dir, exist_ok=True)
    ext = ".xlsx" if is_excel(args.table1) else ".csv"
    cleaned_path = os.path.join(args.output_dir, f"table1_cleaned{ext}")
    exact_path = os.path.join(args.output_dir, "exact_matches.csv")
    close_path = os.path.join(args.output_dir, "close_matches.csv")

    # Write files
    write_table(cleaned_original, cleaned_path)
    exact_matches_original_cols.to_csv(exact_path, index=False)  # ORIGINAL values
    close_matches.to_csv(close_path, index=False)

    # Console summary
    removed_cnt = len(exact_matches_norm)
    print("\n✅ Done.")
    print(f"  Exact matches removed: {removed_cnt}")
    print(f"  Cleaned file: {cleaned_path}")
    print(f"  Exact matches (original values): {exact_path}")
    print(f"  Close matches (for review): {close_path}")

    # Pretty-print removed addresses
    print("\n🗑️  Removed addresses (exact blacklist matches):")
    if removed_cnt == 0:
        print("  (none)")
    else:
        to_print = removed_cnt if args.print_all_removed else min(args.print_removed, removed_cnt)
        for row in exact_matches_original_cols.head(to_print).itertuples(index=False):
            # Format: "PLZ Ort, Straße Hausnummer"
            plz, ort, strasse, hnr = row
            print(f"  - {plz} {ort}, {strasse} {hnr}")
        if not args.print_all_removed and removed_cnt > to_print:
            print(f"  ... and {removed_cnt - to_print} more (use --print-all-removed to show all).")
    print()

if __name__ == "__main__":
    main()
