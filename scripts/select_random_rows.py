#!/usr/bin/env python3
"""
sample_unique_rows.py

Select X random rows from MAIN that do NOT exist in SECONDARY based on "Adress-ID".
Assumes semicolon-separated CSVs (like your examples).

Usage:
  python sample_unique_rows.py main.csv secondary.csv 500 output.csv
"""

import sys
import pandas as pd


def main():
    if len(sys.argv) != 5:
        print("Usage: python sample_unique_rows.py <main.csv> <secondary.csv> <x> <output.csv>")
        sys.exit(1)

    main_path, secondary_path, x_str, out_path = sys.argv[1:5]
    x = int(x_str)

    # read CSVs (semicolon-separated)
    main_df = pd.read_csv(main_path, sep=";", dtype=str)
    secondary_df = pd.read_csv(secondary_path, sep=";", dtype=str)

    # build exclusion set from secondary Adress-ID
    exclude_ids = set(secondary_df["Adress-ID"].dropna().astype(str))

    # keep only main rows not in secondary
    candidates = main_df[~main_df["Adress-ID"].astype(str).isin(exclude_ids)]

    if len(candidates) == 0:
        raise SystemExit("No rows left after filtering (all Adress-ID are present in secondary).")

    # sample up to x rows (if x > available, take all)
    n = min(x, len(candidates))
    sampled = candidates.sample(n=n, random_state=None)

    # write output (keep same delimiter)
    sampled.to_csv(out_path, sep=";", index=False)
    print(f"Wrote {n} rows to {out_path} (from {len(candidates)} eligible rows).")


if __name__ == "__main__":
    main()
