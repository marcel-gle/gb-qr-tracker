#!/usr/bin/env python3
"""
Map 'Branchencode WZ' in File1 to 'Template' from File2 using LONGEST PREFIX match.
Example: Code '74' matches Branchencode '7490005'. Longest matching code wins.

If any Branchencode values cannot be matched, they are printed at the end.

Usage:
  python map_templates.py --file1 path/to/file1.xlsx --file2 path/to/file2.csv --out path/to/output.xlsx
"""

import argparse
import os
import pandas as pd


def read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xls", ".xlsx", ".xlsm"]:
        return pd.read_excel(path, dtype=str)
    elif ext in [".csv", ".tsv"]:
        sep = "\t" if ext == ".tsv" else ","
        return pd.read_csv(path, sep=sep, dtype=str)
    else:
        raise ValueError(f"Unsupported file type for: {path}")


def write_any(df: pd.DataFrame, path: str):
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xls", ".xlsx", ".xlsm"]:
        df.to_excel(path, index=False)
    elif ext in [".csv", ".tsv"]:
        sep = "\t" if ext == ".tsv" else ","
        df.to_csv(path, index=False, sep=sep)
    else:
        raise ValueError(f"Unsupported output file type for: {path}")


def find_col(df: pd.DataFrame, wanted: str) -> str:
    candidates = [c for c in df.columns if c.strip().lower() == wanted.lower()]
    if not candidates:
        raise KeyError(f"Required column '{wanted}' not found. Available: {list(df.columns)}")
    return candidates[0]


def longest_prefix_match(value: str, codes_sorted_desc) -> str | None:
    """Return the longest code that is a prefix of value, or None if nothing matches."""
    if not value or pd.isna(value):
        return None
    v = str(value).strip()
    for code in codes_sorted_desc:
        if v.startswith(code):
            return code
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file1", required=True, help="Original list (CSV/Excel) that gets the new 'Templates' column")
    ap.add_argument("--file2", required=True, help="Mapping file (CSV/Excel) with columns 'Code' and 'Template'")
    ap.add_argument("--out", required=True, help="Output file path (CSV/Excel)")
    args = ap.parse_args()

    df1 = read_any(args.file1)
    df2 = read_any(args.file2)

    # Validate columns
    if "Branchencode WZ" not in df1.columns:
        raise KeyError("File1 must contain a column named 'Branchencode WZ'.")

    code_col = find_col(df2, "Code")
    template_col = find_col(df2, "Template")

    # Normalize to strings, strip
    df1["Branchencode WZ"] = df1["Branchencode WZ"].astype(str).str.strip()
    df2[code_col] = df2[code_col].astype(str).str.strip()
    df2[template_col] = df2[template_col].astype(str).str.strip()

    # Build (unique) code -> template mapping, drop empties
    mapping_df = (
        df2[[code_col, template_col]]
        .dropna(subset=[code_col])
        .drop_duplicates(subset=[code_col], keep="first")
    )

    # Prepare list of codes sorted by length DESC (longest prefix wins)
    codes_sorted_desc = (
        mapping_df[code_col]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )
    codes_sorted_desc = [c for c in codes_sorted_desc if c]
    codes_sorted_desc.sort(key=lambda x: (-len(x), x))

    # Map: for each Branchencode, find longest matching code
    df1["MatchedCode"] = df1["Branchencode WZ"].apply(lambda v: longest_prefix_match(v, codes_sorted_desc))

    # Join to get the Template
    mapping_series = mapping_df.set_index(code_col)[template_col]
    df1["Templates"] = df1["MatchedCode"].map(mapping_series)

    # Write output
    write_any(df1, args.out)

    # Stats
    total = len(df1)
    matched = df1["Templates"].notna().sum()
    unmatched = total - matched
    print(f"✅ Done. Wrote updated file to: {args.out}")
    print(f"📊 Mapping summary: {matched}/{total} rows matched ({matched/total:.1%}).")

    # Show unmatched codes if any
    if unmatched > 0:
        unmatched_codes = sorted(df1.loc[df1["Templates"].isna(), "Branchencode WZ"].unique())
        print("\n⚠️ Unmatched Branchencode WZ values:")
        for code in unmatched_codes:
            print("  -", code)
        print(f"\n❗ Total unmatched: {len(unmatched_codes)} unique Branchencode WZ values.")
    else:
        print("🎯 All Branchencode WZ values successfully matched!")

if __name__ == "__main__":
    main()
