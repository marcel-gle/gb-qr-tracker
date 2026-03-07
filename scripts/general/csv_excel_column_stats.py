#!/usr/bin/env python3
"""
Print per-column statistics (present vs missing) for a CSV or Excel file.

Usage:
  python csv_excel_column_stats.py file.csv
  python csv_excel_column_stats.py file.xlsx
  python csv_excel_column_stats.py file.xlsx --delimiter ";"  # only for CSV

Requires: pandas. For Excel (.xlsx): openpyxl.
"""

import argparse
import csv
import sys

import pandas as pd


def _read_csv_with_delimiter(path: str, delimiter: str, encoding: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=delimiter, encoding=encoding)


def _sniff_delimiter(path: str, encoding: str) -> str:
    """Sniff CSV delimiter from first 64KB of file."""
    with open(path, "r", encoding=encoding) as f:
        sample = f.read(65536)
    try:
        sniffer = csv.Sniffer()
        return sniffer.sniff(sample).delimiter
    except csv.Error:
        return ","


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print column statistics (present/missing counts) for CSV or Excel"
    )
    parser.add_argument("input", help="Input CSV or Excel file path")
    parser.add_argument(
        "-d", "--delimiter",
        default=",",
        help="CSV delimiter (default: ,). Ignored for Excel.",
    )
    parser.add_argument(
        "-e", "--encoding",
        default="utf-8",
        help="Text encoding for CSV (default: utf-8). Ignored for Excel.",
    )
    args = parser.parse_args()

    path = args.input
    if path.lower().endswith((".xlsx", ".xls")):
        try:
            df = pd.read_excel(path)
        except Exception as e:
            print(f"Error reading Excel file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        delimiters_to_try = [args.delimiter] + [
            d for d in (";", ",") if d != args.delimiter
        ]
        df = None
        last_error = None
        for delim in delimiters_to_try:
            try:
                df = _read_csv_with_delimiter(path, delim, args.encoding)
                if len(df.columns) > 1 or (len(df.columns) == 1 and len(df) > 0):
                    if delim != args.delimiter:
                        print(f"Auto-detected delimiter: {repr(delim)}\n", file=sys.stderr)
                    break
            except Exception as e:
                last_error = e
            df = None
        if df is None:
            try:
                sniffed = _sniff_delimiter(path, args.encoding)
                df = _read_csv_with_delimiter(path, sniffed, args.encoding)
                print(f"Auto-detected delimiter: {repr(sniffed)}\n", file=sys.stderr)
            except Exception:
                pass
        if df is None:
            print(f"Error reading CSV file: {last_error}", file=sys.stderr)
            sys.exit(1)

    n = len(df)
    if n == 0:
        print("No rows in file.")
        return

    # Present = non-null and, for strings, non-empty after strip
    def count_present(series: pd.Series) -> int:
        if series.dtype == object or str(series.dtype) == "string":
            return series.dropna().astype(str).str.strip().ne("").sum()
        return series.notna().sum()

    present = df.apply(count_present)
    missing = n - present

    name_width = max(len(str(c)) for c in df.columns) if len(df.columns) else 10
    name_width = min(name_width, 60)

    fmt = f"  {{:<{name_width}}}  {{:>10}}  {{:>10}}  {{:>8}}"
    header = fmt.format("Column", "Present", "Missing", "Missing %")
    print(header)
    print("-" * len(header))

    for col in df.columns:
        p, m = int(present[col]), int(missing[col])
        pct = (m / n * 100) if n else 0
        display_name = (str(col)[:57] + "...") if len(str(col)) > 60 else str(col)
        print(fmt.format(display_name, p, m, f"{pct:.1f}%"))

    print("-" * len(header))
    print(fmt.format("(total rows)", n, "-", "-"))


if __name__ == "__main__":
    main()
