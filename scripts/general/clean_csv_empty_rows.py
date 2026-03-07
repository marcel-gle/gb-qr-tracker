#!/usr/bin/env python3
"""
Remove empty rows from a semicolon-delimited CSV.
A row is considered empty if all fields are empty (e.g. ;;;;;;;;;;;;;;;;).
Usage:
  python clean_csv_empty_rows.py input.csv
  python clean_csv_empty_rows.py input.csv -o cleaned.csv
  python clean_csv_empty_rows.py input.csv  # prints to stdout
"""

import argparse
import sys


def is_empty_row(line: str, delimiter: str = ";") -> bool:
    """True if the line has no non-empty fields."""
    if not line.strip():
        return True
    fields = line.split(delimiter)
    return all(not f.strip() for f in fields)


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove empty rows from semicolon-delimited CSV")
    parser.add_argument("input", help="Input CSV file path")
    parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    parser.add_argument("-d", "--delimiter", default=";", help="Field delimiter (default: ;)")
    parser.add_argument(
        "-e",
        "--encoding",
        help=(
            "Force text encoding for input file. "
            "If omitted, tries utf-8, utf-8-sig, cp1252, then latin-1 automatically. "
            "Output file (if -o is used) is always written as utf-8."
        ),
    )
    args = parser.parse_args()

    # Read input with either forced encoding or sensible fallbacks.
    if args.encoding:
        with open(args.input, "r", encoding=args.encoding) as f:
            lines = f.readlines()
    else:
        encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
        last_error = None
        for enc in encodings_to_try:
            try:
                with open(args.input, "r", encoding=enc) as f:
                    lines = f.readlines()
                break
            except UnicodeDecodeError as e:
                last_error = e
        else:
            # If we get here, all encodings failed.
            raise RuntimeError(
                f"Could not decode {args.input!r} with encodings {encodings_to_try}"
            ) from last_error

    kept = [line for line in lines if not is_empty_row(line, args.delimiter)]

    # Always write output file as utf-8 so that umlauts and other characters are preserved.
    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        for line in kept:
            out.write(line)
    finally:
        if args.output:
            out.close()


if __name__ == "__main__":
    main()
