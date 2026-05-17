"""
CSV Deduplizierung für Brave Search API Ergebnisse
===================================================
Entfernt doppelte URLs aus einer oder mehreren CSV-Dateien.
Bei Duplikaten wird jeweils der erste Treffer (höchstes Ranking) behalten.

Nutzung:
    python deduplicate_csv.py input.csv
    python deduplicate_csv.py input.csv -o output.csv
    python deduplicate_csv.py file1.csv file2.csv file3.csv -o merged.csv
"""

import argparse
import csv
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser(
        description="Entfernt doppelte URLs aus Brave Search CSV-Dateien.",
    )
    p.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="Eine oder mehrere CSV-Dateien (;-delimited, UTF-8 BOM)",
    )
    p.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="Output-Pfad (default: <input>_dedup.csv bzw. merged_dedup.csv)",
    )
    p.add_argument(
        "--column",
        default="display_url",
        help="Spaltenname für Deduplizierung (default: display_url)",
    )
    args = p.parse_args()

    # Alle Zeilen einlesen
    rows = []
    fieldnames = None
    for path in args.inputs:
        if not path.exists():
            sys.exit(f"Fehler: Datei nicht gefunden: {path}")
        with open(path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            if fieldnames is None:
                fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)

    if not rows:
        sys.exit("Keine Daten gefunden.")

    if args.column not in fieldnames:
        sys.exit(f"Spalte '{args.column}' nicht gefunden. Verfügbar: {', '.join(fieldnames)}")

    # Deduplizieren
    seen = set()
    deduped = []
    for row in rows:
        key = row[args.column]
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    removed = len(rows) - len(deduped)

    # Output-Pfad bestimmen
    if args.output:
        out = args.output
    elif len(args.inputs) == 1:
        out = args.inputs[0].with_stem(args.inputs[0].stem + "_dedup")
    else:
        out = Path("merged_dedup.csv")

    # Schreiben
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in deduped:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f"Original:      {len(rows)} Zeilen")
    print(f"Dedupliziert:  {len(deduped)} Zeilen")
    print(f"Entfernt:      {removed} Duplikate")
    print(f"Gespeichert:   {out}")


if __name__ == "__main__":
    main()