"""
CSV: Zeilen aus einer Datei behalten, deren Domain in einer anderen nicht vorkommt
==================================================================================
Liest eine Referenz-CSV (bereits bekannte Domains) und eine zweite CSV; schreibt
nur Zeilen aus der zweiten Datei, deren Wert in der Domain-Spalte nicht in der
Referenz vorkommt (Vergleich normalisiert: strip + lower).

Nutzung:
    python brave_csv_subtract_domains.py referenz.csv neu.csv -o nur_neu.csv
    python brave_csv_subtract_domains.py ref.csv neu.csv --column display_url
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


def norm_domain(value: str) -> str:
    return (value or "").strip().lower()


def main() -> None:
    p = argparse.ArgumentParser(
        description=(
            "Schreibt nur Zeilen aus der zweiten CSV, deren Domain nicht in der "
            "ersten CSV vorkommt (;-delimited, UTF-8 BOM)."
        ),
    )
    p.add_argument(
        "reference",
        type=Path,
        help="CSV mit bereits bekannten Domains (werden ausgeschlossen)",
    )
    p.add_argument(
        "incoming",
        type=Path,
        help="CSV aus der nur nicht in der Referenz vorkommende Zeilen behalten werden",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Ausgabe-Pfad (default: <incoming>_minus_<reference_stem>.csv)",
    )
    p.add_argument(
        "--column",
        default="domain",
        help="Spaltenname für den Domain-Vergleich (default: domain)",
    )
    args = p.parse_args()

    for path, label in ((args.reference, "reference"), (args.incoming, "incoming")):
        if not path.exists():
            sys.exit(f"Fehler: {label}-Datei nicht gefunden: {path}")

    ref_domains: set[str] = set()
    with open(args.reference, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        if not reader.fieldnames:
            sys.exit("Referenz-CSV: keine Kopfzeile.")
        if args.column not in reader.fieldnames:
            sys.exit(
                f"Spalte '{args.column}' in Referenz nicht gefunden. "
                f"Verfügbar: {', '.join(reader.fieldnames)}"
            )
        for row in reader:
            d = norm_domain(row.get(args.column, "") or "")
            if d:
                ref_domains.add(d)

    out_rows: list[dict[str, str]] = []
    fieldnames: list[str] | None = None
    with open(args.incoming, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        fieldnames = reader.fieldnames
        if not fieldnames:
            sys.exit("Eingabe-CSV: keine Kopfzeile.")
        if args.column not in fieldnames:
            sys.exit(
                f"Spalte '{args.column}' in zweiter Datei nicht gefunden. "
                f"Verfügbar: {', '.join(fieldnames)}"
            )
        for row in reader:
            d = norm_domain(row.get(args.column, "") or "")
            if d not in ref_domains:
                out_rows.append(row)

    if args.output:
        out = args.output
    else:
        out = args.incoming.with_stem(
            f"{args.incoming.stem}_minus_{args.reference.stem}"
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in out_rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f"Referenz-Domains (nicht-leer): {len(ref_domains)}")
    print(f"Zeilen in zweiter Datei (nach Filter): {len(out_rows)}")
    print(f"Gespeichert: {out}")


if __name__ == "__main__":
    main()
