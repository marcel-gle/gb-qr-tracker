"""
Map enriched lead CSV into WZ address template format.

Input columns (excerpt/relevant):
- Firma, Website, Domain, Domain public, Branche, ...
- Anrede (AP 1), Vorname (AP 1), Nachname (AP 1), E-Mail (AP 1)
- E-Mail, E-Mail (Import)
- Telefonnummer
- Bundesland/Kanton, Postleitzahl, Stadt, Straße
- companyId

Output columns:
- Adress-ID, Anrede, Namenszeile, Namenszeile 1, Namenszeile 2, Namenszeile 3
- PLZ, Ort, Ortsteil, Straße, Hausnummer
- Branchencode WZ, Branchenname WZ, Dachmarkt WZ, Bundesland
- Entscheider 1 Anrede/Titel/Vorname/Nachname/Funktionsnummer/Funktionsname
- vorwahl_telefon, telefonnummer, e-mail-adresse
- template, tracking_link, Domain

Usage:
    python scripts/map_to_wz_template.py input.csv output.csv
"""

import csv
import re
import sys
from pathlib import Path


INPUT_EMAIL_PRIORITY = ["E-Mail (AP 1)", "E-Mail (Import)", "E-Mail"]

OUTPUT_HEADERS = [
    "Adress-ID",
    "Anrede",
    "Namenszeile",
    "Namenszeile 1",
    "Namenszeile 2",
    "Namenszeile 3",
    "PLZ",
    "Ort",
    "Ortsteil",
    "Straße",
    "Hausnummer",
    "Branchencode WZ",
    "Branchenname WZ",
    "Dachmarkt WZ",
    "Bundesland",
    "Entscheider 1 Anrede",
    "Entscheider 1 Titel",
    "Entscheider 1 Vorname",
    "Entscheider 1 Nachname",
    "Entscheider 1 Funktionsnummer",
    "Entscheider 1 Funktionsname",
    "vorwahl_telefon",
    "telefonnummer",
    "e-mail-adresse",
    "template",
    "tracking_link",
    "Domain",
]


def pick_email(row: dict) -> str:
    """Pick the best available email according to the defined priority."""
    for key in INPUT_EMAIL_PRIORITY:
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def pick_domain(row: dict) -> str:
    """Pick primary domain, fall back to public domain if needed."""
    domain = (row.get("Domain") or "").strip()
    if domain:
        return domain
    return (row.get("Domain public") or "").strip()


def parse_street_and_number(raw_street: str) -> tuple[str, str]:
    """
    Split a full street string into street name and house number.

    Examples:
        "Musterstraße 12"        -> ("Musterstraße", "12")
        "Musterstr. 12a"         -> ("Musterstr.", "12a")
        "Am Bach 3-5"            -> ("Am Bach", "3-5")
        "Hauptstraße 10 b"       -> ("Hauptstraße", "10 b")

    If no number is found, the whole string is returned as street and house number is empty.
    """
    text = (raw_street or "").strip()
    if not text:
        return "", ""

    # Look for last block that starts with a digit (house number and suffixes)
    m = re.search(r"\s+(\d.*)$", text)
    if not m:
        # No number found
        return text, ""

    house = m.group(1).strip()
    street = text[: m.start(1)].strip()

    if not street:
        # Fallback: don't lose the information
        return text, ""

    return street, house


def map_row(row: dict) -> dict:
    """Map a single input row to the WZ template format."""
    # Normalize PLZ to 5 digits, preserving/adding leading zeros
    raw_plz = (row.get("Postleitzahl") or "").strip()
    digits_only = re.sub(r"[^\d]", "", raw_plz)
    if digits_only:
        if len(digits_only) < 5:
            normalized_plz = digits_only.zfill(5)
        else:
            normalized_plz = digits_only[:5]
    else:
        normalized_plz = ""

    full_street = (row.get("Straße") or "").strip()
    street, house_number = parse_street_and_number(full_street)

    return {
        # Leave Adress-ID empty (will be filled later in the target system)
        "Adress-ID": "",
        "Anrede": (row.get("Anrede (AP 1)") or "").strip(),
        "Namenszeile": (row.get("Firma") or "").strip(),
        "Namenszeile 1": (row.get("Firma") or "").strip(),
        "Namenszeile 2": "",
        "Namenszeile 3": "",
        "PLZ": normalized_plz,
        "Ort": (row.get("Stadt") or "").strip(),
        "Ortsteil": "",
        "Straße": street,
        "Hausnummer": house_number,
        "Branchencode WZ": "",
        "Branchenname WZ": (row.get("Branche") or "").strip(),
        "Dachmarkt WZ": "",
        "Bundesland": (row.get("Bundesland/Kanton") or "").strip(),
        "Entscheider 1 Anrede": (row.get("Anrede (AP 1)") or "").strip(),
        "Entscheider 1 Titel": "",
        "Entscheider 1 Vorname": (row.get("Vorname (AP 1)") or "").strip(),
        "Entscheider 1 Nachname": (row.get("Nachname (AP 1)") or "").strip(),
        "Entscheider 1 Funktionsnummer": "",
        "Entscheider 1 Funktionsname": "",
        "vorwahl_telefon": "",
        "telefonnummer": (row.get("Telefonnummer") or "").strip(),
        "e-mail-adresse": pick_email(row),
        "template": "",
        # Leave tracking_link empty; tracking is handled elsewhere
        "tracking_link": "",
        "Domain": pick_domain(row),
    }


def detect_delimiter(file_path: Path) -> str:
    """Detect CSV delimiter by reading first line (comma vs semicolon)."""
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        first_line = f.readline()
        comma_count = first_line.count(",")
        semicolon_count = first_line.count(";")
        return ";" if semicolon_count >= comma_count else ","


def convert_csv(input_path: Path, output_path: Path) -> None:
    """Convert input CSV to WZ template CSV."""
    delimiter = detect_delimiter(input_path)

    with input_path.open("r", encoding="utf-8-sig", newline="") as f_in, output_path.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f_out:
        reader = csv.DictReader(f_in, delimiter=delimiter)
        writer = csv.DictWriter(f_out, fieldnames=OUTPUT_HEADERS, delimiter=delimiter)

        writer.writeheader()
        for row in reader:
            writer.writerow(map_row(row))


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("Usage: python scripts/map_to_wz_template.py input.csv output.csv")
        return 1

    input_path = Path(argv[1])
    output_path = Path(argv[2])

    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_path}")
        return 1

    try:
        convert_csv(input_path, output_path)
        print(f"Done. Mapped CSV written to: {output_path}")
        return 0
    except Exception as e:
        print(f"Error converting CSV: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

