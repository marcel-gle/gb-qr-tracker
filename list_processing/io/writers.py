from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, List, Dict, Any

from ..models import LeadRecord


def write_internal_csv(path: Path, records: Iterable[LeadRecord]) -> None:
    """
    Write records in the \"internal_enriched\" schema.

    This flattens each LeadRecord via `to_internal_dict` and writes a header
    that is the union of all keys across records.
    """
    rows: List[Dict[str, Any]] = [r.to_internal_dict() for r in records]
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    # Build a stable header order, giving priority to the core business columns
    # required by downstream consumers. Any additional fields are appended
    # afterwards in a deterministic order.
    preferred_order: List[str] = [
        "company_name",
        "salutation",
        "first_name",
        "last_name",
        "rep1_raw",
        "imprint_address",
        "street",
        "house_number",
        "postcode",
        "city",
        "email",
        "phone",
        "website",
        "umsatz",
        "gegenstand",
        "match_score",
    ]

    # First collect all keys across all rows in a stable order based on
    # first appearance.
    all_keys: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in all_keys:
                all_keys.append(key)

    # Start with the preferred columns that actually exist, then append
    # any remaining keys.
    fieldnames: List[str] = [k for k in preferred_order if k in all_keys]
    fieldnames.extend(k for k in all_keys if k not in fieldnames)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def write_lettershop_csv(path: Path, records: Iterable[LeadRecord]) -> None:
    """
    Write records in the lettershop-style schema currently produced by
    `transform_csv_to_new_format` in the legacy pipeline.
    """
    fieldnames = [
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
        # Imprint-specific address columns (separate from normalized address)
        "Impressum-Adresse",
        "Impressum-Straße",
        "Impressum-Hausnummer",
        "Impressum-PLZ",
        "Impressum-Ort",
        "Branchencode WZ",
        # Additional business fields to feed upload_processor business docs
        "Gegenstand",
        "Umsatz EUR",
        "Branche (NACE)",
        "Branchenname WZ",
        "Dachmarkt WZ",
        "Bundesland",
        "Entscheider 1 Anrede",
        "Entscheider 1 Titel",
        "Entscheider 1 Vorname",
        "Entscheider 1 Nachname",
        "Entscheider 1 Funktionsnummer",
        "Entscheider 1 Funktionsname",
        # Imprint-specific managing directors (raw strings from imprint)
        "Impressum-Geschaeftsfuehrer 1",
        "Impressum-Geschaeftsfuehrer 2",
        "Impressum-Geschaeftsfuehrer 3",
        "vorwahl_telefon",
        "telefonnummer",
        "e-mail-adresse",
        "template",
        "tracking_link",
        "Domain",
        "Umsatz"
    ]

    def _record_to_row(record: LeadRecord, idx: int) -> Dict[str, Any]:
        # Adress-ID: leave empty for now (can be filled by downstream systems)
        namenszeile = record.legal_name or record.company_name

        # Prefer imprint-derived address for the explicit imprint columns,
        # but keep the normalized address columns as-is for downstream use.
        imprint_full = record.imprint_address or ""
        imprint_street = record.street or ""
        imprint_house_number = record.house_number or ""
        imprint_postcode = record.postcode or ""
        imprint_city = record.city or ""

        return {
            "Adress-ID": "",
            "Anrede": record.salutation or "",
            "Namenszeile": namenszeile,
            "Namenszeile 1": namenszeile,
            "Namenszeile 2": "",
            "Namenszeile 3": "",
            "PLZ": record.postcode or "",
            "Ort": record.city or "",
            "Ortsteil": "",
            "Straße": record.street or "",
            "Hausnummer": record.house_number or "",
            "Impressum-Adresse": imprint_full,
            "Impressum-Straße": imprint_street,
            "Impressum-Hausnummer": imprint_house_number,
            "Impressum-PLZ": imprint_postcode,
            "Impressum-Ort": imprint_city,
            "Branchencode WZ": record.branchencode or "",
            "Gegenstand": record.gegenstand or "",
            "Umsatz EUR": record.umsatz or "",
            "Branche (NACE)": record.branchencode or "",
            "Branchenname WZ": "",
            "Dachmarkt WZ": "",
            "Bundesland": "",
            "Entscheider 1 Anrede": record.salutation or "",
            "Entscheider 1 Titel": "",
            "Entscheider 1 Vorname": record.first_name or "",
            "Entscheider 1 Nachname": record.last_name or "",
            "Entscheider 1 Funktionsnummer": "",
            "Entscheider 1 Funktionsname": (
                "Geschäftsführer/in" if record.managing_director_full else ""
            ),
            "Impressum-Geschaeftsfuehrer 1": record.imprint_managing_director_1 or "",
            "Impressum-Geschaeftsfuehrer 2": record.imprint_managing_director_2 or "",
            "Impressum-Geschaeftsfuehrer 3": record.imprint_managing_director_3 or "",
            "vorwahl_telefon": "",
            "telefonnummer": record.phone or "",
            "e-mail-adresse": record.email or "",
            "template": "",
            "tracking_link": "",
            "Domain": record.website or "",
            "Umsatz": record.umsatz or "",
        }

    rows: List[Dict[str, Any]] = []
    for idx, record in enumerate(records, start=1):
        rows.append(_record_to_row(record, idx))

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

