from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional

from ..models import MIN_FIELDS


@dataclass
class InputMapper:
    """
    Maps raw input row keys to canonical LeadRecord field names.

    The mapping is stored case-insensitively on the source side so that
    differences in header capitalization do not matter.
    """

    source_to_canonical: Dict[str, str]

    def __post_init__(self) -> None:
        # Normalize keys for case-insensitive lookup
        self._normalized: Dict[str, str] = {
            (k or "").strip().lower(): v for k, v in self.source_to_canonical.items()
        }

    def map_row(self, row: Mapping[str, object]) -> Dict[str, object]:
        """
        Convert a raw CSV/JSON row into a canonical dictionary suitable for
        constructing a LeadRecord.
        """
        canonical: Dict[str, object] = {}

        for raw_key, value in row.items():
            if raw_key is None:
                continue

            key_norm = str(raw_key).strip().lower()
            target = self._normalized.get(key_norm)
            if not target:
                continue

            # Basic string normalization; leave other types untouched.
            if isinstance(value, str):
                value = value.strip()

            canonical[target] = value

        return canonical


def guess_mapper_from_header(header: Iterable[str]) -> InputMapper:
    """
    Build a best-effort mapper from a CSV header.

    This is intentionally heuristic but covers the most common variations in
    your current scripts (Company/Domain, Branchencode WZ, etc.).
    """
    candidates: Dict[str, str] = {}

    for name in header:
        if not name:
            continue
        key_norm = name.strip().lower()

        # If the header already uses canonical field names, accept them directly.
        if key_norm == "company_name":
            candidates[name] = "company_name"
        elif key_norm == "website":
            candidates[name] = "website"
        elif key_norm in {"company", "firma", "unternehmen"}:
            candidates[name] = "company_name"
        elif key_norm in {"domain", "website", "webseite", "url"}:
            candidates[name] = "website"
        elif "branchencode" in key_norm or "nace" in key_norm:
            candidates[name] = "branchencode"
        elif "gegenstand" in key_norm:
            candidates[name] = "gegenstand"
        elif key_norm in {"umsatz", "umsatz eur"}:
            candidates[name] = "umsatz"
        elif key_norm in {"vorname", "first name", "firstname"}:
            candidates[name] = "first_name"
        elif key_norm in {"nachname", "last name", "lastname"}:
            candidates[name] = "last_name"
        elif "mail" in key_norm:
            candidates[name] = "email"
        elif (
            "telefon" in key_norm
            or "phone" in key_norm
            or key_norm in {"tel", "tel."}
        ):
            candidates[name] = "phone"
        elif "ges. vertreter 1" in key_norm:
            candidates[name] = "rep1_raw"
        elif "ges. vertreter 2" in key_norm:
            candidates[name] = "rep2_raw"
        elif "ges. vertreter 3" in key_norm:
            candidates[name] = "rep3_raw"
        elif key_norm in {"straße", "strasse", "street"}:
            candidates[name] = "street"
        elif key_norm in {"hausnummer", "nr", "no."}:
            candidates[name] = "house_number"
        elif key_norm in {"plz", "postcode", "zip"}:
            candidates[name] = "postcode"
        elif key_norm in {"ort", "city", "stadt"}:
            candidates[name] = "city"

    # Ensure minimum required fields are mapped if possible; callers can still
    # validate and fail fast if they are missing.
    return InputMapper(source_to_canonical=candidates)


_BUILTIN_MAPPERS: Dict[str, InputMapper] = {
    # Generic ocean.io / current CSV style: "Company" + "Domain"
    "default": InputMapper(
        {
            "Company": "company_name",
            "Domain": "website",
            "Branchencode WZ": "branchencode",
            "Branchencode": "branchencode",
            "Branche (NACE)": "branchencode",
            "Gegenstand": "gegenstand",
            "Umsatz": "umsatz",
            "Umsatz EUR": "umsatz",
            "Ges. Vertreter 1": "rep1_raw",
            "Ges. Vertreter 2": "rep2_raw",
            "Ges. Vertreter 3": "rep3_raw",
            "Vorname": "first_name",
            "Nachname": "last_name",
            "E-Mail": "email",
            "Email": "email",
            "Telefon": "phone",
        }
    )
}


def get_builtin_mapper(name: str = "default") -> InputMapper:
    """
    Return a named built-in mapper.

    Raises KeyError if the name is unknown.
    """
    try:
        return _BUILTIN_MAPPERS[name]
    except KeyError as exc:
        available: List[str] = sorted(_BUILTIN_MAPPERS.keys())
        raise KeyError(f"Unknown mapper '{name}'. Available: {available}") from exc

