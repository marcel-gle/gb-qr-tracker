from __future__ import annotations

from typing import Dict, Mapping, Optional

from ..models import DOMAIN_COLUMN_ALIASES, normalize_domain


def map_row_to_canonical(row: Mapping[str, object]) -> Dict[str, object]:
    canonical: Dict[str, object] = {}
    for raw_key, value in row.items():
        if raw_key is None:
            continue
        key_norm = str(raw_key).strip().lower()
        if isinstance(value, str):
            value = value.strip()

        if key_norm in DOMAIN_COLUMN_ALIASES or key_norm == "domain":
            canonical["domain"] = value
        elif key_norm in {"company", "firma", "unternehmen", "company_name", "name"}:
            canonical["company_name"] = value
        elif "branchencode" in key_norm or "nace" in key_norm:
            canonical["branchencode"] = value
        elif "gegenstand" in key_norm:
            canonical["gegenstand"] = value
        elif "mail" in key_norm:
            canonical["email"] = value
        elif "telefon" in key_norm or key_norm in {"tel", "tel.", "phone"}:
            canonical["phone"] = value
        elif key_norm in {"straße", "strasse", "street"}:
            canonical["street"] = value
        elif key_norm in {"hausnummer", "nr", "no.", "house number"}:
            canonical["house_number"] = value
        elif key_norm in {"plz", "postcode", "zip"}:
            canonical["postcode"] = value
        elif key_norm in {"ort", "city", "stadt"}:
            canonical["city"] = value
        else:
            canonical[str(raw_key)] = value

    domain = normalize_domain(canonical.get("domain"), email_fallback=canonical.get("email"))
    if domain:
        canonical["domain"] = domain
    return canonical


def guess_domain_from_row(row: Mapping[str, object]) -> Optional[str]:
    canonical = map_row_to_canonical(row)
    domain = canonical.get("domain")
    return str(domain) if domain else None
