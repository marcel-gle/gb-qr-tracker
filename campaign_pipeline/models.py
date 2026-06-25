from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set


MAX_DIRECTORS_DEFAULT = 3

DOMAIN_COLUMN_ALIASES = {
    "domain",
    "website",
    "webseite",
    "url",
}


def normalize_domain(value: object | None, email_fallback: object | None = None) -> Optional[str]:
    if value is not None:
        raw = str(value).strip()
        if raw:
            if raw.lower().startswith("http://"):
                raw = raw[7:]
            elif raw.lower().startswith("https://"):
                raw = raw[8:]
            raw = raw.split("/")[0].strip().rstrip("/").lower()
            if raw.startswith("www."):
                raw = raw[4:]
            if raw and "." in raw:
                return raw

    if email_fallback is not None:
        email = str(email_fallback).strip()
        if email:
            candidate = email
            if candidate.lower().startswith("mailto:"):
                candidate = candidate[7:].strip()
            match = re.search(r"@([A-Z0-9.\-]+\.[A-Z]{2,})", candidate, flags=re.IGNORECASE)
            if match:
                return match.group(1).strip().strip(" >)\"'").lower().rstrip(".")
    return None


def normalize_postcode(postcode: object | None) -> Optional[str]:
    if postcode is None:
        return None
    s = str(postcode).strip()
    if not s:
        return None
    upper = s.upper()
    if upper.startswith("D-"):
        s = s[2:].strip()
    elif upper.startswith("D "):
        s = s[2:].strip()
    if re.fullmatch(r"\d{4}", s):
        s = "0" + s
    return s


def _sanitize_key(value: str) -> str:
    key = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip()).strip("_").lower()
    return key or "value"


def flatten_analysis(result: Any, prefix: str = "analysis") -> Dict[str, Any]:
    flat: Dict[str, Any] = {}

    def _walk(value: Any, path: List[str]) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                _walk(child, path + [_sanitize_key(str(key))])
            return
        flat_key = prefix if not path else f"{prefix}_{'_'.join(path)}"
        if isinstance(value, list):
            flat[flat_key] = json.dumps(value, ensure_ascii=False)
        else:
            flat[flat_key] = "" if value is None else value

    if isinstance(result, dict):
        _walk(result, [])
    return flat


def director_field_names(max_directors: int = MAX_DIRECTORS_DEFAULT) -> List[str]:
    names: List[str] = []
    for i in range(1, max_directors + 1):
        names.extend(
            [
                f"first_name_{i}",
                f"last_name_{i}",
                f"salutation_{i}",
                f"linkedin_profile_url_{i}",
                f"imprint_managing_director_{i}",
            ]
        )
    return names


def final_csv_fieldnames(max_directors: int = MAX_DIRECTORS_DEFAULT) -> List[str]:
    """Column order for {base}_final.csv (letter-ready export)."""
    fields = [
        "domain",
        "company_name",
        "full_address",
        "street",
        "house_number",
        "postcode",
        "city",
    ]
    for i in range(1, max_directors + 1):
        fields.extend([f"first_name_{i}", f"last_name_{i}", f"salutation_{i}"])
    fields.extend(["email", "phone"])
    for i in range(1, max_directors + 1):
        fields.append(f"linkedin_profile_url_{i}")
    fields.append("Template")
    return fields


@dataclass
class BusinessRow:
    domain: str

    company_name: Optional[str] = None
    full_address: Optional[str] = None
    street: Optional[str] = None
    house_number: Optional[str] = None
    postcode: Optional[str] = None
    city: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    legal_name: Optional[str] = None

    match_score: Optional[float] = None
    score_raw: Optional[float] = None
    score_scale: Optional[str] = None
    score_field: Optional[str] = None
    passed_score_filter: Optional[bool] = None
    domain_analysis_raw: Optional[Dict[str, Any]] = None

    template: Optional[str] = None

    source_file: Optional[str] = None
    source_row: Optional[int] = None
    gegenstand: Optional[str] = None
    branchencode: Optional[str] = None

    directors: List[Dict[str, Optional[str]]] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def has_score_result(self) -> bool:
        """True when this row already has LLM scoring output."""
        if isinstance(self.domain_analysis_raw, dict) and self.domain_analysis_raw:
            return True
        return self.match_score is not None

    @classmethod
    def from_dict(cls, data: Dict[str, Any], max_directors: int = MAX_DIRECTORS_DEFAULT) -> "BusinessRow":
        domain = normalize_domain(
            data.get("domain") or data.get("Domain") or data.get("website") or data.get("Website"),
            email_fallback=data.get("email") or data.get("E-Mail") or data.get("Email"),
        )
        if not domain:
            raise ValueError("Missing required field: domain")

        directors: List[Dict[str, Optional[str]]] = []
        for i in range(1, max_directors + 1):
            d = {
                "first_name": data.get(f"first_name_{i}"),
                "last_name": data.get(f"last_name_{i}"),
                "salutation": data.get(f"salutation_{i}"),
                "linkedin_profile_url": data.get(f"linkedin_profile_url_{i}"),
                "imprint_managing_director": data.get(f"imprint_managing_director_{i}"),
            }
            if any(v for v in d.values() if v):
                directors.append(d)
            elif i == 1 and not directors:
                directors.append(d)

        known: Set[str] = {
            "domain",
            "Domain",
            "website",
            "Website",
            "company_name",
            "Company",
            "full_address",
            "street",
            "house_number",
            "postcode",
            "city",
            "email",
            "phone",
            "legal_name",
            "match_score",
            "score_raw",
            "score_scale",
            "score_field",
            "passed_score_filter",
            "domain_analysis_raw",
            "analysis_result",
            "template",
            "Template",
            "source_file",
            "source_row",
            "gegenstand",
            "Gegenstand",
            "branchencode",
            "Branchencode WZ",
        }
        known.update(director_field_names(max_directors))

        extra = {k: v for k, v in data.items() if k not in known and not str(k).startswith("analysis_")}

        analysis_raw = data.get("domain_analysis_raw")
        if isinstance(analysis_raw, str) and analysis_raw.strip():
            try:
                analysis_raw = json.loads(analysis_raw)
            except json.JSONDecodeError:
                analysis_raw = None

        return cls(
            domain=domain,
            company_name=data.get("company_name") or data.get("Company"),
            full_address=data.get("full_address"),
            street=data.get("street"),
            house_number=data.get("house_number"),
            postcode=normalize_postcode(data.get("postcode")),
            city=data.get("city"),
            email=data.get("email"),
            phone=data.get("phone"),
            legal_name=data.get("legal_name"),
            match_score=_float_or_none(data.get("match_score")),
            score_raw=_float_or_none(data.get("score_raw")),
            score_scale=data.get("score_scale"),
            score_field=data.get("score_field"),
            passed_score_filter=_bool_or_none(data.get("passed_score_filter")),
            domain_analysis_raw=analysis_raw if isinstance(analysis_raw, dict) else None,
            template=data.get("template") or data.get("Template"),
            source_file=data.get("source_file"),
            source_row=_int_or_none(data.get("source_row")),
            gegenstand=data.get("gegenstand") or data.get("Gegenstand"),
            branchencode=data.get("branchencode") or data.get("Branchencode WZ"),
            directors=directors or [{}],
            extra=extra,
        )

    def to_final_dict(self, max_directors: int = MAX_DIRECTORS_DEFAULT) -> Dict[str, Any]:
        full = self.to_dict(max_directors=max_directors)
        out: Dict[str, Any] = {}
        for key in final_csv_fieldnames(max_directors):
            if key == "Template":
                out[key] = full.get("template") or ""
            else:
                out[key] = full.get(key, "")
        return out

    def to_dict(self, max_directors: int = MAX_DIRECTORS_DEFAULT) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "domain": self.domain,
            "company_name": self.company_name or "",
            "full_address": self.full_address or "",
            "street": self.street or "",
            "house_number": self.house_number or "",
            "postcode": self.postcode or "",
            "city": self.city or "",
            "email": self.email or "",
            "phone": self.phone or "",
            "legal_name": self.legal_name or "",
            "match_score": "" if self.match_score is None else self.match_score,
            "score_raw": "" if self.score_raw is None else self.score_raw,
            "score_scale": self.score_scale or "",
            "score_field": self.score_field or "",
            "passed_score_filter": "" if self.passed_score_filter is None else self.passed_score_filter,
            "template": self.template or "",
            "source_file": self.source_file or "",
            "source_row": "" if self.source_row is None else self.source_row,
            "gegenstand": self.gegenstand or "",
            "branchencode": self.branchencode or "",
        }
        for i in range(1, max_directors + 1):
            d = self.directors[i - 1] if i - 1 < len(self.directors) else {}
            data[f"first_name_{i}"] = (d or {}).get("first_name") or ""
            data[f"last_name_{i}"] = (d or {}).get("last_name") or ""
            data[f"salutation_{i}"] = (d or {}).get("salutation") or ""
            data[f"linkedin_profile_url_{i}"] = (d or {}).get("linkedin_profile_url") or ""
            data[f"imprint_managing_director_{i}"] = (d or {}).get("imprint_managing_director") or ""

        if isinstance(self.domain_analysis_raw, dict):
            data["domain_analysis_raw"] = json.dumps(self.domain_analysis_raw, ensure_ascii=False)
            data["analysis_result"] = data["domain_analysis_raw"]
            data.update(flatten_analysis(self.domain_analysis_raw))
        else:
            data["domain_analysis_raw"] = ""
            data["analysis_result"] = ""

        data.update(self.extra)
        return data


def _float_or_none(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None
