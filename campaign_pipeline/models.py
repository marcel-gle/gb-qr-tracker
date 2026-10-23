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


# Ocean.io / lettershop: "Imprint: Managing director" = "Heike Fiebes" or
# "Christopher Ahrens, Tristan Peveling".
_MD_FIELD_ALIASES = (
    "Imprint: Managing director",
    "imprint_managing_director",
    "managing_director",
    "Managing director",
)
_MD_SPLIT_RE = re.compile(r"\s*(?:,|;|/|\bund\b|&)\s*", re.IGNORECASE)
_MD_TITLE_RE = re.compile(
    r"^(?:herr|frau|hr\.?|fr\.?|dr\.?|prof\.?|dipl\.?-?\w*|ing\.?|mag\.?|"
    r"mba|msc|bsc|ll\.?m\.?)\s+",
    re.IGNORECASE,
)


def _get_ci_value(data: Dict[str, Any], *names: str) -> Optional[str]:
    lower_map = {str(k).lower(): k for k in data.keys() if isinstance(k, str)}
    for name in names:
        key = lower_map.get(name.lower())
        if key is None:
            continue
        val = data.get(key)
        if val is None:
            continue
        text = str(val).strip()
        if text:
            return text
    return None


def split_person_name(full: str) -> Optional[tuple[str, str]]:
    """Split a single person display name into (first_name, last_name)."""
    s = (full or "").strip()
    if not s:
        return None
    while True:
        nxt = _MD_TITLE_RE.sub("", s, count=1).strip(" ,")
        if nxt == s:
            break
        s = nxt
    if not s:
        return None
    # Northdata-style "Nachname, Vorname" (left side has no spaces).
    if "," in s:
        left, right = [p.strip() for p in s.split(",", 1)]
        if left and right and " " not in left and " " in right:
            return right, left
        if left and right and " " not in left and " " not in right:
            return right, left
    tokens = s.split()
    if len(tokens) < 2:
        return None
    return " ".join(tokens[:-1]), tokens[-1]


def parse_managing_director_field(
    raw: object | None,
    *,
    max_directors: int = MAX_DIRECTORS_DEFAULT,
) -> List[tuple[str, str, str]]:
    """Parse Ocean/imprint MD string into up to N (first, last, display) triples."""
    text = str(raw or "").strip()
    if not text:
        return []
    parts = [p.strip() for p in _MD_SPLIT_RE.split(text) if p and p.strip()]
    if not parts:
        parts = [text]
    out: List[tuple[str, str, str]] = []
    for part in parts:
        split = split_person_name(part)
        if not split:
            continue
        first, last = split
        out.append((first, last, part))
        if len(out) >= max_directors:
            break
    return out


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


# Apollo slot key -> CSV column prefix (column = f"{prefix}_{i}").
APOLLO_SLOT_COLUMNS: Dict[str, str] = {
    "phone": "apollo_phone",
    "phone_type": "apollo_phone_type",
    "phones_all": "apollo_phones_all",
    # Person details from the (already paid) match response; verified matches only.
    "full_name": "apollo_full_name",
    "title": "apollo_title",
    "headline": "apollo_headline",
    "seniority": "apollo_seniority",
    "departments": "apollo_departments",
    "position_start": "apollo_position_start",
    "email": "apollo_email",
    "email_status": "apollo_email_status",
    "linkedin_url": "apollo_linkedin_url",
    "twitter_url": "apollo_twitter_url",
    "facebook_url": "apollo_facebook_url",
    "city": "apollo_city",
    "state": "apollo_state",
    "country": "apollo_country",
    "status": "apollo_status",
    "reject_reason": "apollo_reject_reason",
    "match_confidence": "apollo_match_confidence",
    "person_id": "apollo_person_id",
    "request_id": "apollo_request_id",
}

# Apollo organization key -> CSV column (one set per row).
APOLLO_ORG_COLUMNS: Dict[str, str] = {
    "name": "apollo_org_name",
    "domain": "apollo_org_domain",
    "phone": "apollo_org_phone",
    "linkedin_url": "apollo_org_linkedin_url",
    "industry": "apollo_org_industry",
    "keywords": "apollo_org_keywords",
    "employees": "apollo_org_employees",
    "annual_revenue": "apollo_org_annual_revenue",
    "founded_year": "apollo_org_founded_year",
    "street": "apollo_org_street",
    "postcode": "apollo_org_postcode",
    "city": "apollo_org_city",
    "country": "apollo_org_country",
    "description": "apollo_org_description",
    "technologies": "apollo_org_technologies",
    "id": "apollo_org_id",
}

# Letter body text. First non-empty alias wins.
PARAGRAPH_FIELD_ALIASES = ("Paragraph", "paragraph", "absatz", "Absatz")

# Subsets exported to {base}_final.csv.
APOLLO_FINAL_SLOT_KEYS = (
    "phone", "phone_type", "phones_all", "title", "email", "email_status", "linkedin_url",
)
APOLLO_FINAL_ORG_KEYS = ("phone",)


def apollo_field_names(max_directors: int = MAX_DIRECTORS_DEFAULT) -> List[str]:
    return [
        f"{prefix}_{i}"
        for i in range(1, max_directors + 1)
        for prefix in APOLLO_SLOT_COLUMNS.values()
    ]


def apollo_org_field_names() -> List[str]:
    return list(APOLLO_ORG_COLUMNS.values())


def final_csv_fieldnames(max_directors: int = MAX_DIRECTORS_DEFAULT) -> List[str]:
    """Column order for {base}_final.csv (letter-ready export)."""
    fields = [
        "company_name",
        "full_address",
        "street",
        "house_number",
        "postcode",
        "city",
    ]
    for i in range(1, max_directors + 1):
        fields.extend([f"first_name_{i}", f"last_name_{i}", f"salutation_{i}"])
    fields.extend(["email", "phone", "input_phone", "imprint_phone"])
    fields.extend(APOLLO_ORG_COLUMNS[key] for key in APOLLO_FINAL_ORG_KEYS)
    for i in range(1, max_directors + 1):
        fields.extend(f"{APOLLO_SLOT_COLUMNS[key]}_{i}" for key in APOLLO_FINAL_SLOT_KEYS)
    for i in range(1, max_directors + 1):
        fields.append(f"linkedin_profile_url_{i}")
    fields.append("Paragraph")
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
    input_phone: Optional[str] = None
    imprint_phone: Optional[str] = None
    legal_name: Optional[str] = None

    match_score: Optional[float] = None
    score_raw: Optional[float] = None
    score_scale: Optional[str] = None
    score_field: Optional[str] = None
    passed_score_filter: Optional[bool] = None
    domain_analysis_raw: Optional[Dict[str, Any]] = None

    template: Optional[str] = None
    paragraph: Optional[str] = None

    source_file: Optional[str] = None
    source_row: Optional[int] = None
    gegenstand: Optional[str] = None
    branchencode: Optional[str] = None

    directors: List[Dict[str, Optional[str]]] = field(default_factory=list)
    # Per director, keys from APOLLO_SLOT_COLUMNS; status: pending|ok|none|rejected|error
    apollo: List[Dict[str, Optional[str]]] = field(default_factory=list)
    # Keys from APOLLO_ORG_COLUMNS, taken from the first verified Apollo match of the row.
    apollo_org: Dict[str, Optional[str]] = field(default_factory=dict)
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
        apollo: List[Dict[str, Optional[str]]] = []
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

            apollo.append(
                {key: (data.get(f"{prefix}_{i}") or None) for key, prefix in APOLLO_SLOT_COLUMNS.items()}
            )
        while len(apollo) > 1 and not any(apollo[-1].values()):
            apollo.pop()

        # _scored / Ocean.io often only has "Imprint: Managing director" (full names).
        # Fill empty first_name/last_name so Apollo and later steps can use them.
        ocean_md = _get_ci_value(data, *_MD_FIELD_ALIASES)
        parsed_mds = parse_managing_director_field(ocean_md, max_directors=max_directors)
        if not parsed_mds:
            # Fall back to per-slot imprint_managing_director_* already loaded.
            for i, d in enumerate(directors[:max_directors]):
                if (d.get("first_name") or "").strip() and (d.get("last_name") or "").strip():
                    continue
                one = parse_managing_director_field(
                    d.get("imprint_managing_director"), max_directors=1
                )
                if one:
                    parsed_mds.append(one[0])
        if parsed_mds:
            while len(directors) < len(parsed_mds):
                directors.append({})
            for i, (first, last, display) in enumerate(parsed_mds):
                d = directors[i]
                if not (d.get("first_name") or "").strip():
                    d["first_name"] = first
                if not (d.get("last_name") or "").strip():
                    d["last_name"] = last
                if not (d.get("imprint_managing_director") or "").strip():
                    d["imprint_managing_director"] = display

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
            "input_phone",
            "imprint_phone",
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
        known.update(apollo_field_names(max_directors))
        known.update(apollo_org_field_names())
        known.update(_MD_FIELD_ALIASES)
        known.update(PARAGRAPH_FIELD_ALIASES)
        paragraph_alias_keys = {a.lower() for a in PARAGRAPH_FIELD_ALIASES}
        # Case variants of Ocean MD column already in the row.
        for k in list(data.keys()):
            if not isinstance(k, str):
                continue
            if k.lower() in {a.lower() for a in _MD_FIELD_ALIASES} or k.lower() in paragraph_alias_keys:
                known.add(k)
        apollo_org = {key: (data.get(col) or None) for key, col in APOLLO_ORG_COLUMNS.items()}

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
            input_phone=data.get("input_phone"),
            imprint_phone=data.get("imprint_phone"),
            legal_name=data.get("legal_name"),
            match_score=_float_or_none(data.get("match_score")),
            score_raw=_float_or_none(data.get("score_raw")),
            score_scale=data.get("score_scale"),
            score_field=data.get("score_field"),
            passed_score_filter=_bool_or_none(data.get("passed_score_filter")),
            domain_analysis_raw=analysis_raw if isinstance(analysis_raw, dict) else None,
            template=data.get("template") or data.get("Template"),
            paragraph=_get_ci_value(data, *PARAGRAPH_FIELD_ALIASES),
            source_file=data.get("source_file"),
            source_row=_int_or_none(data.get("source_row")),
            gegenstand=data.get("gegenstand") or data.get("Gegenstand"),
            branchencode=data.get("branchencode") or data.get("Branchencode WZ"),
            directors=directors or [{}],
            apollo=apollo or [{}],
            apollo_org={k: v for k, v in apollo_org.items() if v},
            extra=extra,
        )

    def to_final_dict(self, max_directors: int = MAX_DIRECTORS_DEFAULT) -> Dict[str, Any]:
        full = self.to_dict(max_directors=max_directors)
        out: Dict[str, Any] = {}
        for key in final_csv_fieldnames(max_directors):
            if key == "Template":
                out[key] = full.get("template") or ""
            elif key == "Paragraph":
                out[key] = full.get("paragraph") or ""
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
            "input_phone": self.input_phone or "",
            "imprint_phone": self.imprint_phone or "",
            "legal_name": self.legal_name or "",
            "match_score": "" if self.match_score is None else self.match_score,
            "score_raw": "" if self.score_raw is None else self.score_raw,
            "score_scale": self.score_scale or "",
            "score_field": self.score_field or "",
            "passed_score_filter": "" if self.passed_score_filter is None else self.passed_score_filter,
            "template": self.template or "",
            "paragraph": self.paragraph or "",
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

            a = self.apollo[i - 1] if i - 1 < len(self.apollo) else {}
            for key, prefix in APOLLO_SLOT_COLUMNS.items():
                data[f"{prefix}_{i}"] = (a or {}).get(key) or ""

        for key, col in APOLLO_ORG_COLUMNS.items():
            data[col] = (self.apollo_org or {}).get(key) or ""

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
