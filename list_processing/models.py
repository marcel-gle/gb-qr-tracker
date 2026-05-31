from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
import re
from typing import Any, Dict, List, Optional, Set


#: Minimum required logical fields for a record to be processable.
MIN_FIELDS: Set[str] = {"company_name", "website"}

#: Ideal / target set of logical fields we try to populate.
IDEAL_FIELDS: Set[str] = {
    "branchencode",
    "gegenstand",
    "first_name",
    "last_name",
    "email",
    "phone",
}


def normalize_postcode(postcode: object | None) -> Optional[str]:
    """
    Normalize German postcodes for internal use.

    - Strip a leading \"D-\" or \"D \" prefix (case-insensitive), e.g. \"D-12345\" -> \"12345\".
    - If the postcode has only 4 digits, pad with a leading zero, e.g. \"1234\" -> \"01234\".
    - Leave other formats unchanged.
    """
    if postcode is None:
        return None

    s = str(postcode).strip()
    if not s:
        return None

    # Remove leading country prefix variants like \"D-12345\" or \"d 12345\".
    upper = s.upper()
    if upper.startswith("D-"):
        s = s[2:].strip()
    elif upper.startswith("D "):
        s = s[2:].strip()

    # Apply 4-digit padding rule only for purely numeric postcodes.
    if re.fullmatch(r"\d{4}", s):
        s = "0" + s

    return s


def _sanitize_analysis_key(value: str) -> str:
    key = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip()).strip("_").lower()
    return key or "value"


def flatten_analysis_result(result: Any, prefix: str = "analysis") -> Dict[str, Any]:
    flat: Dict[str, Any] = {}

    def _walk(value: Any, path: List[str]) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                _walk(child, path + [_sanitize_analysis_key(str(key))])
            return

        flat_key = prefix if not path else f"{prefix}_{'_'.join(path)}"
        if isinstance(value, list):
            flat[flat_key] = json.dumps(value, ensure_ascii=False)
            return

        flat[flat_key] = "" if value is None else value

    if isinstance(result, dict):
        _walk(result, [])

    return flat


@dataclass
class PromptUsage:
    """
    Lightweight record of which prompt/model was used for a given step.

    This is intentionally generic so it can track both local and remote LLM
    usage without coupling the core model to a specific client implementation.
    """

    step: str
    prompt_name: str
    version: str
    backend: Optional[str] = None
    model: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LeadRecord:
    """
    Canonical representation of a business/contact row used throughout the
    list_processing pipeline.
    """

    # Core identity (minimum required)
    company_name: str
    website: str

    # Optional / ideal input fields
    branchencode: Optional[str] = None
    gegenstand: Optional[str] = None
    umsatz: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None

    # Raw representatives from source list (e.g. \"Ges. Vertreter 1/2/3\" from Northdata-style CSVs)
    rep1_raw: Optional[str] = None
    rep2_raw: Optional[str] = None
    rep3_raw: Optional[str] = None

    # Address
    street: Optional[str] = None
    house_number: Optional[str] = None
    postcode: Optional[str] = None
    city: Optional[str] = None
    raw_address: Optional[str] = None

    # Imprint / managing director information (normalized fields used across pipeline)
    managing_director_full: Optional[str] = None
    managing_director_first: Optional[str] = None
    managing_director_last: Optional[str] = None
    salutation: Optional[str] = None
    legal_name: Optional[str] = None

    # Raw imprint-derived fields (for tracing exactly what came from the imprint)
    imprint_address: Optional[str] = None
    imprint_managing_director_1: Optional[str] = None
    imprint_managing_director_2: Optional[str] = None
    imprint_managing_director_3: Optional[str] = None

    # Scoring / analysis
    domain_match_score: Optional[int] = None
    domain_analysis_raw: Optional[Dict[str, Any]] = None

    # Prompt / traceability metadata
    prompts_used: List[PromptUsage] = field(default_factory=list)

    # Arbitrary extra metadata we might want to preserve from the source row
    # (e.g. IDs, flags). This is not interpreted by the core pipeline.
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_canonical_dict(cls, data: Dict[str, Any]) -> "LeadRecord":
        """
        Construct a LeadRecord from an already-normalized dictionary.

        This is primarily used by the IO layer after applying an InputMapper.
        """
        missing = {field for field in MIN_FIELDS if not data.get(field)}
        if missing:
            raise ValueError(f"Missing required fields for LeadRecord: {sorted(missing)}")

        # Heuristic normalization at load-time:
        # 1) If we have a street that still contains a trailing house number but no
        #    separate house_number field yet, try to split it.
        street_val = (data.get("street") or "").strip()
        house_val = (data.get("house_number") or "").strip()
        if street_val and not house_val:
            m = re.match(r"^(.*?)(\s+\d[0-9A-Za-z\/\- ]*)$", street_val)
            if m:
                street_name = m.group(1).strip(" ,")
                house_raw = m.group(2).strip()
                if street_name:
                    data["street"] = street_name
                if house_raw:
                    data["house_number"] = house_raw

        # 2) Normalize postcode formats (e.g. \"D-12345\" -> \"12345\", \"1234\" -> \"01234\").
        if "postcode" in data and data.get("postcode") is not None:
            normalized_pc = normalize_postcode(data.get("postcode"))
            if normalized_pc is not None:
                data["postcode"] = normalized_pc

        # Only pass known fields to the dataclass; everything else goes into metadata.
        known_keys = {
            "company_name",
            "website",
            "branchencode",
            "gegenstand",
            "umsatz",
            "first_name",
            "last_name",
            "email",
            "phone",
            "rep1_raw",
            "rep2_raw",
            "rep3_raw",
            "street",
            "house_number",
            "postcode",
            "city",
            "raw_address",
            "managing_director_full",
            "managing_director_first",
            "managing_director_last",
            "salutation",
            "legal_name",
            "imprint_address",
            "imprint_managing_director_1",
            "imprint_managing_director_2",
            "imprint_managing_director_3",
            "domain_match_score",
            "domain_analysis_raw",
        }

        init_kwargs: Dict[str, Any] = {k: data.get(k) for k in known_keys if k in data}

        # Metadata keeps any extra keys from the canonical dict.
        metadata = {k: v for k, v in data.items() if k not in known_keys}
        init_kwargs.setdefault("metadata", {})
        init_kwargs["metadata"].update(metadata)

        return cls(**init_kwargs)

    def to_internal_dict(self) -> Dict[str, Any]:
        """
        Convert the record to a flat dictionary suitable for CSV/JSON output
        in the \"internal_enriched\" schema.

        This is intentionally verbose so that downstream consumers can easily
        inspect and debug pipeline results.
        """
        data = asdict(self)

        # prompts_used is a list of dataclasses; turn them into plain dicts.
        data["prompts_used"] = [asdict(p) for p in self.prompts_used]

        if isinstance(self.domain_analysis_raw, dict):
            analysis_json = json.dumps(self.domain_analysis_raw, ensure_ascii=False)
            data["domain_analysis_raw"] = analysis_json
            data["analysis_result"] = analysis_json
            data.update(flatten_analysis_result(self.domain_analysis_raw))
        else:
            data["analysis_result"] = ""

        # Provide a flat alias for the domain match score so downstream
        # consumers can rely on a simple "match_score" column in CSV exports.
        if "domain_match_score" in data and "match_score" not in data:
            data["match_score"] = data["domain_match_score"]

        return data

