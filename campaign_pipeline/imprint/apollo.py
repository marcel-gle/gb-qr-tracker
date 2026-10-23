"""Apollo.io People Enrichment for managing-director phone numbers.

Two-stage, poll-based flow so a phone reveal (8 credits per mobile number) is
only requested for a verified person:

1. ``match_person``: People Enrichment without ``reveal_phone_number``.
2. ``verify_match``: accept only ``high``/``medium`` confidence matches whose
   current employer domain equals the row domain and whose name matches.
3. ``reveal_phone``: People Enrichment by Apollo person ``id`` with
   ``reveal_phone_number=true`` and ``poll_only=true`` (no public webhook).
4. ``poll_once``: read the result from ``/webhook_result/{request_id}``.

Soft-fails when ``APOLLO_API_KEY`` is missing or the API errors; the imprint
step never aborts because of Apollo.
"""

from __future__ import annotations

import logging
import os
import pickle
import re
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from ..models import normalize_domain

logger = logging.getLogger(__name__)

API_BASE = "https://api.apollo.io/api/v1"
MATCH_URL = f"{API_BASE}/people/match"
WEBHOOK_RESULT_URL = f"{API_BASE}/webhook_result"
REQUEST_TIMEOUT = 30
DEFAULT_POLL_TIMEOUT_S = 300
DEFAULT_POLL_INTERVAL_S = 5
MAX_RATE_LIMIT_RETRIES = 3
MAX_RATE_LIMIT_WAIT_S = 30
ENV_API_KEY = "APOLLO_API_KEY"

ACCEPTED_CONFIDENCE = frozenset({"high", "medium"})
# Preference order when a person has several valid numbers.
_PHONE_TYPE_RANK = {"mobile": 0, "work_direct": 1, "work_hq": 2}

REJECT_NO_MATCH = "no_match"
REJECT_LOW_CONFIDENCE = "low_confidence"
REJECT_DOMAIN_MISMATCH = "domain_mismatch"
REJECT_NOT_CURRENT = "not_current"
REJECT_NAME_MISMATCH = "name_mismatch"
POLL_TIMEOUT_REASON = "poll_timeout"


def get_api_key() -> Optional[str]:
    key = (os.environ.get(ENV_API_KEY) or "").strip()
    return key or None


# --------------------------------------------------------------------------- #
# Data containers
# --------------------------------------------------------------------------- #


@dataclass
class MatchResult:
    status: str  # matched | no_match | error
    person_id: Optional[str] = None
    match_confidence: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    organization_id: Optional[str] = None
    organization_domain: Optional[str] = None
    employment_history: List[Dict[str, Any]] = field(default_factory=list)
    person_details: Dict[str, Optional[str]] = field(default_factory=dict)
    org_details: Dict[str, Optional[str]] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class Verification:
    accepted: bool
    reason: Optional[str] = None


@dataclass
class RevealStart:
    status: str  # pending | error
    request_id: Optional[str] = None
    match: Optional[MatchResult] = None
    error: Optional[str] = None


@dataclass
class PhoneNumber:
    number: str
    phone_type: Optional[str]
    confidence: Optional[str]


@dataclass
class PollResult:
    status: str  # pending | ok | none | error
    phones: List[PhoneNumber] = field(default_factory=list)
    retry_after_seconds: Optional[float] = None
    error: Optional[str] = None

    @property
    def best(self) -> Optional[PhoneNumber]:
        return self.phones[0] if self.phones else None


# --------------------------------------------------------------------------- #
# Pure helpers (parsing + verification)
# --------------------------------------------------------------------------- #

_UMLAUTS = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"})


def normalize_name(value: Optional[str]) -> str:
    s = (value or "").strip().lower().translate(_UMLAUTS)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _first_token(value: Optional[str]) -> str:
    norm = normalize_name(value)
    return norm.split(" ", 1)[0] if norm else ""


_NO_EXTRA_REVEALS = {
    "reveal_personal_emails": "false",
    "run_waterfall_email": "false",
    "run_waterfall_phone": "false",
}


def _person_cache_key(person_id: str) -> str:
    return f"person:{person_id}"


def _cache_key(domain: str, first_name: str, last_name: str) -> str:
    return (
        f"{(domain or '').strip().lower()}|"
        f"{normalize_name(first_name)}|"
        f"{normalize_name(last_name)}"
    )


def parse_match_response(data: Any) -> MatchResult:
    if not isinstance(data, dict):
        return MatchResult(status="error", error="invalid_response")
    person = data.get("person")
    if not isinstance(person, dict) or not person.get("id"):
        return MatchResult(status="no_match")
    confidence = person.get("match_confidence") or data.get("match_confidence")
    organization = person.get("organization") if isinstance(person.get("organization"), dict) else {}
    org_domain = normalize_domain(
        organization.get("primary_domain") or organization.get("website_url")
    )
    history = person.get("employment_history")
    history_list = [h for h in history if isinstance(h, dict)] if isinstance(history, list) else []
    organization_id = str(person.get("organization_id") or organization.get("id") or "") or None
    return MatchResult(
        status="matched",
        person_id=str(person["id"]),
        match_confidence=str(confidence).lower() if confidence else None,
        first_name=person.get("first_name"),
        last_name=person.get("last_name"),
        organization_id=organization_id,
        organization_domain=org_domain,
        employment_history=history_list,
        person_details=extract_person_details(person, history_list, organization_id),
        org_details=extract_org_details(organization, org_domain),
    )


def _clean(value: Any, max_len: int = 500) -> Optional[str]:
    if value is None or isinstance(value, (dict, list)):
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text[:max_len] if text else None


def _join(values: Any, limit: int = 15) -> Optional[str]:
    if not isinstance(values, list):
        return None
    items = [_clean(v, 100) for v in values]
    items = [v.replace("_", " ") for v in items if v]
    return ", ".join(items[:limit]) or None


def extract_person_details(
    person: Dict[str, Any],
    history: List[Dict[str, Any]],
    organization_id: Optional[str],
) -> Dict[str, Optional[str]]:
    """Person fields included in the match response (no extra credits)."""
    current = next(
        (h for h in history if h.get("current") is True and organization_id
         and str(h.get("organization_id") or "") == organization_id),
        None,
    ) or next((h for h in history if h.get("current") is True), {})
    email = _clean(person.get("email"))
    # Apollo returns a placeholder like "email_not_unlocked@domain.com" when locked.
    if email and ("not_unlocked" in email.lower() or "@" not in email):
        email = None
    details = {
        "full_name": _clean(person.get("name")),
        "title": _clean(person.get("title")) or _clean(current.get("title")),
        "headline": _clean(person.get("headline")),
        "seniority": _clean(person.get("seniority")),
        "departments": _join(person.get("departments")),
        "position_start": _clean(current.get("start_date")),
        "email": email,
        "email_status": _clean(person.get("email_status")) if email else None,
        "linkedin_url": _clean(person.get("linkedin_url")),
        "twitter_url": _clean(person.get("twitter_url")),
        "facebook_url": _clean(person.get("facebook_url")),
        "city": _clean(person.get("city")),
        "state": _clean(person.get("state")),
        "country": _clean(person.get("country")),
    }
    return {k: v for k, v in details.items() if v}


def extract_org_details(organization: Dict[str, Any], org_domain: Optional[str]) -> Dict[str, Optional[str]]:
    """Organization fields included in the match response (no extra credits)."""
    if not organization:
        return {}
    primary_phone = organization.get("primary_phone") if isinstance(organization.get("primary_phone"), dict) else {}
    details = {
        "id": _clean(organization.get("id")),
        "name": _clean(organization.get("name")),
        "domain": org_domain,
        "phone": _clean(primary_phone.get("sanitized_number") or primary_phone.get("number"))
        or _clean(organization.get("sanitized_phone") or organization.get("phone")),
        "linkedin_url": _clean(organization.get("linkedin_url")),
        "industry": _clean(organization.get("industry")),
        "keywords": _join(organization.get("keywords")),
        "employees": _clean(organization.get("estimated_num_employees")),
        "annual_revenue": _clean(organization.get("annual_revenue_printed")),
        "founded_year": _clean(organization.get("founded_year")),
        "street": _clean(organization.get("street_address")),
        "postcode": _clean(organization.get("postal_code")),
        "city": _clean(organization.get("city")),
        "country": _clean(organization.get("country")),
        "description": _clean(organization.get("short_description") or organization.get("seo_description"), 1000),
        "technologies": _join(organization.get("technology_names"), limit=25),
    }
    return {k: v for k, v in details.items() if v}


def verify_match(match: MatchResult, domain: str, first_name: str, last_name: str) -> Verification:
    """Accept only a confident match whose *current* employer is the row's company."""
    if match.status != "matched" or not match.person_id:
        return Verification(False, REJECT_NO_MATCH)

    if (match.match_confidence or "") not in ACCEPTED_CONFIDENCE:
        return Verification(False, REJECT_LOW_CONFIDENCE)

    row_domain = normalize_domain(domain)
    if not row_domain or not match.organization_domain or match.organization_domain != row_domain:
        return Verification(False, REJECT_DOMAIN_MISMATCH)

    # `domain` also matches previous employers, so require a current position
    # at the matched organization whenever Apollo returns an employment history.
    if match.employment_history:
        current_here = any(
            h.get("current") is True
            and match.organization_id
            and str(h.get("organization_id") or "") == match.organization_id
            for h in match.employment_history
        )
        if not current_here:
            return Verification(False, REJECT_NOT_CURRENT)

    if normalize_name(match.last_name) != normalize_name(last_name):
        return Verification(False, REJECT_NAME_MISMATCH)
    if _first_token(match.first_name) != _first_token(first_name):
        return Verification(False, REJECT_NAME_MISMATCH)

    return Verification(True)


def parse_phone_numbers(phone_numbers: Any) -> List[PhoneNumber]:
    """Valid numbers only, best first (high confidence, then mobile > direct > hq)."""
    if not isinstance(phone_numbers, list):
        return []
    found: List[Tuple[Tuple[int, int, int], PhoneNumber]] = []
    seen: set[str] = set()
    for position, item in enumerate(phone_numbers):
        if not isinstance(item, dict):
            continue
        status_cd = item.get("status_cd")
        if status_cd and str(status_cd).lower() != "valid_number":
            continue
        number = item.get("sanitized_number") or item.get("raw_number")
        if not number:
            continue
        number = str(number).strip()
        if number in seen:
            continue
        seen.add(number)
        phone_type = str(item["type_cd"]).strip() if item.get("type_cd") else None
        confidence = str(item["confidence_cd"]).strip().lower() if item.get("confidence_cd") else None
        rank = (
            0 if confidence == "high" else 1,
            _PHONE_TYPE_RANK.get((phone_type or "").lower(), 9),
            position,
        )
        found.append((rank, PhoneNumber(number, phone_type, confidence)))
    found.sort(key=lambda item: item[0])
    return [phone for _, phone in found]


def parse_poll_people(payload: Any, person_id: Optional[str]) -> List[PhoneNumber]:
    """Phones for ``person_id`` from a poll payload; other people are ignored."""
    if not isinstance(payload, dict):
        return []
    container = payload.get("webhook_result") if isinstance(payload.get("webhook_result"), dict) else payload
    people = container.get("people")
    if not isinstance(people, list):
        return []
    for person in people:
        if not isinstance(person, dict):
            continue
        pid = person.get("id")
        if person_id and pid and str(pid) != str(person_id):
            continue
        phones = parse_phone_numbers(person.get("phone_numbers"))
        if phones:
            return phones
    return []


# --------------------------------------------------------------------------- #
# HTTP client with persistent per-person cache
# --------------------------------------------------------------------------- #


class ApolloPhoneClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        cache_path: Optional[Path] = None,
        session: Optional[requests.Session] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._api_key = (api_key if api_key is not None else get_api_key()) or None
        self._cache_path = cache_path
        self._session = session or requests.Session()
        self._sleep = sleep_fn
        self._lock = Lock()
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._fatal_error: Optional[str] = None
        if cache_path and cache_path.exists():
            try:
                with cache_path.open("rb") as f:
                    data = pickle.load(f)
                    if isinstance(data, dict):
                        self._cache = data
            except Exception as exc:
                logger.warning("Failed to load Apollo phone cache: %s", exc)

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key)

    @property
    def fatal_error(self) -> Optional[str]:
        """Set after an auth/credit error; all further calls are skipped."""
        return self._fatal_error

    def _blocked(self) -> Optional[str]:
        if not self._api_key:
            return "missing_api_key"
        if self._fatal_error:
            return f"aborted_{self._fatal_error}"
        return None

    def save_cache(self) -> None:
        if not self._cache_path:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        with self._cache_path.open("wb") as f:
            pickle.dump(self._cache, f)

    def cached(self, domain: str, first_name: str, last_name: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            hit = self._cache.get(_cache_key(domain, first_name, last_name))
        return dict(hit) if hit else None

    def remember(self, domain: str, first_name: str, last_name: str, **entry: Any) -> None:
        self._merge(_cache_key(domain, first_name, last_name), entry)

    def cached_person(self, person_id: Optional[str]) -> Optional[Dict[str, Any]]:
        if not person_id:
            return None
        with self._lock:
            hit = self._cache.get(_person_cache_key(person_id))
        return dict(hit) if hit else None

    def remember_person(self, person_id: Optional[str], /, **entry: Any) -> None:
        if person_id:
            self._merge(_person_cache_key(person_id), entry)

    def _merge(self, key: str, entry: Dict[str, Any]) -> None:
        with self._lock:
            merged = dict(self._cache.get(key) or {})
            merged.update(entry)
            self._cache[key] = merged

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "accept": "application/json",
            "x-api-key": self._api_key or "",
        }

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            resp = self._session.request(
                method, url, headers=self._headers(), timeout=REQUEST_TIMEOUT, **kwargs
            )
            if _is_fatal_response(resp):
                self._fatal_error = f"http_{resp.status_code}"
                logger.error(
                    "Apollo HTTP %s (%s) — stopping all further Apollo requests for this run.",
                    resp.status_code, (resp.text or "")[:300],
                )
                return resp
            if resp.status_code != 429 or attempt == MAX_RATE_LIMIT_RETRIES:
                return resp
            wait = _retry_after_seconds(resp) or 2 ** (attempt + 1)
            logger.info("Apollo rate limit hit; retrying in %.0fs", wait)
            self._sleep(min(float(wait), MAX_RATE_LIMIT_WAIT_S))
        return resp

    def match_person(
        self,
        first_name: str,
        last_name: str,
        domain: str,
        *,
        organization_name: Optional[str] = None,
        linkedin_url: Optional[str] = None,
    ) -> MatchResult:
        blocked = self._blocked()
        if blocked:
            return MatchResult(status="error", error=blocked)
        params: Dict[str, str] = {
            "first_name": first_name.strip(),
            "last_name": last_name.strip(),
            "domain": domain.strip().lower(),
            **_NO_EXTRA_REVEALS,
            "reveal_phone_number": "false",
        }
        if organization_name and organization_name.strip():
            params["organization_name"] = organization_name.strip()
        if linkedin_url and linkedin_url.strip():
            params["linkedin_url"] = linkedin_url.strip()
        try:
            resp = self._request("POST", MATCH_URL, params=params)
        except requests.RequestException as exc:
            logger.warning("Apollo match failed for %s %s @ %s: %s", first_name, last_name, domain, exc)
            return MatchResult(status="error", error=str(exc))
        if resp.status_code >= 400:
            logger.warning(
                "Apollo match HTTP %s for %s %s @ %s: %s",
                resp.status_code, first_name, last_name, domain, (resp.text or "")[:300],
            )
            return MatchResult(status="error", error=f"http_{resp.status_code}")
        try:
            return parse_match_response(resp.json())
        except ValueError:
            return MatchResult(status="error", error="invalid_json")

    def reveal_phone(self, person_id: str) -> RevealStart:
        blocked = self._blocked()
        if blocked:
            return RevealStart(status="error", error=blocked)
        params = {
            "id": person_id,
            **_NO_EXTRA_REVEALS,
            "reveal_phone_number": "true",
            "poll_only": "true",
        }
        try:
            resp = self._request("POST", MATCH_URL, params=params)
        except requests.RequestException as exc:
            return RevealStart(status="error", error=str(exc))
        if resp.status_code >= 400:
            logger.warning("Apollo reveal HTTP %s for %s: %s", resp.status_code, person_id, (resp.text or "")[:300])
            return RevealStart(status="error", error=f"http_{resp.status_code}")
        try:
            data = resp.json()
        except ValueError:
            return RevealStart(status="error", error="invalid_json")
        request_id = data.get("request_id") if isinstance(data, dict) else None
        if request_id is None:
            return RevealStart(status="error", error="missing_request_id")
        match = parse_match_response(data)
        return RevealStart(
            status="pending",
            request_id=str(request_id),
            match=match if match.status == "matched" and match.person_id == str(person_id) else None,
        )

    def poll_once(self, request_id: str, person_id: Optional[str] = None) -> PollResult:
        blocked = self._blocked()
        if blocked:
            return PollResult(status="error", error=blocked)
        rid = str(request_id).strip()
        if not rid:
            return PollResult(status="error", error="empty_request_id")
        # Polling is free and the reveal is already paid: transient failures
        # must stay pending, otherwise a later run would pay for a new reveal.
        try:
            resp = self._request("GET", f"{WEBHOOK_RESULT_URL}/{rid}")
        except requests.RequestException as exc:
            logger.info("Apollo poll %s transient error: %s", rid, exc)
            return PollResult(status="pending", retry_after_seconds=DEFAULT_POLL_INTERVAL_S)
        if resp.status_code == 429 or resp.status_code >= 500:
            return PollResult(status="pending", retry_after_seconds=DEFAULT_POLL_INTERVAL_S)
        try:
            data = resp.json() if resp.content else {}
        except ValueError:
            data = {}

        if resp.status_code == 200:
            phones = parse_poll_people(data, person_id)
            return PollResult(status="ok", phones=phones) if phones else PollResult(status="none")

        error_code = str(
            (data.get("error_code") if isinstance(data, dict) else "")
            or ((data.get("error_details") or {}).get("code") if isinstance(data, dict) else "")
            or ""
        ).lower()
        if resp.status_code == 404 and "pending" in error_code:
            retry = data.get("retry_after_seconds") if isinstance(data, dict) else None
            try:
                retry_f = float(retry) if retry is not None else DEFAULT_POLL_INTERVAL_S
            except (TypeError, ValueError):
                retry_f = DEFAULT_POLL_INTERVAL_S
            return PollResult(status="pending", retry_after_seconds=retry_f)
        return PollResult(status="error", error=error_code or f"http_{resp.status_code}")


def _is_fatal_response(resp: requests.Response) -> bool:
    """Invalid key, missing permission or no credits left: every further call would fail too."""
    if resp.status_code in (401, 402, 403):
        return True
    if resp.status_code == 422:
        text = (resp.text or "").lower()
        return "credit" in text or "quota" in text
    return False


def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
    try:
        header = (resp.headers or {}).get("Retry-After")
        if header:
            return float(header)
    except (TypeError, ValueError, AttributeError):
        pass
    try:
        body = resp.json()
        if isinstance(body, dict) and body.get("retry_after_seconds") is not None:
            return float(body["retry_after_seconds"])
    except (ValueError, TypeError):
        pass
    return None


# --------------------------------------------------------------------------- #
# Row orchestration
# --------------------------------------------------------------------------- #


def _empty_slot() -> Dict[str, Optional[str]]:
    return {
        "phone": None,
        "phones_all": None,
        "phone_type": None,
        "status": None,
        "reject_reason": None,
        "match_confidence": None,
        "person_id": None,
        "request_id": None,
    }


def ensure_apollo_slot(row_apollo: List[Dict[str, Optional[str]]], index: int) -> Dict[str, Optional[str]]:
    while len(row_apollo) <= index:
        row_apollo.append(_empty_slot())
    if row_apollo[index] is None:
        row_apollo[index] = _empty_slot()
    return row_apollo[index]


def _apply_phones(slot: Dict[str, Optional[str]], phones: List[PhoneNumber]) -> None:
    if phones:
        slot["status"] = "ok"
        slot["phone"] = phones[0].number
        slot["phone_type"] = phones[0].phone_type
        slot["phones_all"] = ", ".join(p.number for p in phones)
    else:
        slot["status"] = "none"
        slot["phone"] = None
        slot["phone_type"] = None
        slot["phones_all"] = None
    slot["reject_reason"] = None


def _named_directors(row: Any, max_directors: int) -> List[Tuple[int, str, str, Optional[str]]]:
    from ..models import parse_managing_director_field

    out: List[Tuple[int, str, str, Optional[str]]] = []
    for i, d in enumerate((row.directors or [])[:max_directors]):
        first = str((d or {}).get("first_name") or "").strip()
        last = str((d or {}).get("last_name") or "").strip()
        if not (first and last):
            parsed = parse_managing_director_field(
                (d or {}).get("imprint_managing_director"), max_directors=1
            )
            if parsed:
                first, last, _display = parsed[0]
                d["first_name"] = d.get("first_name") or first
                d["last_name"] = d.get("last_name") or last
        if first and last:
            out.append((i, first, last, (d or {}).get("linkedin_profile_url")))
    # Scored CSVs may only have Ocean "Imprint: Managing director" in extra.
    if not out and getattr(row, "extra", None):
        ocean = None
        for key, val in (row.extra or {}).items():
            if str(key).lower() in {
                "imprint: managing director",
                "imprint_managing_director",
                "managing director",
            }:
                ocean = val
                break
        for i, (first, last, display) in enumerate(
            parse_managing_director_field(ocean, max_directors=max_directors)
        ):
            while len(row.directors) <= i:
                row.directors.append({})
            d = row.directors[i]
            d["first_name"] = d.get("first_name") or first
            d["last_name"] = d.get("last_name") or last
            d["imprint_managing_director"] = d.get("imprint_managing_director") or display
            out.append((i, first, last, d.get("linkedin_profile_url")))
    return out


def count_apollo_candidates(rows: List[Any], *, max_directors: int = 3) -> int:
    return sum(len(_named_directors(row, max_directors)) for row in rows)


_DETAIL_KEYS = (
    "full_name", "title", "headline", "seniority", "departments", "position_start", "email",
    "email_status", "linkedin_url", "twitter_url", "facebook_url", "city", "state", "country",
)
_RESULT_KEYS = (
    "status", "phone", "phones_all", "phone_type", "reject_reason", "match_confidence", "person_id", "request_id",
    *_DETAIL_KEYS,
)
_FINAL_STATUSES = ("ok", "none", "rejected")


def _copy_result(slot: Dict[str, Optional[str]], entry: Dict[str, Any]) -> None:
    for key in _RESULT_KEYS:
        if key in entry:
            slot[key] = entry.get(key)


def _fill_details(
    row: Any,
    slot: Dict[str, Optional[str]],
    details: Optional[Dict[str, Any]],
    org: Optional[Dict[str, Any]],
) -> None:
    """Fill empty person/org fields; never overwrite values already present."""
    for key, value in (details or {}).items():
        if key in _DETAIL_KEYS and value and not slot.get(key):
            slot[key] = value
    if org:
        current = getattr(row, "apollo_org", None) or {}
        row.apollo_org = {**org, **{k: v for k, v in current.items() if v}}


def _details_of(slot: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    return {k: slot.get(k) for k in _DETAIL_KEYS if slot.get(k)}


def match_and_reveal_for_row(
    row: Any,
    client: ApolloPhoneClient,
    *,
    max_directors: int = 3,
) -> Dict[str, int]:
    """Match, verify and (only if verified) start a phone reveal per director.

    Paid calls are made at most once per person: final results, rejections and
    outstanding reveals are reused from the slot, the (domain, name) cache and
    the Apollo person-id cache before anything is sent to Apollo.
    """
    stats = {"matched": 0, "rejected": 0, "revealed": 0, "resumed": 0, "cached": 0, "error": 0}
    rejected_by_reason: Dict[str, int] = {}
    if row.apollo is None:
        row.apollo = []
    organization_name = getattr(row, "legal_name", None) or getattr(row, "company_name", None)

    for i, first, last, linkedin in _named_directors(row, max_directors):
        slot = ensure_apollo_slot(row.apollo, i)

        if slot.get("status") == "pending" and slot.get("request_id"):
            stats["resumed"] += 1
            continue
        if slot.get("status") in _FINAL_STATUSES:
            stats["cached"] += 1
            continue

        hit = client.cached(row.domain, first, last) or {}
        if hit.get("status") in _FINAL_STATUSES:
            _copy_result(slot, hit)
            if hit.get("status") != "rejected":
                _fill_details(row, slot, None, hit.get("org"))
            stats["cached"] += 1
            continue
        if hit.get("status") == "verified" and hit.get("request_id"):
            _copy_result(slot, {**hit, "status": "pending", "reject_reason": None})
            _fill_details(row, slot, None, hit.get("org"))
            stats["resumed"] += 1
            continue

        person_id = hit.get("person_id") if hit.get("status") == "verified" else None
        confidence = hit.get("match_confidence")
        if person_id:
            _fill_details(row, slot, hit, hit.get("org"))
        if not person_id:
            if client.fatal_error:
                slot.update({"status": "error", "reject_reason": f"aborted_{client.fatal_error}"})
                stats["error"] += 1
                continue
            match = client.match_person(
                first, last, row.domain, organization_name=organization_name, linkedin_url=linkedin
            )
            if match.status == "error":
                slot.update({"status": "error", "reject_reason": match.error})
                stats["error"] += 1
                continue
            verdict = verify_match(match, row.domain, first, last)
            if not verdict.accepted:
                reason = verdict.reason or "unknown"
                # Details of a rejected match may belong to someone else: never stored.
                result = {
                    "status": "rejected", "reject_reason": reason, "phone": None, "phones_all": None,
                    "phone_type": None, "match_confidence": match.match_confidence,
                    "person_id": match.person_id, "request_id": None,
                    **{k: None for k in _DETAIL_KEYS},
                }
                _copy_result(slot, result)
                client.remember(row.domain, first, last, **result)
                stats["rejected"] += 1
                rejected_by_reason[reason] = rejected_by_reason.get(reason, 0) + 1
                continue
            person_id = match.person_id
            confidence = match.match_confidence
            _fill_details(row, slot, match.person_details, match.org_details)
            client.remember(
                row.domain, first, last,
                status="verified", person_id=person_id, match_confidence=confidence,
                org=match.org_details, **match.person_details,
            )
            client.remember_person(person_id, org=match.org_details, **match.person_details)
            stats["matched"] += 1

        slot["person_id"] = person_id
        slot["match_confidence"] = confidence

        # Same Apollo person already revealed via another domain/row.
        known = client.cached_person(person_id) or {}
        known_org = known.get("org") or {}
        same_company = normalize_domain(known_org.get("domain")) == normalize_domain(row.domain)
        _fill_details(row, slot, known, known_org if same_company else None)
        if known.get("status") in ("ok", "none"):
            _copy_result(slot, {
                "status": known["status"], "phone": known.get("phone"), "phones_all": known.get("phones_all"),
                "phone_type": known.get("phone_type"), "request_id": known.get("request_id"),
                "reject_reason": None,
            })
            client.remember(row.domain, first, last, **{k: slot.get(k) for k in _RESULT_KEYS})
            stats["cached"] += 1
            continue
        if known.get("request_id"):
            slot.update({"status": "pending", "request_id": known["request_id"], "reject_reason": None})
            client.remember(row.domain, first, last, status="verified", request_id=known["request_id"])
            stats["resumed"] += 1
            continue

        reveal = client.reveal_phone(str(person_id))
        if reveal.status != "pending":
            slot.update({"status": "error", "reject_reason": reveal.error})
            stats["error"] += 1
            continue
        slot.update({"status": "pending", "request_id": reveal.request_id, "reject_reason": None})
        org = getattr(row, "apollo_org", None) or {}
        if reveal.match:
            _fill_details(row, slot, reveal.match.person_details, reveal.match.org_details)
            org = row.apollo_org
        client.remember(
            row.domain, first, last, status="verified", request_id=reveal.request_id,
            org=org, **_details_of(slot),
        )
        client.remember_person(person_id, request_id=reveal.request_id, org=org, **_details_of(slot))
        stats["revealed"] += 1

    for reason, count in rejected_by_reason.items():
        stats[f"rejected_{reason}"] = count
    return stats


def poll_apollo_pending(
    rows: List[Any],
    client: ApolloPhoneClient,
    *,
    max_directors: int = 3,
    timeout_s: float = DEFAULT_POLL_TIMEOUT_S,
    sleep_fn: Callable[[float], None] = time.sleep,
    progress_callback: Callable[[int, int, float], None] | None = None,
) -> Dict[str, int]:
    """Poll outstanding reveals (free). Timed-out slots stay ``pending`` for the next run.

    Each request id is polled once per round even if several slots share it.
    """
    stats = {"ok": 0, "none": 0, "error": 0, "timeout": 0}
    started = time.monotonic()

    def _pending() -> Dict[str, List[Tuple[Any, Dict[str, Optional[str]], str, str]]]:
        by_rid: Dict[str, List[Tuple[Any, Dict[str, Optional[str]], str, str]]] = {}
        for row in rows:
            directors = row.directors or []
            for i, slot in enumerate((row.apollo or [])[:max_directors]):
                if not slot or slot.get("status") != "pending" or not slot.get("request_id"):
                    continue
                d = directors[i] if i < len(directors) else {}
                by_rid.setdefault(str(slot["request_id"]), []).append(
                    (row, slot, str((d or {}).get("first_name") or ""), str((d or {}).get("last_name") or ""))
                )
        return by_rid

    initial_slots = sum(len(v) for v in _pending().values())
    # Progress reports (checked, total_rids) within each poll round so UIs update
    # per HTTP call instead of freezing until the whole round finishes.
    while True:
        pending = _pending()
        if not pending:
            if progress_callback is not None and initial_slots:
                progress_callback(initial_slots, max(initial_slots, 1), time.monotonic() - started)
            break
        # Budget at least ~2s per outstanding request-id so large batches (500+)
        # are not marked timed-out mid-first-round. Negative timeout_s = expire now (tests).
        if float(timeout_s) < 0:
            deadline = started + float(timeout_s)
        else:
            deadline = started + max(float(timeout_s), len(pending) * 2.0 + 60.0)
        if client.fatal_error or time.monotonic() > deadline:
            for items in pending.values():
                for _row, slot, _first, _last in items:
                    slot["reject_reason"] = POLL_TIMEOUT_REASON
                    stats["timeout"] += 1
            if progress_callback is not None:
                resolved = initial_slots - sum(len(v) for v in _pending().values())
                progress_callback(max(resolved, 0), max(initial_slots, 1), time.monotonic() - started)
            break

        rid_items = list(pending.items())
        round_total = len(rid_items)
        next_sleep = float(DEFAULT_POLL_INTERVAL_S)
        for idx, (rid, items) in enumerate(rid_items, start=1):
            if progress_callback is not None:
                progress_callback(idx - 1, max(round_total, 1), time.monotonic() - started)
            result = client.poll_once(rid, items[0][1].get("person_id"))
            if result.status == "pending":
                if result.retry_after_seconds:
                    next_sleep = min(next_sleep, max(1.0, float(result.retry_after_seconds)))
            elif client.fatal_error:
                if progress_callback is not None:
                    progress_callback(idx, max(round_total, 1), time.monotonic() - started)
                break
            else:
                for row, slot, first, last in items:
                    if result.status in ("ok", "none"):
                        _apply_phones(slot, result.phones)
                        stats[result.status] += 1
                        client.remember(row.domain, first, last, **{k: slot.get(k) for k in _RESULT_KEYS})
                        client.remember_person(slot.get("person_id"), **{k: slot.get(k) for k in _RESULT_KEYS})
                    else:
                        # Request id expired/unknown: allow exactly one new reveal on a later run.
                        slot.update({"status": "error", "reject_reason": result.error, "request_id": None})
                        stats["error"] += 1
                        client.remember(row.domain, first, last, status="verified", request_id=None)
                        client.remember_person(slot.get("person_id"), request_id=None)
            if progress_callback is not None:
                progress_callback(idx, max(round_total, 1), time.monotonic() - started)

        if _pending() and not client.fatal_error:
            sleep_fn(next_sleep)

    return stats

