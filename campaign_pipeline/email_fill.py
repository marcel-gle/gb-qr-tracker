"""Fill empty ``BusinessRow.email`` from the best available alternate source.

Preference (highest first):
1. Personal Apollo emails that match a director name (verified preferred)
2. Other personal-looking emails matching a director name
3. Non-generic company emails (Imprint / Generic Company Emails / …)
4. Generic mailboxes (info@, kontakt@, …) as last resort

Datenschutz mailboxes (datenschutz@, dsb@, …) are never used.
Existing ``email`` values are never overwritten.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple

_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
_PRIVACY_LOCAL_RE = re.compile(
    r"^(?:datenschutz|dsb|privacy|dataprotection|data-protection|gdpr|dsgvo)\b",
    re.IGNORECASE,
)
_GENERIC_LOCALS = frozenset(
    {
        "info",
        "kontakt",
        "contact",
        "office",
        "mail",
        "email",
        "e-mail",
        "hello",
        "hallo",
        "service",
        "support",
        "admin",
        "webmaster",
        "post",
        "poststelle",
        "buero",
        "büro",
        "reception",
        "team",
        "sales",
        "vertrieb",
        "anfrage",
        "anfragen",
        "newsletter",
        "noreply",
        "no-reply",
        "donotreply",
    }
)
_SOURCE_KEY_HINTS = (
    "email",
    "e-mail",
    "e_mail",
    "mail",
)


def _fold(text: str) -> str:
    raw = (text or "").strip().lower()
    raw = raw.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    decomposed = unicodedata.normalize("NFKD", raw)
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def _is_valid_email(value: str) -> bool:
    return bool(_EMAIL_RE.fullmatch(value.strip()))


def _local_part(email: str) -> str:
    return email.split("@", 1)[0].strip().lower()


def _is_privacy(email: str) -> bool:
    return bool(_PRIVACY_LOCAL_RE.match(_local_part(email)))


def _is_generic(email: str) -> bool:
    local = _local_part(email)
    base = re.split(r"[+._\-]", local)[0]
    return base in _GENERIC_LOCALS or local in _GENERIC_LOCALS


def _director_names(row: Any) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for d in getattr(row, "directors", None) or []:
        first = _fold(str((d or {}).get("first_name") or ""))
        last = _fold(str((d or {}).get("last_name") or ""))
        # Drop titles leftover in tokens.
        first = re.sub(r"[^a-z]", "", first.split()[0] if first.split() else "")
        last = re.sub(r"[^a-z]", "", last.split()[-1] if last.split() else last)
        if first or last:
            out.append((first, last))
    return out


def _name_match_score(email: str, directors: List[Tuple[str, str]]) -> int:
    """0 = no name match; 1 = first only; 2 = last only; 3 = first+last."""
    local = _fold(_local_part(email))
    local_compact = re.sub(r"[^a-z0-9]", "", local)
    best = 0
    for first, last in directors:
        if not first and not last:
            continue
        has_first = bool(first) and (first in local or first in local_compact)
        has_last = bool(last) and len(last) >= 2 and (last in local or last in local_compact)
        if has_first and has_last:
            best = max(best, 3)
        elif has_last:
            best = max(best, 2)
        elif has_first and len(first) >= 3:
            best = max(best, 1)
    return best


def _score_candidate(
    email: str,
    *,
    directors: List[Tuple[str, str]],
    source: str,
    apollo_verified: bool = False,
) -> int:
    if not _is_valid_email(email) or _is_privacy(email):
        return -1
    name_hit = _name_match_score(email, directors)
    generic = _is_generic(email)
    score = 0
    if name_hit == 3:
        score = 300
    elif name_hit == 2:
        score = 220
    elif name_hit == 1:
        score = 160
    elif not generic:
        score = 80
    else:
        score = 20
    if apollo_verified:
        score += 40
    elif source == "apollo":
        score += 20
    elif source == "imprint":
        score += 5
    return score


def _extract_emails_from_value(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        found: List[str] = []
        for item in value:
            found.extend(_extract_emails_from_value(item))
        return found
    text = str(value).strip()
    if not text:
        return []
    return [m.group(0) for m in _EMAIL_RE.finditer(text)]


def _looks_like_email_column(key: str) -> bool:
    k = _fold(key).replace(" ", "")
    if "datenschutz" in k or k.startswith("apollo_email_status"):
        return False
    return any(hint in k for hint in _SOURCE_KEY_HINTS)


def collect_email_candidates(
    row: Any,
    *,
    extra_candidates: Optional[Iterable[str]] = None,
) -> List[Tuple[str, str, bool]]:
    """Return (email, source, apollo_verified) candidates from a business row."""
    found: List[Tuple[str, str, bool]] = []
    seen: set[str] = set()

    def add(email: str, source: str, verified: bool = False) -> None:
        key = email.strip().lower()
        if not key or key in seen:
            return
        seen.add(key)
        found.append((email.strip(), source, verified))

    for slot in getattr(row, "apollo", None) or []:
        email = str((slot or {}).get("email") or "").strip()
        if not email:
            continue
        status = str((slot or {}).get("email_status") or "").strip().lower()
        add(email, "apollo", verified=(status == "verified"))

    extra = getattr(row, "extra", None) or {}
    for key, value in extra.items():
        if not isinstance(key, str) or not _looks_like_email_column(key):
            continue
        source = "imprint" if "imprint" in key.lower() else "other"
        for email in _extract_emails_from_value(value):
            add(email, source)

    for email in extra_candidates or []:
        for extracted in _extract_emails_from_value(email):
            add(extracted, "imprint")

    return found


def best_email_for_row(
    row: Any,
    *,
    extra_candidates: Optional[Iterable[str]] = None,
) -> Optional[str]:
    directors = _director_names(row)
    ranked: List[Tuple[int, str]] = []
    for email, source, verified in collect_email_candidates(
        row, extra_candidates=extra_candidates
    ):
        score = _score_candidate(
            email, directors=directors, source=source, apollo_verified=verified
        )
        if score >= 0:
            ranked.append((score, email))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (-item[0], item[1].lower()))
    return ranked[0][1]


def fill_row_email(
    row: Any,
    *,
    extra_candidates: Optional[Iterable[str]] = None,
) -> bool:
    """Fill ``row.email`` when empty. Returns True if a value was written."""
    current = str(getattr(row, "email", None) or "").strip()
    if current:
        return False
    chosen = best_email_for_row(row, extra_candidates=extra_candidates)
    if not chosen:
        return False
    row.email = chosen
    return True


def fill_rows_email(rows: Iterable[Any]) -> int:
    return sum(1 for row in rows if fill_row_email(row))
