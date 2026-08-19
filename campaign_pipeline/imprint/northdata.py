"""North Data lookup for current managing directors.

Some imprints only list a company's postal address (see e.g.
``tht-rheinland-logistik.de``), leaving the letter without a named recipient.
North Data publishes the current legal representatives in a JSON-LD
``LocalBusiness``/``Organization`` block on each company page, which is
available without a paid subscription. This module searches North Data for a
company and extracts those representatives.

The lookup is best-effort: any network/parse failure returns an empty list so
the imprint pipeline degrades gracefully.
"""

from __future__ import annotations

import json
import logging
import re
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://www.northdata.de"
SEARCH_URL = BASE_URL + "/search?query={query}"
REQUEST_TIMEOUT = 12
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

# North Data jobTitles that represent legal management (not auditors etc.).
_MANAGEMENT_TITLES = (
    "geschäftsführer",
    "geschäftsführerin",
    "geschaeftsfuehrer",
    "inhaber",
    "vorstand",
    "vorständin",
    "vorstandsvorsitzende",
    "persönlich haftende",
    "prokurist",
    "prokuristin",
    "komplementär",
    "geschäftsleitung",
    "director",
    "managing director",
    "owner",
)

# Suffixes stripped before comparing company names.
_LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:gmbh|ug|ag|kg|ohg|gbr|mbh|mbb|se|e\.?\s?k\.?|e\.?\s?v\.?|"
    r"co\.?|kgaa|haftungsbeschränkt|und|&|\+)\b|\(.*?\)",
    re.IGNORECASE,
)
_COMPANY_LINK_RE = re.compile(r'href="(/[^"]*?/(?:Amtsgericht[^"]*)?HR[AB][^"]*)"')


def _normalize_company(name: str) -> str:
    cleaned = _LEGAL_SUFFIX_RE.sub(" ", (name or "").lower())
    cleaned = re.sub(r"[^a-z0-9äöüß ]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _name_tokens(name: str) -> set[str]:
    return {t for t in _normalize_company(name).split() if len(t) > 1}


def _names_match(query_name: str, candidate_url: str) -> bool:
    """Heuristic: require meaningful token overlap between query and candidate."""
    q = _name_tokens(query_name)
    if not q:
        return False
    # Candidate company name is the first path segment of the North Data URL.
    path = urlparse(unescape(candidate_url)).path.lstrip("/")
    candidate_name = path.split("/", 1)[0].replace("%20", " ")
    try:
        from urllib.parse import unquote

        candidate_name = unquote(candidate_name)
    except Exception:
        pass
    c = _name_tokens(candidate_name)
    if not c:
        return False
    overlap = q & c
    # Match when the query's distinctive tokens are largely present.
    return len(overlap) >= max(1, min(len(q), 2))


def _split_person(given: str, family: str, full: str) -> List[Dict[str, Optional[str]]]:
    """North Data sometimes merges joint entries ("Enrico und Rinaldo Kosse")."""
    given = (given or "").strip()
    family = (family or "").strip()
    full = (full or "").strip()

    firsts = [g for g in re.split(r"\s+und\s+|\s*&\s*|\s*/\s*", given) if g.strip()]
    if len(firsts) > 1 and family:
        return [
            {
                "first_name": f.strip(),
                "last_name": family,
                "gender": None,
                "full_name": f"{f.strip()} {family}".strip(),
            }
            for f in firsts
        ]
    if not given and not family and full:
        # North Data "name" is usually "Family, Given".
        if "," in full:
            family, _, given = (p.strip() for p in full.partition(","))
        else:
            parts = full.split()
            if len(parts) >= 2:
                family = parts[-1]
                given = " ".join(parts[:-1])
    display = " ".join(p for p in (given, family) if p) or full
    return [
        {
            "first_name": given or None,
            "last_name": family or None,
            "gender": None,
            "full_name": display or None,
        }
    ]


def _parse_members(html: str) -> List[Dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    members: List[Dict[str, Optional[str]]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in data if isinstance(data, list) else [data]:
            if not isinstance(obj, dict):
                continue
            member = obj.get("member")
            if isinstance(member, dict):
                member = [member]
            if not isinstance(member, list):
                continue
            for person in member:
                if not isinstance(person, dict):
                    continue
                if person.get("@type") not in ("Person", "http://schema.org/Person"):
                    continue
                title = str(person.get("jobTitle") or "").lower()
                if title and not any(t in title for t in _MANAGEMENT_TITLES):
                    continue
                members.extend(
                    _split_person(
                        str(person.get("givenName") or ""),
                        str(person.get("familyName") or ""),
                        str(person.get("name") or ""),
                    )
                )
    # De-duplicate on (first, last) preserving order.
    seen: set[tuple] = set()
    unique: List[Dict[str, Optional[str]]] = []
    for m in members:
        key = ((m.get("first_name") or "").lower(), (m.get("last_name") or "").lower())
        if key in seen or key == ("", ""):
            continue
        seen.add(key)
        unique.append(m)
    return unique


def _find_company_url(search_html: str, company_name: str) -> Optional[str]:
    for match in _COMPANY_LINK_RE.finditer(search_html):
        href = unescape(match.group(1))
        if _names_match(company_name, href):
            return urljoin(BASE_URL, href)
    # No confident name match: do not guess (avoid wrong-company data).
    return None


def lookup_managing_directors(
    company_name: str,
    city: Optional[str] = None,
    *,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Optional[str]]]:
    """Return current managing directors for a company from North Data.

    Returns an empty list when the company cannot be confidently matched or on
    any network/parse error.
    """
    company_name = (company_name or "").strip()
    if not company_name:
        return []

    sess = session or requests.Session()
    query = company_name
    if city:
        query = f"{company_name} {city}".strip()

    try:
        resp = sess.get(
            SEARCH_URL.format(query=quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return []
        company_url = _find_company_url(resp.text, company_name)
        if not company_url:
            logger.debug("North Data: no confident match for %r", query)
            return []
        page = sess.get(
            company_url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if page.status_code != 200:
            return []
        members = _parse_members(page.text)
        if members:
            logger.info(
                "North Data: %d representative(s) for %r via %s",
                len(members),
                company_name,
                company_url,
            )
        return members
    except requests.RequestException as exc:
        logger.debug("North Data lookup failed for %r: %s", query, exc)
        return []
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("North Data lookup error for %r: %s", query, exc)
        return []
