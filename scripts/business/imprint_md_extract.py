"""
Heuristics to extract managing director / legal representative names from German imprint text.

Handles common layouts where the label (e.g. "Geschäftsführer:") is on one line and the
name on the next, Yoast-style og:description blobs, and section headings in HTML.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

from bs4 import BeautifulSoup, NavigableString, Tag

LEGAL_ENTITY_IN_NAME = re.compile(
    r"\b(GmbH|UG|AG|KG|OHG|GbR|e\.V\.|e\.K\.|GmbH\s*&\s*Co\.?\s*KG|Limited|LLC)\b",
    re.IGNORECASE,
)

SECTION_START = re.compile(
    r"^(Kontakt|Telefon|Tel\.|Fax|E-?Mail|Register|Handelsregister|Amtsgericht|"
    r"Umsatzsteuer|USt|UID|Steuernummer|Webseite|Internet|AGB|Datenschutz|"
    r"Datenschutzbeauftragt\w*|Impressum|Registernummer|Registergericht)(\s|:|$)",
    re.IGNORECASE,
)

# Compact imprint summaries (og:description, meta) often glue "Name Kontakt:" without newline.
META_MD_RE = re.compile(
    r"(?:Geschäftsführer(?:in)?|Inhaber(?:in)?|Vertreten\s+durch|Vertretungsberechtigt\w*)\s*:\s*"
    r"(?P<name>[^\n<]{2,120}?)"
    r"(?=\s*(?:Kontakt|Telefon|Tel\.|Fax|E-?Mail|Register|Umsatzsteuer|USt-?|Handelsregister|"
    r"Amtsgericht|Datenschutz|Datenschutzbeauftragt\w*|Impressum|Vertretungsberechtigte|"
    r"Geschäftsführer|Inhaber)\b|$)",
    re.IGNORECASE | re.DOTALL,
)

# Longer phrases first so substring matches (e.g. vertretungsberechtigte vs … partner) stay stable.
MD_KEYWORDS = [
    "vertretungsberechtigte partner",
    "vertretungsberechtigte",
    "vertretungsberechtigter",
    "persönlich haftender",
    "vertretungsberechtigt",
    "vertreten durch",
    "geschäftsführende",
    "geschäftsleitung",
    "geschäftsführung",
    "geschäftsführer",
    "komplementär",
    "inhaber",
    "vorstand",
]


def _normalize_lines(text: str) -> List[str]:
    out: List[str] = []
    for line in text.splitlines():
        cleaned = " ".join(line.split())
        if cleaned:
            out.append(cleaned)
    return out


def _looks_like_person_name(s: str) -> bool:
    s = (s or "").strip()
    if len(s) < 3 or len(s) > 120:
        return False
    if "@" in s or "http://" in s.lower() or "https://" in s.lower():
        return False
    if LEGAL_ENTITY_IN_NAME.search(s):
        return False
    if SECTION_START.match(s):
        return False
    if not re.search(r"[A-Za-zÄÖÜäöüß]", s):
        return False
    if re.fullmatch(r"\+?[\d\s()./\-]{10,}", s):
        return False
    digit_ratio = sum(1 for c in s if c.isdigit()) / max(len(s), 1)
    if digit_ratio > 0.35:
        return False
    return True


def _line_has_md_keyword(low: str) -> Optional[str]:
    for k in MD_KEYWORDS:
        if k in low:
            return k
    return None


def extract_managing_director_from_imprint_plaintext(text: str) -> Optional[str]:
    if not text or not text.strip():
        return None

    flat = text.replace("\r\n", "\n").replace("\r", "\n")
    m = META_MD_RE.search(flat)
    if m:
        name = re.sub(r"\s+", " ", m.group("name").strip())
        if _looks_like_person_name(name):
            return name

    lines = _normalize_lines(flat)
    for i, line in enumerate(lines):
        low = line.lower()
        kw = _line_has_md_keyword(low)
        if not kw:
            continue

        if ":" in line:
            after = line.split(":", 1)[1].strip()
            if after and _looks_like_person_name(after):
                return after

        idx = low.find(kw)
        tail = line[idx + len(kw) :].strip(" :\t–-")
        if tail and _looks_like_person_name(tail):
            return tail

        for j in range(i + 1, min(i + 8, len(lines))):
            cand = lines[j].strip()
            if not cand:
                continue
            if SECTION_START.match(cand):
                break
            low_c = cand.lower()
            if _line_has_md_keyword(low_c) and ":" in cand:
                inner = cand.split(":", 1)[1].strip()
                if inner and _looks_like_person_name(inner):
                    return inner
                break
            if _looks_like_person_name(cand):
                return cand
            if ":" in cand and not _looks_like_person_name(cand.split(":", 1)[-1].strip()):
                break

    return None


def _walk_heading_for_name(el: Tag) -> Optional[str]:
    label = el.get_text(" ", strip=True)
    low = label.lower()
    if not any(k in low for k in MD_KEYWORDS):
        return None
    if ":" in label:
        after = label.split(":", 1)[1].strip()
        if after and _looks_like_person_name(after):
            return after

    for sib in el.next_siblings:
        if isinstance(sib, NavigableString):
            raw = str(sib).strip()
            if raw and _looks_like_person_name(raw):
                return raw
        if isinstance(sib, Tag):
            if sib.name in ("script", "style", "noscript"):
                continue
            inner = sib.get_text("\n", strip=True)
            if not inner:
                continue
            first = inner.split("\n", 1)[0].strip()
            if _looks_like_person_name(first):
                return first
            if SECTION_START.match(first):
                break
    return None


def extract_managing_director_from_imprint_soup(soup: BeautifulSoup) -> Optional[str]:
    for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "dt"]):
        if not isinstance(el, Tag):
            continue
        name = _walk_heading_for_name(el)
        if name:
            return name
    return None


def extract_managing_director_from_imprint_html(html: str) -> Optional[str]:
    if not html or not html.strip():
        return None
    soup = BeautifulSoup(html, "html.parser")
    return extract_managing_director_from_imprint_soup(soup)


def extract_managing_director_combined(html_or_text: str) -> Optional[str]:
    """Prefer heading-adjacent HTML structure, then plain-text heuristics (including meta-style blobs)."""
    soup = BeautifulSoup(html_or_text, "html.parser")
    from_html = extract_managing_director_from_imprint_soup(soup)
    if from_html:
        return from_html
    plain = soup.get_text("\n")
    return extract_managing_director_from_imprint_plaintext(plain)


def gpt_managing_directors_is_empty(managing_directors: Any) -> bool:
    if not managing_directors or not isinstance(managing_directors, list):
        return True

    def _nonempty_str(v: Any) -> bool:
        return isinstance(v, str) and bool(v.strip())

    for md in managing_directors:
        if isinstance(md, dict):
            if any(_nonempty_str(md.get(k)) for k in ("first_name", "last_name", "full_name")):
                return False
        elif isinstance(md, str) and md.strip():
            return False
    return True
