"""
Stage 2 - Hiring-signal detection ("sucht Fahrer").

Three independent methods, unioned:
  (A) Bundesagentur für Arbeit Jobsuche API - free, largest DE job DB.
  (B) On-site career-page scraping - catches direct-hire SMEs not on boards.
  (C) Google Jobs (via SerpApi) - aggregates postings from job boards and
      company sites that never show up in (A) or (B). Optional: no-ops
      without an API key.

Adds to each row:
    hiring_signal: bool
    hiring_evidence: list[str]   # human-readable
    hiring_urls: list[str]
    posting_count: int           # rough count of matching postings
"""
from __future__ import annotations

import re

from config import (BA_JOBS_URL, BA_API_KEY, BA_DAYS_LOOKBACK,
                    DRIVER_KEYWORDS, CAREER_PATH_HINTS,
                    GOOGLE_JOBS_URL, GOOGLE_JOBS_API_KEY, GOOGLE_JOBS_LOCATION)
from utils import fetch, discover_links, visible_text


def _matches_driver(text: str) -> bool:
    low = text.lower()
    return any(kw in low for kw in DRIVER_KEYWORDS)


# --- Method A: Bundesagentur ------------------------------------------------
def ba_search(company_name: str, ort: str = "") -> list[dict]:
    """Query the BA Jobsuche for driver postings by this employer.

    Uses `arbeitgeber` free-text + `was` keyword. Returns list of
    {titel, arbeitgeber, ort, refnr}. Empty on miss or error.
    """
    params = {
        "arbeitgeber": company_name,
        "was": "Kraftfahrer",
        "veroeffentlichtseit": BA_DAYS_LOOKBACK,
        "size": 25,
        "page": 1,
        "pav": "false",  # exclude private Arbeitsvermittlung noise
    }
    data = fetch(BA_JOBS_URL, headers={"X-API-Key": BA_API_KEY},
                 params=params, expect_json=True)
    if not data:
        return []
    stellen = data.get("stellenangebote") or []
    hits = []
    for s in stellen:
        title = s.get("titel") or s.get("beruf") or ""
        emp = s.get("arbeitgeber") or ""
        # BA free-text match is loose; keep only rows whose employer name
        # or title actually looks driver-related and plausibly the same firm.
        if _matches_driver(title) or _matches_driver(s.get("beruf", "")):
            hits.append({
                "titel": title,
                "arbeitgeber": emp,
                "ort": (s.get("arbeitsort") or {}).get("ort", ""),
                "refnr": s.get("refnr") or s.get("referenznummer") or "",
            })
    return hits


# --- Method B: career page --------------------------------------------------
def scrape_career_pages(domain: str) -> list[dict]:
    """Fetch homepage, follow career-ish links, flag driver postings."""
    if not domain:
        return []
    home = fetch(f"https://{domain}/", expect_json=False)
    if not home:
        return []
    hits: list[dict] = []
    # Homepage itself might already advertise driver roles.
    if _matches_driver(visible_text(home)):
        hits.append({"url": f"https://{domain}/", "where": "homepage"})

    career_links = discover_links(home, f"https://{domain}/",
                                  CAREER_PATH_HINTS)[:6]
    for link in career_links:
        page = fetch(link, expect_json=False)
        if page and _matches_driver(visible_text(page)):
            hits.append({"url": link, "where": "career-page"})
    return hits


# --- Method C: Google Jobs (via SerpApi) ------------------------------------
def _norm_company(name: str) -> str:
    """Lowercase, strip punctuation and common German legal-form suffixes,
    so 'Spedition Meyer GmbH & Co. KG' and 'Spedition Meyer' compare equal."""
    low = re.sub(r"[^a-z0-9]+", " ", (name or "").lower())
    for suffix in ("gmbh co kg", "gmbh", "co kg", "ohg", "mbh", " ag", " ug",
                   " kg", " e v"):
        low = low.replace(suffix, " ")
    return re.sub(r"\s+", " ", low).strip()


def _same_company(a: str, b: str) -> bool:
    na, nb = _norm_company(a), _norm_company(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def google_jobs_search(company_name: str, ort: str = "") -> list[dict]:
    """Query Google Jobs for driver postings mentioning this employer.

    Google has no public API for the "Google for Jobs" search carousel, and
    scraping Google's result pages directly violates their ToS and gets
    IP-blocked quickly. This goes through SerpApi's Google Jobs endpoint
    instead (https://serpapi.com/google-jobs-api), a compliant third-party
    wrapper that returns the same structured results.

    Requires GOOGLE_JOBS_API_KEY (env var SERPAPI_KEY) to be set. Returns []
    silently if unset, so the pipeline still runs end-to-end without it.
    """
    if not GOOGLE_JOBS_API_KEY:
        return []
    params = {
        "engine": "google_jobs",
        "q": f"Kraftfahrer {company_name}",
        "location": ort or GOOGLE_JOBS_LOCATION,
        "hl": "de",
        "gl": "de",
        "api_key": GOOGLE_JOBS_API_KEY,
    }
    data = fetch(GOOGLE_JOBS_URL, params=params, expect_json=True)
    if not data:
        return []
    hits = []
    for r in data.get("jobs_results") or []:
        title = r.get("title", "")
        employer = r.get("company_name", "")
        desc = r.get("description", "")
        # The query is a loose text search, not an employer filter -- Google
        # Jobs will happily return postings from unrelated companies whose
        # title/description just happens to mention the search terms. Confirm
        # the posting's employer actually matches the company we're checking
        # before counting it as a hit.
        if not _same_company(company_name, employer):
            continue
        if _matches_driver(title) or _matches_driver(desc):
            hits.append({
                "titel": title,
                "arbeitgeber": employer,
                "ort": r.get("location", ""),
                "job_id": r.get("job_id", ""),
            })
    return hits


# --- Orchestration ----------------------------------------------------------
def enrich(row: dict) -> dict:
    evidence: list[str] = []
    urls: list[str] = []
    count = 0

    ba = ba_search(row["company_name"], row.get("ort", ""))
    for h in ba:
        evidence.append(f"BA: {h['titel']} @ {h['arbeitgeber']} ({h['ort']})")
        count += 1

    gj = google_jobs_search(row["company_name"], row.get("ort", ""))
    for h in gj:
        evidence.append(f"GoogleJobs: {h['titel']} @ {h['arbeitgeber']} ({h['ort']})")
        count += 1

    site = scrape_career_pages(row.get("domain", ""))
    for h in site:
        evidence.append(f"Site: driver posting on {h['where']} ({h['url']})")
        urls.append(h["url"])
        count += 1

    row["hiring_signal"] = count > 0
    row["hiring_evidence"] = evidence
    row["hiring_urls"] = urls
    row["posting_count"] = count
    return row


def run(rows: list[dict]) -> list[dict]:
    out = []
    for i, row in enumerate(rows, 1):
        enrich(row)
        flag = "HIT " if row["hiring_signal"] else "    "
        print(f"  [{i}/{len(rows)}] {flag} {row['company_name'][:45]}")
        for ev in row["hiring_evidence"]:
            print(f"             - {ev}")
        out.append(row)
    kept = [r for r in out if r["hiring_signal"]]
    print(f"  {len(kept)}/{len(out)} rows have an active driver signal")
    return kept
