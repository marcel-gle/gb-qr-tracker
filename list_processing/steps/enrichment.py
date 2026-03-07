from __future__ import annotations

import json
import logging
import pickle
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency
    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable

from ..llm.base import LLMClient
from ..models import LeadRecord, PromptUsage, normalize_postcode

logger = logging.getLogger(__name__)


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10


SYSTEM_PROMPT = """
You are an assistant that extracts structured company data from German "Impressum" (imprint) pages.

Your job:
- Read the given text from a German company's website (usually the Impressum / Kontakt page).
- Identify and extract:
  - The company's full postal address, split into structured components (street, house number, postal code, city).
  - The managing director(s) / legal representatives. CRITICAL: For each managing director, you MUST extract:
    * First name (Vorname) - the person's given name
    * Last name (Nachname) - the person's family name/surname
    * Gender/Salutation - determine if it's "Herr" (male) or "Frau" (female) based on the first name or explicit salutation in the text
    * Full name - the complete name as it appears in the text
  - The full legal company name as written in the imprint (including GmbH, UG, AG, KG, etc.).
  - Generic company phone numbers (main switchboard, office numbers; ignore obviously private mobiles if clearly marked as personal).
  - Generic company email addresses (like info@, kontakt@, office@; also include named emails if they are clearly business emails in the imprint).

Output rules:
- Always respond with a single valid JSON object only, no explanation text.
- Use this exact JSON structure and keys:

{
  "full_address": "string or null",
  "address_street": "string or null",
  "address_house_number": "string or null",
  "address_postcode": "string or null",
  "address_city": "string or null",
  "managing_directors": [
    {
      "first_name": "string or null",
      "last_name": "string or null",
      "gender": "Herr" or "Frau" or null,
      "full_name": "string or null"
    }
  ],
  "company_legal_name": "string or null",
  "generic_company_phones": ["+49 ...", "..."],
  "generic_company_emails": ["info@example.com", "..."],
  "confidence": 0.0
}

Notes:
- Only include information that appears in the text; do not invent data.
- If you are not sure about a field, set it to null or an empty list.
""".strip()


def _fetch_url(url: str) -> Optional[requests.Response]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            return resp
    except requests.RequestException:
        return None
    return None


def _normalize_domain_to_base_url(domain: str) -> Optional[str]:
    domain = (domain or "").strip()
    if not domain:
        return None

    if domain.startswith("http://"):
        domain = domain[len("http://") :]
    elif domain.startswith("https://"):
        domain = domain[len("https://") :]

    domain = domain.split("/")[0].rstrip("/")

    for scheme in ("https://", "http://"):
        url = scheme + domain
        resp = _fetch_url(url)
        if resp:
            return resp.url
    return None


def _find_imprint_url(base_html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(base_html, "html.parser")

    candidates: List[str] = []
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"].lower()
        if any(key in text for key in ["impressum", "imprint"]) or any(
            key in href for key in ["impressum", "imprint"]
        ):
            candidates.append(a["href"])

    for href in candidates:
        url = urljoin(base_url, href)
        resp = _fetch_url(url)
        if resp:
            return resp.url

    common_paths = [
        "/impressum",
        "/impressum/",
        "/impressum.html",
        "/imprint",
        "/imprint/",
        "/imprint.html",
        "/kontakt",
        "/kontakt/",
        "/kontakt/impressum/",
        "/kontakt/impressum",
    ]
    for path in common_paths:
        url = urljoin(base_url, path)
        resp = _fetch_url(url)
        if resp:
            return resp.url

    return None


def _extract_text_from_url(url: str) -> Optional[str]:
    resp = _fetch_url(url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(separator="\n")
    max_chars = 15_000
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


def get_imprint_text_for_domain(domain: str) -> Optional[str]:
    base_url = _normalize_domain_to_base_url(domain)
    if not base_url:
        logger.warning("Could not reach base URL for domain: %s", domain)
        return None

    logger.info("Base URL for %s: %s", domain, base_url)
    home_resp = _fetch_url(base_url)
    if not home_resp:
        logger.warning("Could not fetch homepage for: %s", domain)
        return None

    imprint_url = _find_imprint_url(home_resp.text, base_url)
    if imprint_url:
        logger.info("Imprint URL for %s: %s", domain, imprint_url)
        return _extract_text_from_url(imprint_url)

    logger.info("No imprint URL found for %s, using homepage text as fallback.", domain)
    soup = BeautifulSoup(home_resp.text, "html.parser")
    text = soup.get_text(separator="\n")
    max_chars = 15_000
    if len(text) > max_chars:
        text = text[:max_chars]
    return text


class EnrichmentService:
    """
    High-level service that enriches LeadRecord instances by scraping website
    imprint pages and calling an LLM to extract structured company data.
    """

    def __init__(
        self,
        llm: LLMClient,
        *,
        cache_path: Optional[Path] = None,
        prompt_name: str = "imprint_enrichment",
        prompt_version: str = "1.0",
    ) -> None:
        self._llm = llm
        self._prompt_name = prompt_name
        self._prompt_version = prompt_version
        self._cache_path = cache_path
        self._cache_lock = Lock()
        self._domain_cache: Dict[str, Dict[str, object]] = {}

        if cache_path and cache_path.exists():
            try:
                with cache_path.open("rb") as f:
                    data = pickle.load(f)
                    if isinstance(data, dict):
                        self._domain_cache = data
                        logger.info(
                            "Loaded enrichment cache from %s with %d entries",
                            cache_path,
                            len(self._domain_cache),
                        )
            except Exception as exc:  # pragma: no cover - best-effort
                logger.warning("Failed to load enrichment cache from %s: %s", cache_path, exc)

    def save_cache(self) -> None:
        if not self._cache_path:
            return
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self._cache_path.open("wb") as f:
                pickle.dump(self._domain_cache, f)
            logger.info(
                "Saved enrichment cache to %s with %d entries",
                self._cache_path,
                len(self._domain_cache),
            )
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning("Failed to save enrichment cache to %s: %s", self._cache_path, exc)

    def _call_llm_for_imprint(
        self,
        domain: str,
        company_name: str,
        imprint_text: str,
    ) -> Dict[str, object]:
        user_prompt = f"""
Extract company data from this website content.

Domain: {domain}
CRM company name: {company_name}

Text from Impressum / Kontakt page:
\"\"\" 
{imprint_text}
\"\"\"

Remember: respond with a single JSON object only, using the exact schema described in the system prompt.
""".strip()

        content = self._llm.chat(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            response_format={"type": "json_object"},
            temperature=1.0,
        )

        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            logger.warning("LLM response for %s was not valid JSON, using fallback.", domain)
            data = {
                "full_address": None,
                "address_street": None,
                "address_house_number": None,
                "address_postcode": None,
                "address_city": None,
                "managing_directors": [],
                "company_legal_name": None,
                "generic_company_phones": [],
                "generic_company_emails": [],
                "confidence": 0.0,
            }

        return data

    def _split_name_with_llm(self, full_name: str) -> tuple[Optional[str], Optional[str]]:
        """
        Best-effort splitting of a personal name into first and last name using the LLM.
        Returns (first_name, last_name); either may be None on failure.
        """
        full_name = (full_name or "").strip()
        if not full_name:
            return None, None

        user_prompt = f"""
Split the following German personal name into first_name and last_name.

Name: {full_name}

Respond ONLY with a JSON object of the form:
{{"first_name": "...", "last_name": "..."}}
""".strip()

        try:
            content = self._llm.chat(
                system_prompt="You split German personal names into first and last names.",
                user_prompt=user_prompt,
                response_format={"type": "json_object"},
                temperature=0.0,
            )
            data = json.loads(content)
            first = data.get("first_name") or None
            last = data.get("last_name") or None
            return (str(first).strip() or None) if first else None, (str(last).strip() or None) if last else None
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("LLM name split failed for %r: %s", full_name, exc)
            return None, None

    def enrich_record(self, record: LeadRecord) -> bool:
        """
        Enrich a single record in-place.

        Returns True if any field was updated.
        """
        domain = (record.website or "").strip()
        if not domain:
            logger.debug("Record %s has no website, skipping enrichment.", record.company_name)
            return False

        with self._cache_lock:
            cached = self._domain_cache.get(domain)

        if cached is None:
            imprint_text = get_imprint_text_for_domain(domain)
            if not imprint_text:
                logger.info("No imprint text found for domain %s", domain)
                # Fall back to an \"empty\" imprint payload so that downstream
                # normalization and representative fallbacks still run.
                gpt_data = {
                    "full_address": None,
                    "address_street": None,
                    "address_house_number": None,
                    "address_postcode": None,
                    "address_city": None,
                    "managing_directors": [],
                    "company_legal_name": None,
                    "generic_company_phones": [],
                    "generic_company_emails": [],
                    "confidence": 0.0,
                }
            else:
                logger.info("Calling LLM for imprint extraction for %s", domain)
                gpt_data = self._call_llm_for_imprint(domain, record.company_name, imprint_text)
                with self._cache_lock:
                    self._domain_cache[domain] = gpt_data
        else:
            logger.debug("Using cached imprint data for %s", domain)
            gpt_data = cached

        updated = False

        # Always keep a copy of the raw imprint address for traceability.
        full_addr = gpt_data.get("full_address") or None
        if full_addr:
            record.imprint_address = full_addr

        # Normalize address fields using structured imprint data when available.
        if full_addr and not record.raw_address:
            record.raw_address = full_addr
            updated = True

        street = gpt_data.get("address_street") or None
        if street:
            if street != record.street:
                record.street = street
                updated = True

        house = gpt_data.get("address_house_number") or None
        if house:
            if house != record.house_number:
                record.house_number = house
                updated = True

        postcode_raw = gpt_data.get("address_postcode") or None
        if postcode_raw:
            postcode_norm = normalize_postcode(postcode_raw)
            if postcode_norm and postcode_norm != record.postcode:
                record.postcode = postcode_norm
                updated = True

        city = gpt_data.get("address_city") or None
        if city:
            if city != record.city:
                record.city = city
                updated = True

        md_list = gpt_data.get("managing_directors") or []
        # Always store up to three raw imprint managing director names for auditing.
        if isinstance(md_list, list) and md_list:
            def _md_display(md: Dict[str, object]) -> Optional[str]:
                if not isinstance(md, dict):
                    return None
                full = (md.get("full_name") or "") if isinstance(md.get("full_name"), str) else ""
                first = (md.get("first_name") or "") if isinstance(md.get("first_name"), str) else ""
                last = (md.get("last_name") or "") if isinstance(md.get("last_name"), str) else ""
                gender = (md.get("gender") or "") if isinstance(md.get("gender"), str) else ""
                # Prefer full_name if available; otherwise build from parts.
                base_name = full or " ".join(part for part in [first, last] if part).strip()
                if not base_name:
                    return None
                return base_name if not gender else f"{gender} {base_name}"

            md_strings = [_md_display(md) for md in md_list[:3]]
            if len(md_strings) > 0:
                record.imprint_managing_director_1 = md_strings[0]
            if len(md_strings) > 1:
                record.imprint_managing_director_2 = md_strings[1]
            if len(md_strings) > 2:
                record.imprint_managing_director_3 = md_strings[2]

            # Also continue to backfill the primary normalized managing director fields if empty.
            first_md = md_list[0] if isinstance(md_list[0], dict) else None
            if isinstance(first_md, dict):
                first_name = first_md.get("first_name") or None
                last_name = first_md.get("last_name") or None
                gender = first_md.get("gender") or None
                full_name = first_md.get("full_name") or None

                if full_name and not record.managing_director_full:
                    record.managing_director_full = full_name
                    updated = True
                if first_name and not record.managing_director_first:
                    record.managing_director_first = first_name
                    updated = True
                if last_name and not record.managing_director_last:
                    record.managing_director_last = last_name
                    updated = True
                if gender and not record.salutation:
                    record.salutation = gender
                    updated = True

        # If imprint provided only a full_name, try to split it with the LLM.
        if md_list and record.managing_director_full and (
            not record.managing_director_first or not record.managing_director_last
        ):
            split_first, split_last = self._split_name_with_llm(record.managing_director_full)
            if split_first and not record.managing_director_first:
                record.managing_director_first = split_first
                updated = True
            if split_last and not record.managing_director_last:
                record.managing_director_last = split_last
                updated = True

        # Fallback: use Ges. Vertreter 1 (rep1_raw) to fill missing managing director fields when available.
        if record.rep1_raw:
            rep = (record.rep1_raw or "").strip()
            first_rep: Optional[str] = None
            last_rep: Optional[str] = None
            if rep:
                # Northdata-style format \"Last, First\"; fall back to simple \"First Last\".
                parts = [p.strip() for p in rep.split(",") if p.strip()]
                if len(parts) >= 2:
                    last_rep, first_rep = parts[0], parts[1]
                else:
                    tokens = rep.split()
                    if len(tokens) >= 2:
                        first_rep = tokens[0]
                        last_rep = " ".join(tokens[1:]).strip()
                    else:
                        last_rep = rep

            full_rep = " ".join(p for p in [first_rep, last_rep] if p).strip() or rep
            if full_rep and not record.managing_director_full:
                record.managing_director_full = full_rep
                updated = True
            if first_rep and not record.managing_director_first:
                record.managing_director_first = first_rep
                updated = True
            if last_rep and not record.managing_director_last:
                record.managing_director_last = last_rep
                updated = True

        # Ensure first_name/last_name reflect the best available contact if they were missing.
        if not record.first_name and record.managing_director_first:
            record.first_name = record.managing_director_first
            updated = True
        if not record.last_name and record.managing_director_last:
            record.last_name = record.managing_director_last
            updated = True

        # If street still contains the house number at the end, normalize by stripping it out.
        if record.street and record.house_number:
            s = str(record.street).strip()
            hn = str(record.house_number).strip()
            if s.endswith(" " + hn):
                record.street = s[: -(len(hn) + 1)].rstrip(" ,")
                updated = True

        # If we still have no house_number but the street has a trailing number, split it heuristically.
        if record.street and not record.house_number:
            m = re.match(r"^(.*?)(\s+\d[0-9A-Za-z\/\- ]*)$", str(record.street).strip())
            if m:
                street_name = m.group(1).strip(" ,")
                house_raw = m.group(2).strip()
                if street_name:
                    record.street = street_name
                    updated = True
                if house_raw:
                    record.house_number = house_raw
                    updated = True

        legal = gpt_data.get("company_legal_name") or None
        if legal and not record.legal_name:
            record.legal_name = legal
            updated = True

        phones = gpt_data.get("generic_company_phones") or []
        if isinstance(phones, list) and phones and not record.phone:
            record.phone = str(phones[0])
            updated = True

        emails = gpt_data.get("generic_company_emails") or []
        if isinstance(emails, list) and emails and not record.email:
            record.email = str(emails[0])
            updated = True

        # Track which prompt/backend was used for traceability.
        record.prompts_used.append(
            PromptUsage(
                step="enrichment",
                prompt_name=self._prompt_name,
                version=self._prompt_version,
                backend=type(self._llm).__name__,
                model=getattr(self._llm, "model_name", None),
            )
        )

        return updated


def run_enrichment(
    records: List[LeadRecord],
    service: EnrichmentService,
    *,
    max_workers_http: int = 10,
) -> None:
    """
    Enrich a list of records concurrently.

    This function mutates the records in-place and saves the service cache
    at the end of the run.
    """
    if not records:
        return

    logger.info("Starting enrichment for %d records (max_workers_http=%d)", len(records), max_workers_http)

    def _worker(rec: LeadRecord) -> None:
        try:
            service.enrich_record(rec)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Error enriching record %s (%s): %s", rec.company_name, rec.website, exc)

    with ThreadPoolExecutor(max_workers=max_workers_http) as executor:
        futures = [executor.submit(_worker, rec) for rec in records]
        # Progress bar over completed futures
        for _ in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Enrichment",
            unit="record",
        ):
            # We don't need individual results here; errors are logged in _worker.
            pass

    service.save_cache()
    logger.info("Enrichment finished")

