from __future__ import annotations

import json
import logging
import pickle
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict, List, Optional

from list_processing.llm.base import LLMClient

from ..models import MAX_DIRECTORS_DEFAULT, BusinessRow, normalize_postcode
from .fetch import close_browser_pool, get_imprint_text_for_domain

logger = logging.getLogger(__name__)


def gender_to_salutation(gender: object | None) -> str | None:
    if gender is None:
        return None
    s = str(gender).strip().lower()
    if not s or s in ("null", "none", "unknown", "unklar"):
        return None
    if s in ("herr", "mr", "male", "männlich", "m"):
        return "Herr"
    if s in ("frau", "ms", "mrs", "female", "weiblich", "f"):
        return "Frau"
    return None


def _first_name_for_salutation(director: Dict[str, Optional[str]]) -> str:
    first = (director.get("first_name") or "").strip()
    if first:
        return first
    full = (director.get("imprint_managing_director") or "").strip()
    if full:
        return full.split()[0]
    return ""


SYSTEM_PROMPT = """
You are an assistant that extracts structured company data from German "Impressum" (imprint) pages.

Extract:
- full postal address split into street, house_number, postcode, city
- managing_directors array with first_name, last_name, gender (Herr/Frau), full_name
- company_legal_name
- generic_company_phones, generic_company_emails

Respond with a single JSON object only:
{
  "full_address": "string or null",
  "address_street": "string or null",
  "address_house_number": "string or null",
  "address_postcode": "string or null",
  "address_city": "string or null",
  "managing_directors": [
    {"first_name": "...", "last_name": "...", "gender": "Herr|Frau|null", "full_name": "..."}
  ],
  "company_legal_name": "string or null",
  "generic_company_phones": ["..."],
  "generic_company_emails": ["..."],
  "confidence": 0.0
}
""".strip()


class ImprintExtractor:
    def __init__(self, llm: LLMClient, cache_path: Optional[Path] = None) -> None:
        self._llm = llm
        self._cache_path = cache_path
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()
        self._salutation_service: Any = None
        if cache_path and cache_path.exists():
            try:
                with cache_path.open("rb") as f:
                    data = pickle.load(f)
                    if isinstance(data, dict):
                        self._cache = data
            except Exception as exc:
                logger.warning("Failed to load imprint cache: %s", exc)

    def _salutation_infer(self, first_name: str) -> str:
        if self._salutation_service is None:
            from list_processing.steps.salutation import SalutationService

            self._salutation_service = SalutationService(self._llm)
        return self._salutation_service.infer_salutation(first_name)

    def save_cache(self) -> None:
        if not self._cache_path:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        with self._cache_path.open("wb") as f:
            pickle.dump(self._cache, f)

    def _call_llm(self, domain: str, company_name: str, imprint_text: str) -> Dict[str, Any]:
        user_prompt = f"""
Extract company data from this website content.

Domain: {domain}
CRM company name: {company_name}

Text from Impressum / Kontakt page:
\"\"\"
{imprint_text}
\"\"\"
""".strip()
        content = self._llm.chat(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {
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

    def extract_for_domain(self, domain: str, company_name: str = "") -> Dict[str, Any]:
        with self._lock:
            cached = self._cache.get(domain)
        if cached is not None:
            return cached
        imprint_text = get_imprint_text_for_domain(domain)
        if not imprint_text:
            data: Dict[str, Any] = {
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
            data = self._call_llm(domain, company_name, imprint_text)
        with self._lock:
            self._cache[domain] = data
        return data

    def apply_to_row(self, row: BusinessRow, max_directors: int = MAX_DIRECTORS_DEFAULT) -> bool:
        data = self.extract_for_domain(row.domain, row.company_name or "")
        updated = False

        full_addr = data.get("full_address")
        if full_addr:
            row.full_address = str(full_addr)
            updated = True

        for src, dst in (
            ("address_street", "street"),
            ("address_house_number", "house_number"),
            ("address_city", "city"),
        ):
            val = data.get(src)
            if val and not getattr(row, dst):
                setattr(row, dst, str(val))
                updated = True

        pc = data.get("address_postcode")
        if pc:
            norm = normalize_postcode(pc)
            if norm and not row.postcode:
                row.postcode = norm
                updated = True

        legal = data.get("company_legal_name")
        if legal and not row.legal_name:
            row.legal_name = str(legal)
            if not row.company_name:
                row.company_name = str(legal)
            updated = True

        phones = data.get("generic_company_phones") or []
        if isinstance(phones, list) and phones and not row.phone:
            row.phone = str(phones[0])
            updated = True

        emails = data.get("generic_company_emails") or []
        if isinstance(emails, list) and emails and not row.email:
            row.email = str(emails[0])
            updated = True

        md_list = data.get("managing_directors") or []
        directors: List[Dict[str, Optional[str]]] = []
        if isinstance(md_list, list):
            for md in md_list[:max_directors]:
                if not isinstance(md, dict):
                    continue
                first = md.get("first_name")
                last = md.get("last_name")
                gender = md.get("gender")
                full = md.get("full_name")
                display = str(full or " ".join(p for p in [first, last] if p)).strip()
                directors.append(
                    {
                        "first_name": str(first).strip() if first else None,
                        "last_name": str(last).strip() if last else None,
                        "salutation": gender_to_salutation(gender),
                        "linkedin_profile_url": None,
                        "imprint_managing_director": display or None,
                    }
                )
        if directors:
            existing = row.directors or []
            for i, d in enumerate(directors):
                if d.get("salutation"):
                    continue
                if i < len(existing) and existing[i].get("salutation"):
                    d["salutation"] = existing[i]["salutation"]
                    continue
                first_name = _first_name_for_salutation(d)
                if first_name:
                    d["salutation"] = self._salutation_infer(first_name)
            row.directors = directors
            updated = True

        if row.street and not row.house_number:
            m = re.match(r"^(.*?)(\s+\d[0-9A-Za-z\/\- ]*)$", str(row.street).strip())
            if m:
                row.street = m.group(1).strip(" ,")
                row.house_number = m.group(2).strip()
                updated = True

        for d in row.directors:
            if d.get("salutation"):
                continue
            first_name = _first_name_for_salutation(d)
            if first_name:
                d["salutation"] = self._salutation_infer(first_name)
                updated = True

        return updated


def run_imprint_scrape(
    rows: List[BusinessRow],
    extractor: ImprintExtractor,
    *,
    max_workers: int = 10,
    progress_callback: Callable[[int, int, float, str], None] | None = None,
) -> None:
    def _worker(row: BusinessRow) -> None:
        try:
            extractor.apply_to_row(row)
        except Exception as exc:
            logger.warning("Imprint failed for %s: %s", row.domain, exc)

    total = len(rows)
    if progress_callback and total == 0:
        progress_callback(0, 0, 0.0, "")

    started = time.monotonic()
    completed = 0
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_worker, r): r for r in rows}
            for fut in as_completed(futures):
                row = futures[fut]
                fut.result()
                completed += 1
                if progress_callback is not None:
                    progress_callback(completed, total, time.monotonic() - started, row.domain)
        extractor.save_cache()
    finally:
        close_browser_pool()
