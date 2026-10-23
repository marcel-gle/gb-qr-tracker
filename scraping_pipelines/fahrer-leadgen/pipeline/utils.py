"""Shared helpers: polite HTTP, domain normalization, disk cache, dedup."""
from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path
from urllib.parse import urlparse, urljoin

import httpx

from config import USER_AGENT, REQUEST_TIMEOUT, REQUEST_DELAY_SECONDS

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_last_hit: dict[str, float] = {}


def normalize_domain(url_or_domain: str | None) -> str:
    """example.com from https://www.Example.com/path -> 'example.com'."""
    if not url_or_domain:
        return ""
    s = url_or_domain.strip().lower()
    if "://" not in s:
        s = "http://" + s
    host = urlparse(s).netloc or ""
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def _throttle(host: str) -> None:
    now = time.monotonic()
    last = _last_hit.get(host, 0.0)
    wait = REQUEST_DELAY_SECONDS - (now - last)
    if wait > 0:
        time.sleep(wait)
    _last_hit[host] = time.monotonic()


def _cache_path(key: str) -> Path:
    h = hashlib.sha256(key.encode()).hexdigest()[:24]
    return CACHE_DIR / f"{h}.json"


def fetch(url: str, *, headers: dict | None = None, params: dict | None = None,
          cache: bool = True, expect_json: bool = False, quiet: bool = False):
    """GET with per-host throttle + disk cache. Returns text or parsed JSON.

    Returns None on any network/HTTP error (caller decides how to handle).
    Pass quiet=True to suppress the per-URL error print (e.g. when the
    caller already reports its own aggregated summary).
    """
    cache_key = url + json.dumps(params or {}, sort_keys=True)
    cpath = _cache_path(cache_key)
    if cache and cpath.exists():
        raw = json.loads(cpath.read_text())
        return raw["json"] if expect_json else raw["text"]

    host = normalize_domain(url) or url
    _throttle(host)
    hdrs = {"User-Agent": USER_AGENT}
    if headers:
        hdrs.update(headers)
    try:
        r = httpx.get(url, headers=hdrs, params=params,
                      timeout=REQUEST_TIMEOUT, follow_redirects=True)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001 - pipeline should not crash on one URL
        if not quiet:
            print(f"  [fetch] {url} -> {type(e).__name__}: {e}")
        return None

    payload = {"text": r.text, "json": None}
    if expect_json:
        try:
            payload["json"] = r.json()
        except Exception:  # noqa: BLE001
            payload["json"] = None
    if cache:
        cpath.write_text(json.dumps(payload))
    return payload["json"] if expect_json else payload["text"]


def discover_links(html: str, base_url: str, hints: list[str]) -> list[str]:
    """Return absolute URLs whose href contains one of the hint fragments."""
    found: set[str] = set()
    for m in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.I):
        href = m.group(1)
        low = href.lower()
        if any(hint in low for hint in hints):
            found.add(urljoin(base_url, href))
    return sorted(found)


def visible_text(html: str) -> str:
    """Crude tag strip for feeding text to keyword checks / the LLM."""
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html,
                  flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


_EMPLOYEE_COUNT_RE = re.compile(
    r"(?:ca\.?|circa|rund|über|mehr als)?\s*"
    r"(\d{1,4})\s*"
    r"(?:-?köpfige[srn]?\s+team"
    r"|mitarbeiter(?:innen)?(?:/-?innen)?"
    r"|beschäftigte[n]?)",
    re.IGNORECASE,
)


def extract_employee_count(text: str) -> int | None:
    """Best-effort scan for an explicit headcount mention on the company's
    own site (e.g. '45 Mitarbeiter', 'rund 60 Mitarbeitern', '30-köpfiges
    Team'). Returns the first plausible match, or None if nothing found.

    This is a heuristic soft signal, not authoritative -- it exists to
    replace North Data's employee estimate (often stale/approximate) with
    whatever the company itself states, when they state it at all.
    """
    for m in _EMPLOYEE_COUNT_RE.finditer(text or ""):
        n = int(m.group(1))
        if 1 <= n <= 5000:  # sanity bound: filters out stray years/prices/IDs
            return n
    return None


def dedup_rows(rows: list[dict]) -> list[dict]:
    """Dedup on normalized domain first, then (name+plz) fallback."""
    seen_domain: dict[str, dict] = {}
    seen_nameplz: dict[str, dict] = {}
    out: list[dict] = []
    for row in rows:
        dom = normalize_domain(row.get("domain"))
        namekey = (
            re.sub(r"[^a-z0-9]", "", (row.get("company_name") or "").lower())
            + "|" + str(row.get("plz") or "")
        )
        if dom and dom in seen_domain:
            _merge_sources(seen_domain[dom], row)
            continue
        if namekey.strip("|") and namekey in seen_nameplz:
            _merge_sources(seen_nameplz[namekey], row)
            continue
        out.append(row)
        if dom:
            seen_domain[dom] = row
        if namekey.strip("|"):
            seen_nameplz[namekey] = row
    return out


def _merge_sources(keep: dict, dup: dict) -> None:
    srcs = set(str(keep.get("source", "")).split("+")) | \
           set(str(dup.get("source", "")).split("+"))
    keep["source"] = "+".join(sorted(s for s in srcs if s))
    # Fill any blank fields on the kept row from the duplicate.
    for k, v in dup.items():
        if not keep.get(k) and v:
            keep[k] = v
