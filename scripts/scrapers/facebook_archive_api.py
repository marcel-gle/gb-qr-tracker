"""
Meta Ad Library (Facebook Ads Archive) — CSV enrichment

Reads a CSV with ``facebook_page_name`` / ``instagram_page_name`` (e.g. from
``extract_social_links.py``), queries the Graph ``ads_archive`` endpoint: for each
row it searches **Facebook page name first**, and only if that returns no ads with a
``page_id`` it searches **Instagram page name** (when different from the Facebook
term). If both names are empty, it tries a **domain-based term**: hostname from the
``domain`` column (see ``--domain-column``) with the last dot-label removed (e.g.
``example.com`` → ``example``, ``www.shop.de`` → ``shop``). Writes Meta ad match
columns to the output CSV. Each query is logged in a
compact one-line format (via ``tqdm.write``).

Requirements:
    - Meta app with Marketing API / Ad Library access and a valid user or system token
      with ``ads_read`` (and permissions required for your use case).

Usage (from repo root):

    export META_ACCESS_TOKEN="..."   # or FACEBOOK_ACCESS_TOKEN
    python scripts/scrapers/facebook_archive_api.py input.csv output.csv

    python scripts/scrapers/facebook_archive_api.py input.csv output.csv \\
        --access-token "$META_ACCESS_TOKEN" --delay 2.5 --countries DE,AT

    python scripts/scrapers/facebook_archive_api.py input.csv output.csv --limit 10
    python scripts/scrapers/facebook_archive_api.py --help

At the end of a run the script prints a detailed summary (counts and percentages) for API
usage, archive payload size, and filled CSV columns.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests
from tqdm import tqdm

try:
    from dotenv import load_dotenv

    _env = Path(__file__).resolve().parent.parent.parent / ".env"
    if _env.exists():
        load_dotenv(_env)
    else:
        load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

DEFAULT_GRAPH_VERSION = "v25.0"
DEFAULT_DELAY_SECONDS = 1.0


def normalize_graph_version(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return DEFAULT_GRAPH_VERSION
    return s if s.startswith("v") else f"v{s}"


REQUEST_TIMEOUT = 60
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

COL_FB_NAME = "facebook_page_name"
COL_IG_NAME = "instagram_page_name"
COL_DOMAIN_DEFAULT = "domain"

META_COLS = (
    "meta_ads_found",
    "meta_ads_page_ids_count",
    "meta_best_match_page_id",
    "meta_best_match_page_name",
    "meta_best_match_score",
    "meta_best_match_ad_url",
    "meta_all_page_ids",
)


def detect_delimiter(file_path: Path) -> str:
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        counts = {d: sample.count(d) for d in [",", ";", "\t", "|"]}
        return max(counts, key=counts.get) if max(counts.values()) > 0 else ","


def normalize(text: Optional[str]) -> str:
    if text is None:
        return ""
    text = str(text).strip().lower()
    if not text:
        return ""

    replacements = {
        "gmbh": "",
        "ag": "",
        "ug": "",
        "mbh": "",
        ".": "",
        ",": "",
        "-": " ",
        "_": " ",
        "/": " ",
    }

    for k, v in replacements.items():
        text = text.replace(k, v)

    return " ".join(text.split())


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


def query_ads_archive(
    search_term: str,
    *,
    access_token: str,
    graph_version: str,
    countries: list[str],
    session: requests.Session,
    dry_run: bool,
) -> tuple[Optional[dict[str, Any]], Optional[int]]:
    """Call Meta ``ads_archive``. Returns (parsed JSON or None, HTTP status code or None)."""
    if dry_run:
        logger.info("dry-run: would query ads_archive for %r", search_term)
        return {"data": []}, None

    url = f"https://graph.facebook.com/{graph_version}/ads_archive"
    params: dict[str, Any] = {
        "search_terms": search_term,
        "search_type": "KEYWORD_EXACT_PHRASE",
        "ad_reached_countries": json.dumps([c.upper()[:2] for c in countries if c.strip()]),
        "ad_type": "ALL",
        "fields": "page_id,page_name,ad_snapshot_url,publisher_platforms",
        "limit": 100,
        "access_token": access_token,
    }

    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    code = resp.status_code
    logger.debug("ads_archive status=%s search=%r", code, search_term)

    try:
        data = resp.json()
    except Exception:
        logger.warning("Could not parse JSON for search_term=%r", search_term)
        return None, code

    if code != 200 or "error" in data:
        logger.warning("ads_archive error: %s", json.dumps(data, indent=2)[:2000])
        return None, code

    return data, code


def row_has_any_social_name(row: dict[str, str]) -> bool:
    """True if Facebook or Instagram page name column has a value."""
    return bool((row.get(COL_FB_NAME) or "").strip() or (row.get(COL_IG_NAME) or "").strip())


def resolve_csv_column(fieldnames: list[str], preferred: str) -> str:
    """Return the actual header matching ``preferred`` (case-insensitive), or ``\"\"``."""
    if not preferred or not fieldnames:
        return ""
    if preferred in fieldnames:
        return preferred
    plow = preferred.lower()
    for col in fieldnames:
        if col.lower() == plow:
            return col
    return ""


def _hostname_from_domain_field(raw: str) -> str:
    """Extract hostname (no port, lowercased) from a URL or bare domain string."""
    s = (raw or "").strip()
    if not s:
        return ""
    if "://" in s:
        host = urlparse(s).netloc
    else:
        host = s.split("/")[0].split("@")[-1]
    host = host.strip().lower()
    if not host:
        return ""
    if ":" in host:
        host = host.split(":")[0]
    return host


def domain_search_term_without_tld(raw: str) -> str:
    """
    Brand-style label for Ad Library search: strip final DNS label (``.com``, ``.de``, …).

    Examples: ``example.com`` → ``example``; ``www.foo-bar.de`` → ``foo-bar``;
    ``a.b.example.co.uk`` → ``a.b.example.co`` (multi-part ccTLDs are not special-cased).
    """
    host = _hostname_from_domain_field(raw)
    if not host:
        return ""
    while host.startswith("www."):
        host = host[4:]
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 1:
        return parts[0] if parts else ""
    core = ".".join(parts[:-1])
    while core.startswith("www."):
        core = core[4:]
    return core


def row_has_search_term(row: dict[str, str], domain_column: str) -> bool:
    """True if the row has FB/IG names or a non-empty domain-derived fallback term."""
    if row_has_any_social_name(row):
        return True
    if not domain_column:
        return False
    return bool(domain_search_term_without_tld(row.get(domain_column, "")))


def pick_search_term(row: dict[str, str]) -> str:
    """Single combined term preferring Facebook (for legacy / display)."""
    fb = (row.get(COL_FB_NAME) or "").strip()
    if fb:
        return fb
    return (row.get(COL_IG_NAME) or "").strip()


def response_has_page_ids(data: Optional[dict[str, Any]]) -> bool:
    """True if API returned at least one ad object with a page_id."""
    if not data:
        return False
    for item in data.get("data", []):
        if item.get("page_id"):
            return True
    return False


def compact_ads_response_summary(data: Optional[dict[str, Any]]) -> str:
    """One-line summary for logging (no secrets)."""
    if data is None:
        return "http/json=fail"
    items = data.get("data", [])
    if not isinstance(items, list):
        return "data=malformed"
    n = len(items)
    pids = {str(x.get("page_id")) for x in items if isinstance(x, dict) and x.get("page_id")}
    bits: list[str] = []
    for x in items[:4]:
        if not isinstance(x, dict):
            continue
        pn = (x.get("page_name") or "")[:28].replace("\n", " ")
        pid = x.get("page_id")
        bits.append(f"{pn!r}:{pid}")
    extra = ""
    if n > len(bits):
        extra = f" …+{n - len(bits)} ads"
    inner = ", ".join(bits) if bits else "—"
    return f"items={n} unique_page_ids={len(pids)} [{inner}{extra}]"


def run_archive_query(
    term: str,
    label: str,
    *,
    row_idx: int,
    access_token: str,
    graph_version: str,
    countries: list[str],
    session: requests.Session,
    dry_run: bool,
    delay_s: float,
    run: dict[str, int],
) -> Optional[dict[str, Any]]:
    """Sleep (unless dry-run), call ads_archive, log compact line, update run counters."""
    if not dry_run and delay_s > 0:
        time.sleep(delay_s)
    data, http_code = query_ads_archive(
        term,
        access_token=access_token,
        graph_version=graph_version,
        countries=countries,
        session=session,
        dry_run=dry_run,
    )
    term_disp = term if len(term) <= 56 else term[:53] + "..."
    status_bit = "dry-run" if dry_run else (f"http={http_code}" if http_code is not None else "http=?")
    tqdm.write(
        f"[row {row_idx + 1}] {label} term={term_disp!r} | {status_bit} | "
        f"{compact_ads_response_summary(data)}"
    )
    if data is None:
        run["api_failed"] += 1
    else:
        run["api_success"] += 1
        run["total_ad_objects_in_responses"] += len(data.get("data", []))
    return data


def find_best_match(
    data: dict[str, Any],
    expected_names: list[str],
) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    """Pick the archive row whose page_name best matches expected names."""
    candidates: list[dict[str, Any]] = []

    for item in data.get("data", []):
        page_name = item.get("page_name", "") or ""
        page_id = item.get("page_id", "") or ""
        ad_url = item.get("ad_snapshot_url", "") or ""

        best_score = 0.0
        for expected in expected_names:
            if not expected:
                continue
            score = similarity(expected, page_name)
            if score > best_score:
                best_score = score

        candidates.append(
            {
                "page_name": page_name,
                "page_id": page_id,
                "ad_url": ad_url,
                "score": best_score,
            }
        )

    if not candidates:
        return None, []

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[0], candidates


def apply_meta_defaults(row: dict[str, str]) -> None:
    for c in META_COLS:
        row.setdefault(c, "")
    if not row.get("meta_ads_found"):
        row["meta_ads_found"] = "false"
    if not str(row.get("meta_ads_page_ids_count", "")).strip():
        row["meta_ads_page_ids_count"] = "0"
    if not (row.get("meta_all_page_ids") or "").strip():
        row["meta_all_page_ids"] = "[]"


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]], delimiter: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _pct(count: int, denom: int) -> str:
    if denom <= 0:
        return "—"
    return f"{100.0 * count / denom:.1f}%"


def _empty_run_counters() -> dict[str, int]:
    return {
        "skipped_no_search_term": 0,
        "api_failed": 0,
        "api_success": 0,
        "total_ad_objects_in_responses": 0,
        "rows_with_unique_page_ids": 0,
        "rows_with_best_match_id": 0,
        "rows_with_snapshot_url": 0,
    }


def print_meta_collection_summary(
    rows: list[dict[str, str]],
    indices: list[int],
    n_total_rows: int,
    output_path: Path,
    run: dict[str, int],
    dry_run: bool,
    elapsed_s: float,
    domain_column_resolved: str,
) -> None:
    """Print what was collected: API outcomes, ads_archive payload, and enriched columns."""
    sep = "=" * 72
    n_run = len(indices)
    skipped = run.get("skipped_no_search_term", 0)
    api_failed = run.get("api_failed", 0)
    api_ok = run.get("api_success", 0)
    api_attempts = api_failed + api_ok
    ad_objects = run.get("total_ad_objects_in_responses", 0)
    rows_ads = run.get("rows_with_unique_page_ids", 0)
    rows_best = run.get("rows_with_best_match_id", 0)
    rows_snap = run.get("rows_with_snapshot_url", 0)

    eligible_all = sum(1 for r in rows if row_has_search_term(r, domain_column_resolved))
    ads_true_all = sum(1 for r in rows if r.get("meta_ads_found") == "true")
    with_best_all = sum(1 for r in rows if (r.get("meta_best_match_page_id") or "").strip())
    with_snap_all = sum(1 for r in rows if (r.get("meta_best_match_ad_url") or "").strip())

    counts_in_run: list[int] = []
    scores_in_run: list[float] = []
    for i in indices:
        c_raw = (rows[i].get("meta_ads_page_ids_count") or "").strip()
        try:
            counts_in_run.append(int(c_raw) if c_raw else 0)
        except ValueError:
            counts_in_run.append(0)
        sc = (rows[i].get("meta_best_match_score") or "").strip()
        if sc:
            try:
                scores_in_run.append(float(sc))
            except ValueError:
                pass

    sum_ids = sum(counts_in_run)
    max_ids = max(counts_in_run) if counts_in_run else 0
    avg_ids = sum_ids / len(counts_in_run) if counts_in_run else 0.0

    hi = mid = lo = 0
    for v in scores_in_run:
        if v >= 0.8:
            hi += 1
        elif v >= 0.5:
            mid += 1
        else:
            lo += 1
    n_scored = len(scores_in_run)
    avg_score = sum(scores_in_run) / n_scored if n_scored else 0.0

    print(f"\n{sep}")
    print(f"Summary  →  {output_path}")
    if dry_run:
        print("  Mode: --dry-run (no live API calls)")
    if elapsed_s > 0:
        print(f"  Wall time (this run): {elapsed_s:.1f}s")
    print(sep)
    print("Dataset")
    print(f"  Total CSV rows:                    {n_total_rows}")
    print(f"  Rows in this run (index slice):    {n_run}  ({_pct(n_run, n_total_rows)} of CSV)")
    dom_note = (
        f" — FB/IG name or {domain_column_resolved!r} without TLD"
        if domain_column_resolved
        else " — FB/IG name only (no domain column)"
    )
    print(
        f"  Rows with a search term (whole file): {eligible_all}  "
        f"({_pct(eligible_all, n_total_rows)} of CSV){dom_note}"
    )
    print()

    print("This run — row handling")
    print(
        f"  Skipped (no search term):          {skipped:>6}  ({_pct(skipped, n_run)} of rows in this run)"
    )
    attempted = n_run - skipped
    print(
        f"  Rows attempted (had search term): {attempted:>6}  ({_pct(attempted, n_run)} of rows in this run)"
    )
    print()

    print("This run — Graph ads_archive calls")
    if api_attempts == 0:
        print("  (No API attempts in this slice — e.g. all rows skipped or empty row range)")
    else:
        print(
            f"  API responses parsed OK:            {api_ok:>6}  ({_pct(api_ok, api_attempts)} of calls)"
        )
        print(
            f"  API errors / non-JSON / error JSON: {api_failed:>6}  ({_pct(api_failed, api_attempts)} of calls)"
        )
        print(f"  Total ad objects in responses:      {ad_objects:>6}  (sum of len(data) per OK response)")
        if api_ok > 0:
            print(f"  Avg ad objects per OK response:     {ad_objects / api_ok:.1f}")
    print()

    print("This run — collected fields (subset of rows processed)")
    print(
        f"  Rows with ≥1 unique page_id:      {rows_ads:>6}  ({_pct(rows_ads, attempted)} of attempted; {_pct(rows_ads, n_run)} of run slice)"
    )
    print(
        f"  Rows with best-match page_id:     {rows_best:>6}  ({_pct(rows_best, attempted)} of attempted)"
    )
    print(
        f"  Rows with ad snapshot URL:       {rows_snap:>6}  ({_pct(rows_snap, attempted)} of attempted)"
    )
    if counts_in_run:
        print(
            f"  meta_ads_page_ids_count (run):   sum={sum_ids}, max={max_ids}, avg={avg_ids:.2f} per row in slice"
        )
    if n_scored:
        print(
            f"  Best-match name score (run):     avg={avg_score:.3f};  ≥0.8: {hi}, 0.5–0.8: {mid}, <0.5: {lo}  (rows with a score: {n_scored})"
        )
    print()

    print("Whole output file — cumulative columns")
    print(
        f"  meta_ads_found = true:            {ads_true_all:>6}  ({_pct(ads_true_all, n_total_rows)} of all CSV rows)"
    )
    print(
        f"  meta_best_match_page_id set:     {with_best_all:>6}  ({_pct(with_best_all, n_total_rows)} of all CSV rows)"
    )
    print(
        f"  meta_best_match_ad_url set:      {with_snap_all:>6}  ({_pct(with_snap_all, n_total_rows)} of all CSV rows)"
    )
    if eligible_all:
        print(
            f"  Hit rate (ads_found on file):     {ads_true_all} / {eligible_all}  ({_pct(ads_true_all, eligible_all)} of rows that have a search term)"
        )
    print(sep)


def process_csv(
    input_path: Path,
    output_path: Path,
    *,
    access_token: str,
    graph_version: str,
    delay_s: float,
    countries: list[str],
    limit: Optional[int],
    save_every: int,
    dry_run: bool,
    domain_column: str = COL_DOMAIN_DEFAULT,
) -> None:
    delimiter = detect_delimiter(input_path)
    print(f"Detected delimiter: {repr(delimiter)}")

    with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldnames = list(reader.fieldnames or [])
        for c in META_COLS:
            if c not in fieldnames:
                fieldnames.append(c)
        rows = [{k: (v if v is not None else "") for k, v in r.items()} for r in reader]

    dom_pref = (domain_column or "").strip() or COL_DOMAIN_DEFAULT
    domain_col_resolved = resolve_csv_column(fieldnames, dom_pref)
    if not domain_col_resolved:
        print(
            f"Note: column {dom_pref!r} not in CSV; rows without FB/IG page names are skipped "
            "(no domain fallback)."
        )

    for row in rows:
        apply_meta_defaults(row)

    n = len(rows)
    if limit is not None and limit > 0:
        indices = list(range(min(limit, n)))
        print(f"Processing first {len(indices)} of {n} row(s) (--limit)")
    else:
        indices = list(range(n))

    graph_version = normalize_graph_version(graph_version)

    if not indices:
        write_rows(output_path, fieldnames, rows, delimiter)
        print(f"\nWrote {output_path} (no rows in range to process)")
        print_meta_collection_summary(
            rows,
            [],
            n,
            output_path,
            _empty_run_counters(),
            dry_run,
            0.0,
            domain_col_resolved,
        )
        return

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    save_every = max(1, save_every)
    run = _empty_run_counters()
    t_loop = time.time()

    with tqdm(indices, desc="Meta Ad Library", unit="row") as pbar:
        for idx in pbar:
            row = rows[idx]
            fb_raw = (row.get(COL_FB_NAME) or "").strip()
            ig_raw = (row.get(COL_IG_NAME) or "").strip()
            domain_raw = (row.get(domain_col_resolved, "") if domain_col_resolved else "").strip()
            domain_term = domain_search_term_without_tld(domain_raw) if domain_col_resolved else ""
            postfix = fb_raw or ig_raw or domain_term or "—"
            pbar.set_postfix_str(postfix[:28] + "…" if len(postfix) > 28 else postfix)

            if not row_has_search_term(row, domain_col_resolved):
                run["skipped_no_search_term"] += 1
                logger.debug("row %s: no FB/IG page name and no domain fallback", idx)
                if (idx + 1) % save_every == 0 or idx == indices[-1]:
                    write_rows(output_path, fieldnames, rows, delimiter)
                continue

            data_final: Optional[dict[str, Any]] = None

            if fb_raw:
                d_fb = run_archive_query(
                    fb_raw,
                    "facebook_page_name",
                    row_idx=idx,
                    access_token=access_token,
                    graph_version=graph_version,
                    countries=countries,
                    session=session,
                    dry_run=dry_run,
                    delay_s=delay_s,
                    run=run,
                )
                if response_has_page_ids(d_fb):
                    data_final = d_fb

            try_ig = bool(ig_raw) and (not fb_raw or ig_raw.casefold() != fb_raw.casefold())
            if data_final is None and try_ig:
                d_ig = run_archive_query(
                    ig_raw,
                    "instagram_page_name",
                    row_idx=idx,
                    access_token=access_token,
                    graph_version=graph_version,
                    countries=countries,
                    session=session,
                    dry_run=dry_run,
                    delay_s=delay_s,
                    run=run,
                )
                if response_has_page_ids(d_ig):
                    data_final = d_ig

            if data_final is None and not fb_raw and not ig_raw and domain_term:
                d_dom = run_archive_query(
                    domain_term,
                    "domain_no_tld",
                    row_idx=idx,
                    access_token=access_token,
                    graph_version=graph_version,
                    countries=countries,
                    session=session,
                    dry_run=dry_run,
                    delay_s=delay_s,
                    run=run,
                )
                if response_has_page_ids(d_dom):
                    data_final = d_dom

            work: dict[str, Any] = data_final if data_final is not None else {"data": []}
            results = work.get("data", [])
            unique_page_ids = sorted(
                {str(item.get("page_id")) for item in results if item.get("page_id")}
            )
            ads_found = len(unique_page_ids) > 0
            if ads_found:
                run["rows_with_unique_page_ids"] += 1

            if fb_raw or ig_raw:
                expected_for_match = [x for x in (fb_raw, ig_raw) if x]
            else:
                expected_for_match = [domain_term] if domain_term else []
            best_match, _candidates = find_best_match(work, expected_for_match)

            row["meta_ads_found"] = "true" if ads_found else "false"
            row["meta_ads_page_ids_count"] = str(len(unique_page_ids))
            row["meta_all_page_ids"] = json.dumps(unique_page_ids)

            if best_match:
                row["meta_best_match_page_id"] = str(best_match["page_id"])
                row["meta_best_match_page_name"] = str(best_match["page_name"])
                row["meta_best_match_score"] = str(round(float(best_match["score"]), 4))
                row["meta_best_match_ad_url"] = str(best_match["ad_url"])
                if (best_match.get("page_id") or "").strip():
                    run["rows_with_best_match_id"] += 1
                if (best_match.get("ad_url") or "").strip():
                    run["rows_with_snapshot_url"] += 1
                logger.debug(
                    "row %s: best_match page_name=%r score=%s",
                    idx,
                    best_match["page_name"],
                    row["meta_best_match_score"],
                )
            else:
                row["meta_best_match_page_id"] = ""
                row["meta_best_match_page_name"] = ""
                row["meta_best_match_score"] = ""
                row["meta_best_match_ad_url"] = ""

            if (idx + 1) % save_every == 0 or idx == indices[-1]:
                write_rows(output_path, fieldnames, rows, delimiter)

    elapsed_loop = time.time() - t_loop

    write_rows(output_path, fieldnames, rows, delimiter)
    print(f"\nWrote {output_path}")

    print_meta_collection_summary(
        rows,
        indices,
        n,
        output_path,
        run,
        dry_run,
        elapsed_loop,
        domain_col_resolved,
    )


def _resolve_access_token(cli_token: Optional[str]) -> str:
    if cli_token and cli_token.strip():
        return cli_token.strip()
    for env_key in (
        "META_ACCESS_TOKEN",
        "FACEBOOK_ACCESS_TOKEN",
        "GRAPH_API_ACCESS_TOKEN",
    ):
        v = os.environ.get(env_key, "").strip()
        if v:
            return v
    return ""


def _parse_countries(s: str) -> list[str]:
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Enrich a CSV with Meta Ad Library (ads_archive) results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment:\n"
            "  META_ACCESS_TOKEN, FACEBOOK_ACCESS_TOKEN, or GRAPH_API_ACCESS_TOKEN\n"
            "  if --access-token is not passed.\n"
        ),
    )
    p.add_argument("input_csv", type=Path, help="Input CSV path")
    p.add_argument("output_csv", type=Path, help="Output CSV path")
    p.add_argument(
        "--access-token",
        default="",
        help="Graph API access token (otherwise env META_ACCESS_TOKEN / FACEBOOK_ACCESS_TOKEN)",
    )
    p.add_argument(
        "--graph-version",
        default=os.environ.get("META_GRAPH_API_VERSION", DEFAULT_GRAPH_VERSION),
        help=f"Graph API version, e.g. v25.0 (default: env META_GRAPH_API_VERSION or {DEFAULT_GRAPH_VERSION})",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help=f"Seconds to sleep between API calls (default: {DEFAULT_DELAY_SECONDS})",
    )
    p.add_argument(
        "--countries",
        default="DE",
        help="Comma-separated ISO country codes for ad_reached_countries (default: DE)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N rows (for testing)",
    )
    p.add_argument(
        "--save-every",
        type=int,
        default=1,
        metavar="N",
        help="Write the full CSV every N processed rows (default: 1). Use a larger N to reduce I/O.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call the API; still walks rows and writes CSV shell",
    )
    p.add_argument(
        "--domain-column",
        default=COL_DOMAIN_DEFAULT,
        metavar="COL",
        help=(
            "CSV column for domain/URL when facebook_page_name and instagram_page_name "
            f"are both empty; last DNS label is stripped for the search term (default: {COL_DOMAIN_DEFAULT})"
        ),
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    if argv is None:
        return p.parse_args()
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    # Strip script name (same pattern as score_domains / extract_social_links).
    if argv is None:
        args = parse_args(None)
    else:
        args = parse_args(argv[1:])
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    token = _resolve_access_token(args.access_token or None)
    if not token and not args.dry_run:
        print(
            "error: no access token. Set META_ACCESS_TOKEN or use --access-token.",
            file=sys.stderr,
        )
        return 1

    if not args.input_csv.exists():
        print(f"error: input not found: {args.input_csv}", file=sys.stderr)
        return 1

    countries = _parse_countries(args.countries)
    if not countries:
        print("error: --countries must list at least one code", file=sys.stderr)
        return 1

    try:
        process_csv(
            args.input_csv,
            args.output_csv,
            access_token=token,
            graph_version=args.graph_version,
            delay_s=max(0.0, float(args.delay)),
            countries=countries,
            limit=args.limit,
            save_every=args.save_every,
            dry_run=args.dry_run,
            domain_column=args.domain_column,
        )
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except Exception as e:
        logger.exception("failed")
        print(f"error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
