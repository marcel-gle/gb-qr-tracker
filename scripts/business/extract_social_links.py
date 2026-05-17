"""
Read a CSV with a domain column, fetch each homepage, and extract Facebook / Instagram
profile URLs (common in footers). Writes URL columns, matching page-name columns (plain
identifiers, not URLs), plus status fields for resumable runs. Prints a summary with counts
and percentages after each run.

Usage:
    python scripts/business/extract_social_links.py input.csv output.csv
    python scripts/business/extract_social_links.py input.csv output.csv --domain-column url
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()
except ImportError:
    pass

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10

OUTPUT_FB_COL = "facebook_url"
OUTPUT_IG_COL = "instagram_url"
OUTPUT_FB_NAME_COL = "facebook_page_name"
OUTPUT_IG_NAME_COL = "instagram_page_name"
STATUS_COL = "social_links_status"
ERROR_COL = "social_links_error"

_FB_EXCLUDED_PATH_PREFIXES = (
    "/sharer",
    "/share.php",
    "/plugins",
    "/dialog",
    "/tr",
    "/privacy",
    "/policies",
    "/help",
    "/images",
    "/ads",
    "/business",
    "/login",
    "/signup",
    "/marketplace",
    "/gaming",
    "/events",
    "/groups/discover",
    "/watch",
    "/l.php",
)
_FB_EXCLUDED_NETLOCS = (
    "connect.facebook.net",
    "developers.facebook.com",
    "graph.facebook.com",
)

_IG_RESERVED_FIRST_SEGMENTS = frozenset(
    {
        "p",
        "reel",
        "reels",
        "tv",
        "stories",
        "explore",
        "accounts",
        "direct",
        "legal",
        "about",
        "developer",
    }
)


def fetch_url(url: str) -> Optional[requests.Response]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        ct = resp.headers.get("Content-Type", "")
        if resp.status_code == 200 and "text/html" in ct:
            return resp
        logger.debug("fetch_url bad response %s ct=%s", resp.status_code, ct)
    except requests.RequestException as e:
        logger.debug("fetch_url exception %s: %s", url, e)
    return None


def fetch_homepage(domain: str) -> Optional[tuple[str, str]]:
    """Return (final_url, html) for the first reachable scheme, or None."""
    domain = domain.strip()
    if not domain:
        return None
    if domain.startswith("http://"):
        domain = domain[len("http://") :]
    elif domain.startswith("https://"):
        domain = domain[len("https://") :]
    host = domain.split("/")[0].rstrip("/")
    for scheme in ("https://", "http://"):
        url = scheme + host
        resp = fetch_url(url)
        if resp:
            return (resp.url, resp.text)
    logger.warning("Could not reach domain: %s", domain)
    return None


def detect_delimiter(file_path: Path) -> str:
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        counts = {d: sample.count(d) for d in [",", ";", "\t", "|"]}
        return max(counts, key=counts.get) if max(counts.values()) > 0 else ","


def _netloc_host(netloc: str) -> str:
    return netloc.lower().split("@")[-1].split(":")[0]


def canonical_facebook_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url.strip())
    except Exception:
        return None
    host = _netloc_host(p.netloc)
    if host in _FB_EXCLUDED_NETLOCS:
        return None
    if not (
        host == "facebook.com"
        or host == "www.facebook.com"
        or host == "m.facebook.com"
        or host == "fb.com"
        or host == "www.fb.com"
        or host.endswith(".facebook.com")
    ):
        return None
    path_lower = (p.path or "/").lower()
    for pref in _FB_EXCLUDED_PATH_PREFIXES:
        if path_lower.startswith(pref):
            return None
    if "profile.php" in path_lower:
        qs = parse_qs(p.query)
        ids = qs.get("id", [])
        if not ids:
            return None
        clean = urlunparse(("https", "www.facebook.com", "/profile.php", "", f"id={ids[0]}", ""))
        return clean
    path = p.path or "/"
    if path in ("/", ""):
        return None
    clean = urlunparse(("https", "www.facebook.com", path.rstrip("/") or "/", "", "", ""))
    return clean


def canonical_instagram_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url.strip())
    except Exception:
        return None
    host = _netloc_host(p.netloc)
    if "instagram.com" not in host:
        return None
    parts = [x for x in (p.path or "").strip("/").split("/") if x]
    if not parts:
        return None
    first = parts[0].lower()
    if first in _IG_RESERVED_FIRST_SEGMENTS:
        return None
    if not re.match(r"^[A-Za-z0-9._]{1,30}$", parts[0]):
        return None
    user = parts[0]
    return f"https://www.instagram.com/{user}/"


def facebook_page_name_from_canonical(url: str) -> str:
    """Human-readable Facebook identifier from our normalized URL (not a full URL)."""
    u = (url or "").strip()
    if not u:
        return ""
    p = urlparse(u)
    path = p.path or ""
    if "profile.php" in path.lower():
        ids = parse_qs(p.query).get("id", [])
        return ids[0] if ids else ""
    segs = [s for s in path.strip("/").split("/") if s]
    if not segs:
        return ""
    head = segs[0].lower()
    if head == "pages" and len(segs) >= 2:
        return segs[1]
    if head == "people" and len(segs) >= 2:
        return segs[1]
    if head == "groups" and len(segs) >= 2:
        return segs[1]
    return segs[0]


def instagram_page_name_from_canonical(url: str) -> str:
    """Instagram username from our normalized URL (not a full URL)."""
    u = (url or "").strip()
    if not u:
        return ""
    p = urlparse(u)
    segs = [s for s in (p.path or "").strip("/").split("/") if s]
    return segs[0] if segs else ""


def ensure_social_name_columns(row: dict[str, str]) -> None:
    """Set page-name columns from URL columns (names only; no scheme/host)."""
    fb = (row.get(OUTPUT_FB_COL) or "").strip()
    ig = (row.get(OUTPUT_IG_COL) or "").strip()
    row[OUTPUT_FB_NAME_COL] = facebook_page_name_from_canonical(fb) if fb else ""
    row[OUTPUT_IG_NAME_COL] = instagram_page_name_from_canonical(ig) if ig else ""


def _iter_footer_hrefs(soup: BeautifulSoup) -> Iterable[str]:
    for footer in soup.find_all("footer"):
        for a in footer.find_all("a", href=True):
            yield a["href"]
    for tag in soup.find_all(True):
        if tag.name == "footer":
            continue
        classes = " ".join(tag.get("class") or []).lower()
        tid = (tag.get("id") or "").lower()
        if "footer" not in classes and "footer" not in tid:
            continue
        for a in tag.find_all("a", href=True):
            yield a["href"]


def _iter_all_hrefs(soup: BeautifulSoup) -> Iterable[str]:
    for a in soup.find_all("a", href=True):
        yield a["href"]


def _same_as_urls_from_jsonld(soup: BeautifulSoup) -> list[str]:
    out: list[str] = []
    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        blobs: list[Any] = data if isinstance(data, list) else [data]

        def walk(o: Any) -> None:
            if isinstance(o, dict):
                if "sameAs" in o:
                    sa = o["sameAs"]
                    if isinstance(sa, str):
                        out.append(sa)
                    elif isinstance(sa, list):
                        out.extend(str(x) for x in sa if isinstance(x, str))
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for x in o:
                    walk(x)

        for b in blobs:
            walk(b)
    return out


def extract_facebook_instagram(html: str, base_url: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    fb, ig = "", ""

    def consider(raw_href: str) -> None:
        nonlocal fb, ig
        if not raw_href or not raw_href.strip():
            return
        href = raw_href.strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "#")):
            return
        abs_url = urljoin(base_url, href)
        if not fb:
            c = canonical_facebook_url(abs_url)
            if c:
                fb = c
        if not ig:
            c = canonical_instagram_url(abs_url)
            if c:
                ig = c

    for href in _iter_footer_hrefs(soup):
        consider(href)
        if fb and ig:
            return fb, ig

    for href in _iter_all_hrefs(soup):
        consider(href)
        if fb and ig:
            return fb, ig

    for u in _same_as_urls_from_jsonld(soup):
        consider(u)
        if fb and ig:
            break

    return fb, ig


def process_row(domain: str) -> dict[str, Any]:
    domain = domain.strip()
    if not domain:
        return {
            OUTPUT_FB_COL: "",
            OUTPUT_IG_COL: "",
            OUTPUT_FB_NAME_COL: "",
            OUTPUT_IG_NAME_COL: "",
            STATUS_COL: "empty_domain",
            ERROR_COL: "",
        }
    home = fetch_homepage(domain)
    if not home:
        return {
            OUTPUT_FB_COL: "",
            OUTPUT_IG_COL: "",
            OUTPUT_FB_NAME_COL: "",
            OUTPUT_IG_NAME_COL: "",
            STATUS_COL: "fetch_failed",
            ERROR_COL: "could not fetch homepage",
        }
    final_url, html = home
    fb, ig = extract_facebook_instagram(html, final_url)
    return {
        OUTPUT_FB_COL: fb,
        OUTPUT_IG_COL: ig,
        OUTPUT_FB_NAME_COL: facebook_page_name_from_canonical(fb) if fb else "",
        OUTPUT_IG_NAME_COL: instagram_page_name_from_canonical(ig) if ig else "",
        STATUS_COL: "ok",
        ERROR_COL: "",
    }


def _pct(count: int, denom: int) -> str:
    if denom <= 0:
        return "—"
    return f"{100.0 * count / denom:.1f}%"


def print_social_discovery_summary(
    rows: list[dict[str, str]],
    domain_column: str,
    output_path: Path,
    *,
    elapsed_s: Optional[float],
    rows_fetched_this_run: Optional[int],
    process_stats: Optional[dict[str, int]],
) -> None:
    """Print counts and percentages for domains, fetch status, and found social links."""
    n_total = len(rows)
    n_empty_domain = sum(1 for r in rows if not r.get(domain_column, "").strip())
    n_domain = n_total - n_empty_domain
    eligible = [r for r in rows if r.get(domain_column, "").strip()]

    with_fb = sum(1 for r in eligible if r.get(OUTPUT_FB_COL, "").strip())
    with_ig = sum(1 for r in eligible if r.get(OUTPUT_IG_COL, "").strip())
    with_both = sum(
        1
        for r in eligible
        if r.get(OUTPUT_FB_COL, "").strip() and r.get(OUTPUT_IG_COL, "").strip()
    )
    with_either = sum(
        1
        for r in eligible
        if r.get(OUTPUT_FB_COL, "").strip() or r.get(OUTPUT_IG_COL, "").strip()
    )
    with_neither = n_domain - with_either

    ok_rows = [r for r in eligible if (r.get(STATUS_COL) or "").strip() == "ok"]
    n_ok = len(ok_rows)
    with_fb_ok = sum(1 for r in ok_rows if r.get(OUTPUT_FB_COL, "").strip())
    with_ig_ok = sum(1 for r in ok_rows if r.get(OUTPUT_IG_COL, "").strip())
    with_both_ok = sum(
        1
        for r in ok_rows
        if r.get(OUTPUT_FB_COL, "").strip() and r.get(OUTPUT_IG_COL, "").strip()
    )
    with_either_ok = sum(
        1
        for r in ok_rows
        if r.get(OUTPUT_FB_COL, "").strip() or r.get(OUTPUT_IG_COL, "").strip()
    )
    with_neither_ok = n_ok - with_either_ok

    status_hist: dict[str, int] = {}
    for r in eligible:
        st = (r.get(STATUS_COL) or "").strip() or "(no status)"
        status_hist[st] = status_hist.get(st, 0) + 1

    sep = "=" * 72
    print(f"\n{sep}")
    print(f"Summary  →  {output_path}")
    if elapsed_s is not None and elapsed_s > 0:
        print(f"  Wall time (fetch + parse this run): {elapsed_s:.1f}s")
    print(sep)
    print("Dataset")
    print(f"  Total CSV rows:           {n_total}")
    print(f"  Empty domain column:      {n_empty_domain} ({_pct(n_empty_domain, n_total)} of total)")
    print(f"  Rows with domain:         {n_domain} ({_pct(n_domain, n_total)} of total)")
    print()
    print("Status (rows with domain only)")
    for st in sorted(status_hist.keys(), key=lambda k: (-status_hist[k], k)):
        c = status_hist[st]
        print(f"  {st:<18} {c:>5}  ({_pct(c, n_domain)} of domain rows)")
    print()

    if process_stats is not None and rows_fetched_this_run is not None and rows_fetched_this_run > 0:
        n_run = rows_fetched_this_run
        print(f"This run only ({n_run} row(s) fetched / parsed)")
        for key in ("ok", "fetch_failed", "error", "empty_domain"):
            c = process_stats.get(key, 0)
            print(f"  {key:<18} {c:>5}  ({_pct(c, n_run)} of this run)")
        print()

    print(f"Found social links (% of {n_domain} row(s) with domain)")
    print(
        f"  Facebook URL:             {with_fb:>5}  ({_pct(with_fb, n_domain)} of domain rows; {_pct(with_fb, n_total)} of all rows)"
    )
    print(
        f"  Instagram URL:            {with_ig:>5}  ({_pct(with_ig, n_domain)} of domain rows; {_pct(with_ig, n_total)} of all rows)"
    )
    print(
        f"  Both FB + Instagram:      {with_both:>5}  ({_pct(with_both, n_domain)} of domain rows; {_pct(with_both, n_total)} of all rows)"
    )
    print(
        f"  At least one link:        {with_either:>5}  ({_pct(with_either, n_domain)} of domain rows; {_pct(with_either, n_total)} of all rows)"
    )
    print(
        f"  Neither link:             {with_neither:>5}  ({_pct(with_neither, n_domain)} of domain rows; {_pct(with_neither, n_total)} of all rows)"
    )
    print()

    if n_ok > 0:
        print(
            f"Among pages successfully loaded (status=ok, n={n_ok}) — hit rate on fetched HTML"
        )
        print(
            f"  Facebook URL:             {with_fb_ok:>5}  ({_pct(with_fb_ok, n_ok)} of loaded pages)"
        )
        print(
            f"  Instagram URL:            {with_ig_ok:>5}  ({_pct(with_ig_ok, n_ok)} of loaded pages)"
        )
        print(
            f"  Both FB + Instagram:      {with_both_ok:>5}  ({_pct(with_both_ok, n_ok)} of loaded pages)"
        )
        print(
            f"  At least one link:        {with_either_ok:>5}  ({_pct(with_either_ok, n_ok)} of loaded pages)"
        )
        print(
            f"  Neither link:             {with_neither_ok:>5}  ({_pct(with_neither_ok, n_ok)} of loaded pages)"
        )
        print()

    print("Page name columns (non-empty, among domain rows)")
    with_fb_name = sum(1 for r in eligible if r.get(OUTPUT_FB_NAME_COL, "").strip())
    with_ig_name = sum(1 for r in eligible if r.get(OUTPUT_IG_NAME_COL, "").strip())
    print(
        f"  facebook_page_name:       {with_fb_name:>5}  ({_pct(with_fb_name, n_domain)} of domain rows; {_pct(with_fb_name, n_total)} of all rows)"
    )
    print(
        f"  instagram_page_name:      {with_ig_name:>5}  ({_pct(with_ig_name, n_domain)} of domain rows; {_pct(with_ig_name, n_total)} of all rows)"
    )
    print(sep)


def process_csv(
    input_path: Path,
    output_path: Path,
    domain_column: str = "domain",
    max_workers: int = 10,
    skip_existing: bool = True,
    limit: Optional[int] = None,
) -> None:
    delimiter = detect_delimiter(input_path)
    print(f"Detected delimiter: {repr(delimiter)}")

    rows: list[dict[str, str]] = []
    with open(input_path, "r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile, delimiter=delimiter)
        fieldnames = list(reader.fieldnames or [])
        if domain_column not in fieldnames:
            dlow = domain_column.lower()
            for col in fieldnames:
                if col.lower() == dlow:
                    domain_column = col
                    break
            else:
                raise ValueError(
                    f"Column {domain_column!r} not found. Available: {fieldnames}"
                )
        extra = [
            OUTPUT_FB_COL,
            OUTPUT_IG_COL,
            OUTPUT_FB_NAME_COL,
            OUTPUT_IG_NAME_COL,
            STATUS_COL,
            ERROR_COL,
        ]
        for c in extra:
            if c not in fieldnames:
                fieldnames.append(c)
        for row in reader:
            rows.append({k: (v or "") if v is not None else "" for k, v in row.items()})

    print(f"Loaded {len(rows)} rows")

    rows_to_process: list[tuple[int, str]] = []
    skipped_empty = 0
    skipped_existing = 0
    for i, row in enumerate(rows):
        d = row.get(domain_column, "").strip()
        if not d:
            skipped_empty += 1
            continue
        if skip_existing and row.get(STATUS_COL, "").strip() == "ok":
            skipped_existing += 1
            continue
        rows_to_process.append((i, d))

    original = len(rows_to_process)
    if limit is not None and limit > 0:
        rows_to_process = rows_to_process[:limit]
        if len(rows_to_process) < original:
            print(f"Limited to first {limit} rows (of {original})")

    print(
        f"Rows to process: {len(rows_to_process)} "
        f"(skipped {skipped_empty} empty domains, {skipped_existing} already ok)"
    )

    if not rows_to_process:
        with open(output_path, "w", encoding="utf-8-sig", newline="") as outfile:
            w = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=delimiter)
            w.writeheader()
            for row in rows:
                for c in extra:
                    row.setdefault(c, "")
                ensure_social_name_columns(row)
                w.writerow(row)
        print(f"Wrote {output_path}")
        print_social_discovery_summary(
            rows,
            domain_column,
            output_path,
            elapsed_s=None,
            rows_fetched_this_run=None,
            process_stats=None,
        )
        return

    stats = {"ok": 0, "fetch_failed": 0, "empty_domain": 0, "error": 0}
    results: dict[int, dict[str, Any]] = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(process_row, dom): (i, dom) for i, dom in rows_to_process}
        with tqdm(total=len(futs), desc="Social links", unit="row") as pbar:
            for fut in as_completed(futs):
                i, dom = futs[fut]
                try:
                    r = fut.result()
                    results[i] = r
                    st = r.get(STATUS_COL, "")
                    stats[st] = stats.get(st, 0) + 1
                except Exception as e:
                    logger.exception("row %s domain %s", i, dom)
                    results[i] = {
                        OUTPUT_FB_COL: "",
                        OUTPUT_IG_COL: "",
                        OUTPUT_FB_NAME_COL: "",
                        OUTPUT_IG_NAME_COL: "",
                        STATUS_COL: "error",
                        ERROR_COL: str(e),
                    }
                    stats["error"] += 1
                pbar.update(1)

    for idx, data in results.items():
        rows[idx][OUTPUT_FB_COL] = data.get(OUTPUT_FB_COL, "") or ""
        rows[idx][OUTPUT_IG_COL] = data.get(OUTPUT_IG_COL, "") or ""
        rows[idx][OUTPUT_FB_NAME_COL] = data.get(OUTPUT_FB_NAME_COL, "") or ""
        rows[idx][OUTPUT_IG_NAME_COL] = data.get(OUTPUT_IG_NAME_COL, "") or ""
        rows[idx][STATUS_COL] = data.get(STATUS_COL, "") or ""
        rows[idx][ERROR_COL] = data.get(ERROR_COL, "") or ""

    with open(output_path, "w", encoding="utf-8-sig", newline="") as outfile:
        w = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=delimiter)
        w.writeheader()
        for row in rows:
            for c in extra:
                row.setdefault(c, "")
            ensure_social_name_columns(row)
            w.writerow(row)

    elapsed = time.time() - t0
    n_run = len(rows_to_process)
    print(f"\nDone in {elapsed:.1f}s → {output_path}")
    print_social_discovery_summary(
        rows,
        domain_column,
        output_path,
        elapsed_s=elapsed,
        rows_fetched_this_run=n_run,
        process_stats=dict(stats),
    )


def main(argv: list[str]) -> int:
    import argparse

    p = argparse.ArgumentParser(
        description="Fetch homepages from a CSV and extract Facebook / Instagram profile URLs."
    )
    p.add_argument("input_csv", help="Input CSV path")
    p.add_argument("output_csv", help="Output CSV path")
    p.add_argument(
        "--domain-column",
        default="domain",
        help="Column with domain or site URL (default: domain)",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Concurrent fetches (default: 10)",
    )
    p.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-process rows that already have social_links_status=ok",
    )
    p.add_argument("--limit", type=int, default=None, help="Process at most N rows")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = p.parse_args(argv[1:])

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    inp = Path(args.input_csv)
    out = Path(args.output_csv)
    if not inp.exists():
        print(f"Input not found: {inp}", file=sys.stderr)
        return 1
    try:
        process_csv(
            inp,
            out,
            domain_column=args.domain_column,
            max_workers=args.max_workers,
            skip_existing=not args.no_skip_existing,
            limit=args.limit,
        )
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        return 1
    except Exception as e:
        logger.exception("failed")
        print(e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
