"""
Brave Search API Scraper
========================
Sucht über die Brave Search API nach einem oder mehreren Begriffen und
schreibt die organischen Ergebnisse in eine CSV-Datei.

Voraussetzungen:
    1. Brave Search API Account (https://brave.com/search/api/)
    2. API-Key (X-Subscription-Token)

Nutzung (Projektroot):

    python scripts/scrapers/brave_search_api.py \
        --api-key KEY \
        --query "eventlocation berlin" \
        --num-results 30

    python scripts/scrapers/brave_search_api.py \
        --api-key KEY \
        --queries "eventlocation berlin,tagungsraum hamburg" \
        --num-results 50

    python scripts/scrapers/brave_search_api.py \
        --api-key KEY \
        --queries-file queries.txt --num-results 100

    python scripts/scrapers/brave_search_api.py --help

Kosten/Quota (Stand 2025/2026):
    - Free tier: 2.000 Queries/Monat (1 Query == 1 API-Call)
    - Search plan: ~$5 / 1.000 Queries
    - Pagination: max 20 Treffer/Seite, max 10 Seiten (offset 0-9) = 200 Treffer/Query
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("Installing requests...")
    os.system(f"{sys.executable} -m pip install requests --break-system-packages -q")
    import requests


# ============================================================
# KONFIGURATION
# ============================================================

BASE_URL = "https://api.search.brave.com/res/v1/web/search"
RESULTS_PER_PAGE = 20          # Brave max count per request
MAX_OFFSET = 9                 # Brave offset 0-9 (page index)
MAX_RESULTS_PER_QUERY = RESULTS_PER_PAGE * (MAX_OFFSET + 1)  # 200
COST_PER_REQUEST_USD = 0.005   # ~$5 / 1.000 Queries (Search plan)

_CFG: Optional["SearchConfig"] = None


@dataclass
class SearchConfig:
    api_key: str
    queries: List[str]
    num_results: int
    output: Optional[Path]
    search_lang: str
    country: str
    ui_lang: str
    safesearch: str
    site: Optional[str]
    freshness: Optional[str]
    extra_snippets: bool
    delay_ms: int
    dry_run: bool
    timestamp: str = field(init=False, default="")

    def __post_init__(self) -> None:
        self.num_results = max(1, min(int(self.num_results), MAX_RESULTS_PER_QUERY))
        self.delay_ms = max(0, int(self.delay_ms))
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    @property
    def output_csv(self) -> Path:
        if self.output is not None:
            return self.output
        return Path("output") / f"brave_search_{self.timestamp}.csv"


def _cfg() -> SearchConfig:
    if _CFG is None:
        raise RuntimeError("Search not configured; run via main() with CLI arguments.")
    return _CFG


# ============================================================
# CLI
# ============================================================

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Brave Search API — Suchergebnisse als CSV exportieren.",
        epilog=(
            "Beispiel:\n"
            '  %(prog)s --api-key KEY --query "eventlocation berlin"\n'
            '  %(prog)s --api-key KEY --queries "a,b,c" --num-results 30\n'
            "  %(prog)s --api-key KEY --queries-file queries.txt --num-results 100\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--api-key",
        required=True,
        help="Brave Search API Key (X-Subscription-Token)",
    )

    q = p.add_mutually_exclusive_group(required=True)
    q.add_argument("--query", metavar="TERM", help="Einzelner Suchbegriff")
    q.add_argument(
        "--queries",
        metavar="T1,T2,...",
        help="Mehrere Suchbegriffe, kommasepariert",
    )
    q.add_argument(
        "--queries-file",
        type=Path,
        metavar="PATH",
        help="Datei mit einem Suchbegriff pro Zeile (Leerzeilen und '#'-Kommentare werden ignoriert)",
    )

    p.add_argument(
        "--num-results",
        type=int,
        default=20,
        metavar="N",
        help=f"Max. Treffer pro Begriff (1-{MAX_RESULTS_PER_QUERY}, paginiert in 20er-Blöcken; default: 20)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help="CSV-Pfad (default: output/brave_search_<timestamp>.csv)",
    )
    p.add_argument(
        "--search-lang",
        default="de",
        metavar="LANG",
        help="Sprache der Suchergebnisse (search_lang, z.B. de, en; default: de)",
    )
    p.add_argument(
        "--country",
        default="DE",
        metavar="CC",
        help="Ländercode für Geo-Bias (country, z.B. DE, US; default: DE)",
    )
    p.add_argument(
        "--ui-lang",
        default="de-DE",
        metavar="HL",
        help="UI-Sprache (ui_lang, z.B. de-DE, en-US; default: de-DE)",
    )
    p.add_argument(
        "--safesearch",
        choices=["off", "moderate", "strict"],
        default="off",
        help="SafeSearch (default: off)",
    )
    p.add_argument(
        "--site",
        default=None,
        metavar="DOMAIN",
        help="Suche auf eine Domain einschränken (wird als 'site:DOMAIN' dem Query vorangestellt)",
    )
    p.add_argument(
        "--freshness",
        default=None,
        metavar="EXPR",
        help=(
            "Zeitfilter (freshness): pd=letzter Tag, pw=letzte Woche, pm=letzter Monat, "
            "py=letztes Jahr, oder YYYY-MM-DDtoYYYY-MM-DD für custom range"
        ),
    )
    p.add_argument(
        "--extra-snippets",
        action="store_true",
        help="Zusätzliche Snippets pro Ergebnis anfordern (erfordert AI/Data Plan)",
    )
    p.add_argument(
        "--delay-ms",
        type=int,
        default=200,
        metavar="MS",
        help="Pause zwischen Requests in Millisekunden (default: 200)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Nur ausgeben, was gequeried würde, ohne API-Call",
    )
    return p.parse_args(argv)


def _load_queries_file(path: Path) -> List[str]:
    if not path.exists():
        sys.exit(f"error: --queries-file not found: {path}")
    queries: List[str] = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            queries.append(line)
    if not queries:
        sys.exit(f"error: --queries-file '{path}' is empty")
    return queries


def _build_config(args: argparse.Namespace) -> SearchConfig:
    if args.query:
        queries = [args.query.strip()]
    elif args.queries:
        queries = [q.strip() for q in args.queries.split(",") if q.strip()]
    else:
        queries = _load_queries_file(args.queries_file)

    if not queries:
        sys.exit("error: no non-empty queries provided")

    return SearchConfig(
        api_key=args.api_key.strip(),
        queries=queries,
        num_results=args.num_results,
        output=args.output,
        search_lang=args.search_lang.strip(),
        country=args.country.strip(),
        ui_lang=args.ui_lang.strip(),
        safesearch=args.safesearch,
        site=(args.site.strip() if args.site else None),
        freshness=(args.freshness.strip() if args.freshness else None),
        extra_snippets=args.extra_snippets,
        delay_ms=args.delay_ms,
        dry_run=args.dry_run,
    )


# ============================================================
# API CLIENT
# ============================================================

class RateLimitError(RuntimeError):
    """Wird geworfen bei HTTP 429 nach Retry."""


class BraveSearchClient:
    """Client für die Brave Search API."""

    def __init__(self, cfg: SearchConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": cfg.api_key,
        })
        self.request_count = 0
        self.total_cost_estimate = 0.0

    def _build_query(self, query: str) -> str:
        """Optionalen site:-Operator voranstellen."""
        if self.cfg.site:
            return f"site:{self.cfg.site} {query}"
        return query

    def _base_params(self, query: str, count: int, offset: int) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "q": self._build_query(query),
            "count": count,
            "offset": offset,
            "search_lang": self.cfg.search_lang,
            "country": self.cfg.country,
            "ui_lang": self.cfg.ui_lang,
            "safesearch": self.cfg.safesearch,
        }
        if self.cfg.freshness:
            params["freshness"] = self.cfg.freshness
        if self.cfg.extra_snippets:
            params["extra_snippets"] = "true"
        return params

    def search_page(
        self,
        query: str,
        offset: int,
        count: int,
        _retry: bool = False,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Liefert (items, more_results_available) für eine Ergebnisseite.

        offset ist die Seiten-Nummer (0-basiert), nicht der Ergebnis-Index.
        """
        count = max(1, min(count, RESULTS_PER_PAGE))

        if self.cfg.delay_ms > 0:
            time.sleep(self.cfg.delay_ms / 1000.0)

        params = self._base_params(query, count, offset)

        try:
            resp = self.session.get(BASE_URL, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request Fehler: {e}")
            return [], False

        self.request_count += 1
        self.total_cost_estimate += COST_PER_REQUEST_USD

        if resp.status_code == 200:
            data = resp.json()
            web = data.get("web") or {}
            items = web.get("results") or []
            query_meta = data.get("query") or {}
            more = query_meta.get("more_results_available", False)
            return items, more

        if resp.status_code == 429:
            if _retry:
                print("  ✗ Rate limit weiterhin aktiv — Suche für diesen Query abgebrochen.")
                return [], False
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else 30
            print(f"  ⚠ Rate limit (429) erreicht, warte {wait}s und versuche erneut...")
            time.sleep(wait)
            return self.search_page(query, offset, count, _retry=True)

        if resp.status_code == 401:
            print(f"  ✗ Authentifizierung fehlgeschlagen (401). API-Key prüfen.")
            return [], False

        if resp.status_code == 403:
            print(f"  ✗ Zugriff verweigert (403): {resp.text[:200]}")
            return [], False

        print(f"  ✗ API Fehler {resp.status_code}: {resp.text[:200]}")
        return [], False

    def search_all(self, query: str, max_results: int) -> Tuple[List[Dict[str, Any]], int]:
        """
        Paginiert über offset 0-9 solange more_results_available == True
        oder max_results erreicht ist.

        Gibt (items, pages_fetched) zurück.
        """
        max_results = max(1, min(max_results, MAX_RESULTS_PER_QUERY))
        collected: List[Dict[str, Any]] = []
        pages = 0

        for offset in range(MAX_OFFSET + 1):
            if len(collected) >= max_results:
                break

            remaining = max_results - len(collected)
            page_size = min(RESULTS_PER_PAGE, remaining)

            items, more = self.search_page(query, offset, page_size)
            pages += 1

            if items:
                collected.extend(items)

            if not items or not more:
                break

        return collected[:max_results], pages


# ============================================================
# DATENVERARBEITUNG
# ============================================================

CSV_HEADERS = [
    "query",
    "rank",
    "title",
    "url",
    "display_url",
    "description",
    "age",
    "language",
    "family_friendly",
    "page_fetched",
    "thumbnail_src",
    "extra_snippets",
    "fetched_at",
]

_WS_RE = re.compile(r"\s+")


def _collapse_ws(value: Optional[str]) -> str:
    if not value:
        return ""
    return _WS_RE.sub(" ", str(value)).strip()


def _get_thumbnail(item: Dict[str, Any]) -> str:
    thumb = item.get("thumbnail") or {}
    return thumb.get("src", "") or ""


def _get_extra_snippets(item: Dict[str, Any]) -> str:
    snippets = item.get("extra_snippets") or []
    if not snippets:
        return ""
    cleaned = [_collapse_ws(s) for s in snippets if s]
    return " ||| ".join(cleaned)


def extract_item(
    item: Dict[str, Any], query: str, rank: int, fetched_at: str
) -> Dict[str, str]:
    """Flacht ein Brave API web.results[]-Objekt für die CSV-Zeile ab."""
    meta = item.get("meta_url") or {}
    return {
        "query": query,
        "rank": str(rank),
        "title": _collapse_ws(item.get("title")),
        "url": item.get("url", "") or "",
        "display_url": meta.get("netloc", "") or item.get("url", ""),
        "description": _collapse_ws(item.get("description")),
        "age": item.get("age", "") or "",
        "language": item.get("language", "") or "",
        "family_friendly": str(item.get("family_friendly", "")),
        "page_fetched": item.get("page_fetched", "") or "",
        "thumbnail_src": _get_thumbnail(item),
        "extra_snippets": _get_extra_snippets(item),
        "fetched_at": fetched_at,
    }


# ============================================================
# CSV EXPORT
# ============================================================

def export_csv(rows: List[Dict[str, str]], filepath: Path) -> None:
    """Exportiert die Ergebnisse als CSV (`;`-delimited, UTF-8 BOM)."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_HEADERS})

    print(f"\n✅ CSV exportiert: {filepath}")
    print(f"   {len(rows)} Treffer gespeichert")


# ============================================================
# HAUPTPROGRAMM
# ============================================================

def main(argv: Optional[List[str]] = None) -> List[Dict[str, str]]:
    global _CFG
    args = _parse_args(argv)
    _CFG = _build_config(args)
    cfg = _CFG

    print("=" * 60)
    print("🔎 Brave Search API Scraper")
    print("=" * 60)

    if not cfg.api_key:
        sys.exit("error: --api-key fehlt oder ist leer.")

    n_queries = len(cfg.queries)
    pages_per_query = (cfg.num_results + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
    est_calls_max = n_queries * pages_per_query

    print(f"\n🔍 {n_queries} Suchbegriff(e), max. {cfg.num_results} Treffer pro Begriff")
    print(f"   Sprache: {cfg.search_lang} | Country: {cfg.country} | UI: {cfg.ui_lang} | safe: {cfg.safesearch}")
    if cfg.site:
        print(f"   Site-Filter: {cfg.site}")
    if cfg.freshness:
        print(f"   Freshness: {cfg.freshness}")
    if cfg.extra_snippets:
        print(f"   Extra Snippets: aktiviert")
    print(f"📊 Obere Schranke API-Calls: ~{est_calls_max} ({pages_per_query} Seite(n) × {n_queries} Begriff(e))")
    print(f"💰 Grobe Kosten-Schätzung (Max): ~${est_calls_max * COST_PER_REQUEST_USD:.2f} (nach Free Tier)")
    print(f"📝 Output: {cfg.output_csv}")

    if cfg.dry_run:
        print("\n🛈 --dry-run aktiv, kein API-Call. Geplante Queries:")
        for i, q in enumerate(cfg.queries, 1):
            print(f"   [{i}/{n_queries}] {q!r}")
        return []

    print()

    client = BraveSearchClient(cfg)
    rows: List[Dict[str, str]] = []
    aborted_after: Optional[int] = None

    try:
        for i, query in enumerate(cfg.queries, 1):
            print(f"[{i}/{n_queries}] {query!r}...", end=" ", flush=True)

            items, pages = client.search_all(query, cfg.num_results)

            fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            for rank, item in enumerate(items, 1):
                rows.append(extract_item(item, query, rank, fetched_at))
            print(f"{len(items)} Treffer ({pages} Seite{'n' if pages != 1 else ''})")
    except KeyboardInterrupt:
        print("\n\n⚠  Abgebrochen!")

    print()
    print("=" * 60)
    print("📊 ERGEBNIS")
    print("=" * 60)
    print(f"   Suchbegriffe abgefragt: {aborted_after if aborted_after is not None else n_queries}/{n_queries}")
    print(f"   CSV-Zeilen gesamt:      {len(rows)}")
    print(f"   API-Calls gesamt:       {client.request_count}")
    print(f"   Geschätzte Kosten:      ~${client.total_cost_estimate:.2f}")
    if aborted_after is not None and aborted_after < n_queries:
        not_run = cfg.queries[aborted_after:]
        print(
            f"   Nicht ausgeführt ({len(not_run)}): {', '.join(repr(q) for q in not_run[:5])}"
            + (" ..." if len(not_run) > 5 else "")
        )
    print()

    if rows:
        export_csv(rows, cfg.output_csv)
    else:
        print("⚠  Keine Treffer — keine CSV geschrieben.")

    return rows


if __name__ == "__main__":
    main()