#!/usr/bin/env python3
"""
Brave Search: Domains aus CSV-Zeilen ermitteln
==============================================
Liest eine CSV, baut pro Zeile einen Suchbegriff (z. B. Name + Stadt),
fragt die Brave Search API ab und schreibt die beste Treffer-Domain
(standardmäßig Rank 1) in eine erweiterte CSV.

Basiert auf dem API-Client in brave_search_api.py.

Abhängigkeiten:
    pip install requests

Nutzung (Projektroot):

    python scripts/scrapers/brave_search_csv_domains.py \\
        --api-key KEY \\
        --input output/photovoltaikforum_companies.csv \\
        --name-column name --city-column city

    python scripts/scrapers/brave_search_csv_domains.py \\
        --api-key KEY \\
        --input leads.csv \\
        --query-column search_query \\
        --limit 50

    python scripts/scrapers/brave_search_csv_domains.py \\
        --api-key KEY \\
        --input leads.csv \\
        --query-template "{name} {city} photovoltaik" \\
        --resume

Kosten/Quota: siehe brave_search_api.py (1 API-Call pro Zeile bei --num-results <= 20).
"""

from __future__ import annotations

import argparse
import csv
import re
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

_SCRAPERS_DIR = Path(__file__).resolve().parent
if str(_SCRAPERS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPERS_DIR))

from brave_search_api import (  # noqa: E402
    COST_PER_REQUEST_USD,
    BraveSearchClient,
    SearchConfig,
)

# ============================================================
# KONFIGURATION
# ============================================================

DEFAULT_QUERY_TEMPLATE = "{name} {city}"
DEFAULT_DOMAIN_COLUMN = "domain"
DEFAULT_NUM_RESULTS = 1

ENRICHMENT_COLUMNS = [
    "brave_query",
    "brave_rank",
    "brave_domain",
    "brave_url",
    "brave_title",
    "brave_description",
    "brave_fetched_at",
    "brave_error",
]

_CFG: Optional["CsvDomainConfig"] = None

_WS_RE = re.compile(r"\s+")


@dataclass
class CsvDomainConfig:
    api_key: str
    input_csv: Path
    output_csv: Path
    delimiter: str
    query_template: Optional[str]
    query_column: Optional[str]
    name_column: str
    city_column: str
    extra_columns: List[str]
    domain_column: str
    num_results: int
    search_lang: str
    country: str
    ui_lang: str
    safesearch: str
    site: Optional[str]
    freshness: Optional[str]
    extra_snippets: bool
    delay_ms: int
    limit: Optional[int]
    offset: int
    resume: bool
    dry_run: bool
    checkpoint_every: int

    @property
    def template_columns(self) -> List[str]:
        cols = [self.name_column, self.city_column, *self.extra_columns]
        seen: set[str] = set()
        out: List[str] = []
        for c in cols:
            if c and c not in seen:
                seen.add(c)
                out.append(c)
        return out


# ============================================================
# HILFSFUNKTIONEN
# ============================================================


def _collapse_ws(value: Optional[str]) -> str:
    if not value:
        return ""
    return _WS_RE.sub(" ", str(value)).strip()


def normalize_domain(value: str) -> str:
    value = (value or "").strip().lower()
    if not value:
        return ""
    if "://" in value:
        host = urlparse(value).netloc
    else:
        host = value.split("/")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def domain_from_result(item: Dict[str, Any]) -> str:
    meta = item.get("meta_url") or {}
    netloc = meta.get("netloc") or ""
    if netloc:
        return normalize_domain(netloc)
    return normalize_domain(item.get("url", "") or "")


def build_query_from_template(template: str, row: Dict[str, str]) -> str:
    mapping = {k: _collapse_ws(v) for k, v in row.items()}
    try:
        return _collapse_ws(template.format_map(mapping))
    except KeyError as exc:
        missing = str(exc).strip("'")
        raise ValueError(
            f"Query template references column {missing!r} which is missing in CSV row"
        ) from exc


def row_query(cfg: CsvDomainConfig, row: Dict[str, str]) -> str:
    if cfg.query_column:
        return _collapse_ws(row.get(cfg.query_column, ""))
    if cfg.query_template:
        return build_query_from_template(cfg.query_template, row)
    raise RuntimeError("No query source configured")


def row_key(row: Dict[str, str], query: str) -> str:
    """Stable key for --resume (query string)."""
    return query


def read_csv_rows(path: Path, delimiter: str) -> tuple[List[str], List[Dict[str, str]]]:
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        if not reader.fieldnames:
            sys.exit(f"error: CSV has no header: {path}")
        fieldnames = list(reader.fieldnames)
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def load_resume_keys(
    path: Path, delimiter: str, domain_column: str
) -> Set[str]:
    if not path.exists():
        return set()
    _, rows = read_csv_rows(path, delimiter)
    keys: Set[str] = set()
    for row in rows:
        domain = normalize_domain(
            row.get(domain_column, "") or row.get("brave_domain", "")
        )
        query = _collapse_ws(row.get("brave_query", ""))
        if domain and query:
            keys.add(query)
    return keys


def empty_enrichment(query: str, error: str = "") -> Dict[str, str]:
    return {
        "brave_query": query,
        "brave_rank": "",
        "brave_domain": "",
        "brave_url": "",
        "brave_title": "",
        "brave_description": "",
        "brave_fetched_at": "",
        "brave_error": error,
    }


def enrichment_from_result(
    query: str, item: Dict[str, Any], rank: int, fetched_at: str
) -> Dict[str, str]:
    return {
        "brave_query": query,
        "brave_rank": str(rank),
        "brave_domain": domain_from_result(item),
        "brave_url": item.get("url", "") or "",
        "brave_title": _collapse_ws(item.get("title")),
        "brave_description": _collapse_ws(item.get("description")),
        "brave_fetched_at": fetched_at,
        "brave_error": "",
    }


def merge_output_row(
    row: Dict[str, str],
    enrichment: Dict[str, str],
    domain_column: str,
) -> Dict[str, str]:
    out = dict(row)
    out.update(enrichment)
    out[domain_column] = enrichment.get("brave_domain", "")
    return out


def output_fieldnames(input_fieldnames: List[str], domain_column: str) -> List[str]:
    names = list(input_fieldnames)
    for col in ENRICHMENT_COLUMNS:
        if col not in names:
            names.append(col)
    if domain_column not in names:
        names.append(domain_column)
    return names


def write_csv(
    path: Path,
    fieldnames: List[str],
    rows: List[Dict[str, str]],
    delimiter: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def append_csv_rows(
    path: Path,
    fieldnames: List[str],
    rows: List[Dict[str, str]],
    delimiter: str,
    write_header: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if write_header else "a"
    with open(path, mode, newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


# ============================================================
# CLI
# ============================================================


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Brave Search API — pro CSV-Zeile die beste Domain ermitteln.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Beispiel PV-Forum:\n"
            "  %(prog)s --api-key KEY -i output/photovoltaikforum_companies.csv "
            "--name-column name --city-column city\n"
        ),
    )
    p.add_argument("--api-key", required=True, help="Brave Search API Key")
    p.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Input-CSV (;-delimited, UTF-8 BOM)",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output-CSV (default: <input_stem>_domains.csv)",
    )
    p.add_argument(
        "--delimiter",
        default=";",
        help="CSV delimiter (default: ;)",
    )

    q = p.add_mutually_exclusive_group()
    q.add_argument(
        "--query-column",
        metavar="COL",
        help="Fertige Suchzeile aus dieser Spalte (kein Template)",
    )
    q.add_argument(
        "--query-template",
        default=DEFAULT_QUERY_TEMPLATE,
        metavar="FMT",
        help=f"Python format string für die Query (default: {DEFAULT_QUERY_TEMPLATE!r})",
    )

    p.add_argument(
        "--name-column",
        default="name",
        help="Spalte für Firmenname (default: name)",
    )
    p.add_argument(
        "--city-column",
        default="city",
        help="Spalte für Stadt/PLZ (default: city)",
    )
    p.add_argument(
        "--extra-columns",
        default="",
        metavar="C1,C2",
        help="Zusätzliche Spalten für --query-template, kommasepariert",
    )
    p.add_argument(
        "--domain-column",
        default=DEFAULT_DOMAIN_COLUMN,
        help=f"Output-Spalte für die Domain (default: {DEFAULT_DOMAIN_COLUMN})",
    )
    p.add_argument(
        "--num-results",
        type=int,
        default=DEFAULT_NUM_RESULTS,
        metavar="N",
        help=f"Max. Treffer pro Zeile; Domain aus Rank 1 (default: {DEFAULT_NUM_RESULTS})",
    )
    p.add_argument(
        "--search-lang",
        default="de",
        help="search_lang (default: de)",
    )
    p.add_argument(
        "--country",
        default="DE",
        help="country (default: DE)",
    )
    p.add_argument(
        "--ui-lang",
        default="de-DE",
        help="ui_lang (default: de-DE)",
    )
    p.add_argument(
        "--safesearch",
        choices=["off", "moderate", "strict"],
        default="off",
    )
    p.add_argument(
        "--site",
        default=None,
        help="site:DOMAIN Filter",
    )
    p.add_argument(
        "--freshness",
        default=None,
        help="freshness filter (pd, pw, pm, py, or date range)",
    )
    p.add_argument(
        "--extra-snippets",
        action="store_true",
    )
    p.add_argument(
        "--delay-ms",
        type=int,
        default=200,
        help="Pause zwischen API-Calls (default: 200)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nur die ersten N Zeilen verarbeiten (nach --offset)",
    )
    p.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Erste N Zeilen der Input-CSV überspringen (default: 0)",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Zeilen mit gleicher brave_query und gesetzter Domain in Output überspringen",
    )
    p.add_argument(
        "--checkpoint-every",
        type=int,
        default=0,
        metavar="N",
        help="Output alle N neuen Zeilen anhängen (0 = nur am Ende schreiben)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Queries anzeigen, kein API-Call",
    )
    return p.parse_args(argv)


def _build_config(args: argparse.Namespace) -> CsvDomainConfig:
    if not args.input.exists():
        sys.exit(f"error: input not found: {args.input}")

    output = args.output
    if output is None:
        output = args.input.with_name(f"{args.input.stem}_domains.csv")

    extra_columns = [c.strip() for c in args.extra_columns.split(",") if c.strip()]

    query_template: Optional[str] = None
    query_column: Optional[str] = None
    if args.query_column:
        query_column = args.query_column.strip()
    else:
        query_template = args.query_template

    return CsvDomainConfig(
        api_key=args.api_key.strip(),
        input_csv=args.input,
        output_csv=output,
        delimiter=args.delimiter,
        query_template=query_template,
        query_column=query_column,
        name_column=args.name_column.strip(),
        city_column=args.city_column.strip(),
        extra_columns=extra_columns,
        domain_column=args.domain_column.strip(),
        num_results=max(1, int(args.num_results)),
        search_lang=args.search_lang.strip(),
        country=args.country.strip(),
        ui_lang=args.ui_lang.strip(),
        safesearch=args.safesearch,
        site=(args.site.strip() if args.site else None),
        freshness=(args.freshness.strip() if args.freshness else None),
        extra_snippets=args.extra_snippets,
        delay_ms=max(0, int(args.delay_ms)),
        limit=args.limit,
        offset=max(0, int(args.offset)),
        resume=args.resume,
        dry_run=args.dry_run,
        checkpoint_every=max(0, int(args.checkpoint_every)),
    )


def _validate_columns(cfg: CsvDomainConfig, fieldnames: List[str]) -> None:
    if cfg.query_column:
        if cfg.query_column not in fieldnames:
            sys.exit(
                f"error: --query-column {cfg.query_column!r} not in CSV. "
                f"Available: {', '.join(fieldnames)}"
            )
        return

    template_cols = set(cfg.template_columns)
    formatter = string.Formatter()
    required = {
        field_name
        for _, field_name, _, _ in formatter.parse(cfg.query_template or "")
        if field_name
    }
    missing = required - set(fieldnames)
    if missing:
        sys.exit(
            f"error: query template needs columns {sorted(missing)} but CSV has: "
            f"{', '.join(fieldnames)}"
        )


# ============================================================
# HAUPTPROGRAMM
# ============================================================


def main(argv: Optional[List[str]] = None) -> int:
    global _CFG
    args = _parse_args(argv)
    cfg = _build_config(args)
    _CFG = cfg

    fieldnames, rows = read_csv_rows(cfg.input_csv, cfg.delimiter)
    _validate_columns(cfg, fieldnames)

    if cfg.offset:
        rows = rows[cfg.offset :]
    if cfg.limit is not None:
        rows = rows[: cfg.limit]

    out_fields = output_fieldnames(fieldnames, cfg.domain_column)
    written: List[Dict[str, str]] = []
    if cfg.resume and cfg.output_csv.exists() and cfg.checkpoint_every == 0:
        _, written = read_csv_rows(cfg.output_csv, cfg.delimiter)
    resume_keys = (
        load_resume_keys(cfg.output_csv, cfg.delimiter, cfg.domain_column)
        if cfg.resume
        else set()
    )

    print("=" * 60)
    print("Brave Search CSV → Domain")
    print("=" * 60)
    print(f"Input:   {cfg.input_csv} ({len(rows)} rows to process)")
    print(f"Output:  {cfg.output_csv}")
    if cfg.query_column:
        print(f"Query:   column {cfg.query_column!r}")
    else:
        print(f"Query:   template {cfg.query_template!r}")
    print(f"Domain:  rank 1 of up to {cfg.num_results} result(s) → column {cfg.domain_column!r}")
    est_calls = len(rows) - len(resume_keys) if cfg.resume else len(rows)
    print(f"Est. API calls: ~{est_calls} (~${est_calls * COST_PER_REQUEST_USD:.2f})")
    if cfg.resume and resume_keys:
        print(f"Resume:  skipping {len(resume_keys)} queries already in output")

    if cfg.dry_run:
        print("\n--dry-run: sample queries")
        for i, row in enumerate(rows[:10], 1):
            q = row_query(cfg, row)
            print(f"  [{i}] {q!r}")
        if len(rows) > 10:
            print(f"  ... and {len(rows) - 10} more")
        return 0

    search_cfg = SearchConfig(
        api_key=cfg.api_key,
        queries=["__csv_batch__"],
        num_results=cfg.num_results,
        output=None,
        search_lang=cfg.search_lang,
        country=cfg.country,
        ui_lang=cfg.ui_lang,
        safesearch=cfg.safesearch,
        site=cfg.site,
        freshness=cfg.freshness,
        extra_snippets=cfg.extra_snippets,
        delay_ms=cfg.delay_ms,
        dry_run=False,
    )
    client = BraveSearchClient(search_cfg)

    skipped = 0
    errors = 0
    found = 0
    checkpoint_buffer: List[Dict[str, str]] = []
    write_header = not (cfg.checkpoint_every > 0 and cfg.output_csv.exists())

    try:
        for i, row in enumerate(rows, 1):
            query = row_query(cfg, row)
            if not query:
                enrichment = empty_enrichment("", "empty_query")
                errors += 1
            elif cfg.resume and row_key(row, query) in resume_keys:
                skipped += 1
                continue
            else:
                print(f"[{i}/{len(rows)}] {query!r}...", end=" ", flush=True)
                items, pages = client.search_all(query, cfg.num_results)
                fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

                if items:
                    enrichment = enrichment_from_result(query, items[0], 1, fetched_at)
                    found += 1
                    print(f"→ {enrichment['brave_domain'] or '?'} ({pages} page(s))")
                else:
                    enrichment = empty_enrichment(query, "no_results")
                    errors += 1
                    print("→ (no results)")

            out_row = merge_output_row(row, enrichment, cfg.domain_column)
            written.append(out_row)
            checkpoint_buffer.append(out_row)

            if cfg.checkpoint_every > 0 and len(checkpoint_buffer) >= cfg.checkpoint_every:
                append_csv_rows(
                    cfg.output_csv,
                    out_fields,
                    checkpoint_buffer,
                    cfg.delimiter,
                    write_header,
                )
                write_header = False
                checkpoint_buffer.clear()

    except KeyboardInterrupt:
        print("\n\nInterrupted — saving progress...")

    if checkpoint_buffer:
        append_csv_rows(
            cfg.output_csv,
            out_fields,
            checkpoint_buffer,
            cfg.delimiter,
            write_header,
        )
        write_header = False
    elif written:
        write_csv(cfg.output_csv, out_fields, written, cfg.delimiter)

    print()
    print("=" * 60)
    print(f"Output:   {len(written)} rows → {cfg.output_csv}")
    print(f"Domains:  {found}")
    print(f"No result / empty: {errors}")
    print(f"Skipped (resume): {skipped}")
    print(f"API calls: {client.request_count} (~${client.total_cost_estimate:.2f})")

    return 0 if written or skipped else 1


if __name__ == "__main__":
    sys.exit(main())
