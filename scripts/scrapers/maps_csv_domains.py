#!/usr/bin/env python3
"""
Google Places: offizielle Website pro CSV-Zeile ermitteln
=========================================================
Liest eine CSV, baut pro Zeile einen Suchbegriff (z. B. Name + Stadt),
fragt die Google Places API (New) Text Search ab und schreibt die offizielle
Website/Domain des besten Treffers in eine erweiterte CSV.

Zeilen, die bereits einen Wert in ``website``, ``domain`` oder ``webseite``
haben, werden immer übersprungen (kein API-Call; bestehende Werte bleiben).

Standardmäßig wird die **Input-CSV in-place** aktualisiert (neue Domains in
``website``/``domain``/``webseite`` geschrieben). Erneutes Ausführen holt nur
noch fehlende Domains. Mit ``-o`` kannst du weiterhin in eine andere Datei
schreiben. ``--limit`` begrenzt die Anzahl neuer API-Lookups, nicht die
Dateigröße — die volle CSV bleibt erhalten.

Anders als eine reine Web-Suche liefert Google Places die aufgelöste
Firmen-Entität samt ``websiteUri`` direkt zurück — meist die offizielle Seite.
Zusätzlich wird ein ``gmaps_match``-Signal (high/medium/low) berechnet, das
Namen und Stadt des Treffers mit der Eingabe abgleicht.

Basiert auf dem API-Client in maps_api.py.

Abhängigkeiten:
    pip install requests

Nutzung (Projektroot):

    python scripts/scrapers/maps_csv_domains.py \\
        --api-key KEY \\
        --input output/companies.csv \\
        --name-column name --city-column city

    python scripts/scrapers/maps_csv_domains.py \\
        --api-key KEY \\
        --input leads.csv \\
        --query-template "{name} {city}" \\
        --resume

Kosten/Quota:
    - Text Search (New) mit websiteUri → SKU „Text Search Enterprise“:
      $35 / 1.000 Requests *nach* dem Free Cap von 1.000/Monat.
    - Die Skript-Schätzung zeigt den Listenpreis und erwähnt den Free Cap.
    - 1 API-Call pro Zeile ohne bereits gesetzte website/domain/webseite.
"""

from __future__ import annotations

import argparse
import csv
import re
import string
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

_SCRAPERS_DIR = Path(__file__).resolve().parent
if str(_SCRAPERS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRAPERS_DIR))

from maps_api import (  # noqa: E402
    GooglePlacesClient,
    extract_domain,
)

# ============================================================
# KONFIGURATION
# ============================================================

DEFAULT_QUERY_TEMPLATE = "{name} {city}"
DEFAULT_DOMAIN_COLUMN = "domain"

# Zeilen mit bereits gesetzter Website/Domain in einer dieser Spalten werden
# immer übersprungen (kein API-Call, bestehende Werte bleiben erhalten).
EXISTING_WEBSITE_COLUMNS = ("website", "domain", "webseite")

# Schlanke Field-Mask: nur was für Website-Ermittlung + Matching gebraucht wird.
LEAN_FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.websiteUri",
    "places.nationalPhoneNumber",
])

# Pricing: requesting ``websiteUri`` (and phone) bills Text Search at the
# *Enterprise* SKU: $35 / 1,000 after the free monthly cap of 1,000
# (see https://developers.google.com/maps/billing-and-pricing/pricing).
# The estimate below is the *list price* and ignores free-tier credit.
COST_PER_REQUEST_USD = 0.035
FREE_MONTHLY_CAP = 1000  # Text Search Enterprise free usage cap

ENRICHMENT_COLUMNS = [
    "gmaps_query",
    "gmaps_domain",
    "gmaps_website",
    "gmaps_name",
    "gmaps_address",
    "gmaps_phone",
    "gmaps_place_id",
    "gmaps_match",
    "gmaps_fetched_at",
    "gmaps_error",
]

_CFG: Optional["CsvPlacesConfig"] = None

_WS_RE = re.compile(r"\s+")

# Rechtsformen/Füllwörter, die vor dem Namensvergleich entfernt werden
# (angelehnt an campaign_pipeline/imprint/northdata.py::_normalize_company).
_LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:gmbh|ug|ag|kg|ohg|gbr|mbh|mbb|se|e\.?\s?k\.?|e\.?\s?v\.?|"
    r"co\.?|kgaa|haftungsbeschränkt|und|&|\+)\b|\(.*?\)",
    re.IGNORECASE,
)

_PLZ_RE = re.compile(r"\b\d{5}\b")


@dataclass
class CsvPlacesConfig:
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
    language_code: str
    region_code: str
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


def existing_website_value(row: Dict[str, str]) -> str:
    """Erste nicht-leere Website/Domain aus website|domain|webseite (case-insensitive)."""
    wanted = {c.lower() for c in EXISTING_WEBSITE_COLUMNS}
    # Prefer the canonical column order when present.
    for col in EXISTING_WEBSITE_COLUMNS:
        val = _collapse_ws(row.get(col, ""))
        if val:
            return val
    for key, value in row.items():
        if key and key.lower() in wanted:
            val = _collapse_ws(value)
            if val:
                return val
    return ""


def row_has_existing_website(row: Dict[str, str]) -> bool:
    return bool(existing_website_value(row))


def _normalize_company(name: str) -> str:
    cleaned = _LEGAL_SUFFIX_RE.sub(" ", (name or "").lower())
    cleaned = re.sub(r"[^a-z0-9äöüß ]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _name_tokens(name: str) -> Set[str]:
    return {t for t in _normalize_company(name).split() if len(t) > 1}


def _names_overlap(input_name: str, place_name: str) -> bool:
    """Heuristik: verlangt sinnvolle Token-Überschneidung beider Namen."""
    a = _name_tokens(input_name)
    b = _name_tokens(place_name)
    if not a or not b:
        return False
    overlap = a & b
    return len(overlap) >= max(1, min(len(a), 2))


def _city_matches(input_city: str, place_address: str) -> bool:
    """True, wenn Stadtname oder PLZ der Eingabe in der Places-Adresse steht."""
    address = (place_address or "").lower()
    if not address:
        return False
    city = (input_city or "").lower()

    # PLZ-Abgleich (falls Eingabe eine 5-stellige PLZ enthält).
    for plz in _PLZ_RE.findall(input_city or ""):
        if plz in address:
            return True

    # Stadtname: nicht-PLZ-Tokens der Eingabe gegen die Adresse prüfen.
    city_wo_plz = _PLZ_RE.sub(" ", city)
    tokens = [t for t in re.split(r"[^a-z0-9äöüß]+", city_wo_plz) if len(t) > 2]
    return any(t in address for t in tokens)


def match_confidence(
    input_name: str, input_city: str, place_name: str, place_address: str
) -> str:
    """Bewertet, wie sicher der Places-Treffer zur Eingabe passt."""
    name_ok = _names_overlap(input_name, place_name)
    city_ok = _city_matches(input_city, place_address)
    if name_ok and city_ok:
        return "high"
    if name_ok or city_ok:
        return "medium"
    return "low"


def build_query_from_template(template: str, row: Dict[str, str]) -> str:
    mapping = {k: _collapse_ws(v) for k, v in row.items()}
    try:
        return _collapse_ws(template.format_map(mapping))
    except KeyError as exc:
        missing = str(exc).strip("'")
        raise ValueError(
            f"Query template references column {missing!r} which is missing in CSV row"
        ) from exc


def row_query(cfg: CsvPlacesConfig, row: Dict[str, str]) -> str:
    if cfg.query_column:
        return _collapse_ws(row.get(cfg.query_column, ""))
    if cfg.query_template:
        return build_query_from_template(cfg.query_template, row)
    raise RuntimeError("No query source configured")


def _place_field(place: Dict[str, Any], key: str) -> str:
    return _collapse_ws(place.get(key, "") or "")


def _display_name(place: Dict[str, Any]) -> str:
    dn = place.get("displayName") or {}
    if isinstance(dn, dict):
        return _collapse_ws(dn.get("text", ""))
    return _collapse_ws(dn)


def detect_delimiter(path: Path, sample_bytes: int = 8192) -> str:
    """Detect CSV delimiter from a file sample (``;``, ``,``, or tab).

    Prefers ``csv.Sniffer``; falls back to counting separators on the header line.
    """
    with open(path, encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(sample_bytes)
    if not sample.strip():
        return ";"

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        if dialect.delimiter:
            return dialect.delimiter
    except csv.Error:
        pass

    header = sample.splitlines()[0] if sample.splitlines() else sample
    candidates = [";", ",", "\t", "|"]
    counts = {d: header.count(d) for d in candidates}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ";"


def read_csv_rows(path: Path, delimiter: str) -> tuple[List[str], List[Dict[str, str]]]:
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        if not reader.fieldnames:
            sys.exit(f"error: CSV has no header: {path}")
        fieldnames = list(reader.fieldnames)
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def load_resume_keys(path: Path, delimiter: str, domain_column: str) -> Set[str]:
    if not path.exists():
        return set()
    _, rows = read_csv_rows(path, delimiter)
    keys: Set[str] = set()
    for row in rows:
        domain = _collapse_ws(
            row.get(domain_column, "") or row.get("gmaps_domain", "")
        )
        query = _collapse_ws(row.get("gmaps_query", ""))
        if domain and query:
            keys.add(query)
    return keys


def empty_enrichment(query: str, error: str = "") -> Dict[str, str]:
    return {
        "gmaps_query": query,
        "gmaps_domain": "",
        "gmaps_website": "",
        "gmaps_name": "",
        "gmaps_address": "",
        "gmaps_phone": "",
        "gmaps_place_id": "",
        "gmaps_match": "",
        "gmaps_fetched_at": "",
        "gmaps_error": error,
    }


def enrichment_from_place(
    query: str,
    place: Dict[str, Any],
    input_name: str,
    input_city: str,
    fetched_at: str,
) -> Dict[str, str]:
    website = _place_field(place, "websiteUri")
    place_name = _display_name(place)
    place_address = _place_field(place, "formattedAddress")
    return {
        "gmaps_query": query,
        "gmaps_domain": extract_domain(website),
        "gmaps_website": website,
        "gmaps_name": place_name,
        "gmaps_address": place_address,
        "gmaps_phone": _place_field(place, "nationalPhoneNumber"),
        "gmaps_place_id": _place_field(place, "id"),
        "gmaps_match": match_confidence(
            input_name, input_city, place_name, place_address
        ),
        "gmaps_fetched_at": fetched_at,
        "gmaps_error": "" if website else "no_website",
    }


def merge_output_row(
    row: Dict[str, str],
    enrichment: Dict[str, str],
    domain_column: str,
    fieldnames: List[str],
    *,
    preserve_existing_domain: bool = False,
) -> Dict[str, str]:
    out = dict(row)
    out.update(enrichment)
    if preserve_existing_domain:
        # Keep website/domain/webseite as they were; do not overwrite them.
        return out

    domain = enrichment.get("gmaps_domain", "") or ""
    website_url = enrichment.get("gmaps_website", "") or ""
    if domain:
        out[domain_column] = domain
        _fill_empty_website_columns(out, fieldnames, domain=domain, website_url=website_url)
    else:
        # Still ensure domain_column exists as empty when we looked up but found none.
        if domain_column not in out:
            out[domain_column] = ""
    return out


def _field_key(fieldnames: List[str], wanted: str) -> Optional[str]:
    wanted_l = wanted.lower()
    for name in fieldnames:
        if name and name.lower() == wanted_l:
            return name
    return None


def _fill_empty_website_columns(
    row: Dict[str, str],
    fieldnames: List[str],
    *,
    domain: str,
    website_url: str,
) -> None:
    """Write found domain/URL into empty website|domain|webseite columns."""
    for wanted in EXISTING_WEBSITE_COLUMNS:
        key = _field_key(fieldnames, wanted)
        if not key:
            continue
        if _collapse_ws(row.get(key, "")):
            continue
        if wanted == "domain":
            row[key] = domain
        else:
            row[key] = website_url or domain


def output_fieldnames(input_fieldnames: List[str], domain_column: str) -> List[str]:
    names = list(input_fieldnames)
    for col in ENRICHMENT_COLUMNS:
        if col not in names:
            names.append(col)
    # Prefer an existing website/domain/webseite column; else add domain_column.
    has_website_col = any(
        _field_key(names, c) for c in EXISTING_WEBSITE_COLUMNS
    )
    if not has_website_col and domain_column not in names:
        names.append(domain_column)
    elif domain_column not in names and _field_key(names, "domain") is None:
        # Keep domain_column for gmaps domain even if only website/webseite exist.
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


def write_csv_atomic(
    path: Path,
    fieldnames: List[str],
    rows: List[Dict[str, str]],
    delimiter: str,
) -> None:
    """Write via a sibling .tmp file, then replace — safe for in-place updates."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    write_csv(tmp, fieldnames, rows, delimiter)
    tmp.replace(path)

# ============================================================
# CLI
# ============================================================


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Google Places API — pro CSV-Zeile die offizielle Website ermitteln.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Beispiel:\n"
            "  %(prog)s --api-key KEY -i output/companies.csv "
            "--name-column name --city-column city\n"
        ),
    )
    p.add_argument("--api-key", required=True, help="Google Maps API Key (Places API New enabled)")
    p.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Input-CSV (UTF-8 BOM; delimiter auto-detected unless --delimiter is set)",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output-CSV (default: update --input in place)",
    )
    p.add_argument(
        "--delimiter",
        default=None,
        metavar="CHAR",
        help="CSV delimiter (default: auto-detect from input; e.g. ',' or ';')",
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
        "--language-code",
        default="de",
        help="languageCode (default: de)",
    )
    p.add_argument(
        "--region-code",
        default="DE",
        help="regionCode (default: DE)",
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
        help="Max. neue API-Lookups in diesem Lauf (Zeilen mit bestehender Website zählen nicht)",
    )
    p.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Erste N *fehlende* Domains überspringen (nach Skip bestehender Websites; default: 0)",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Zeilen mit gleicher gmaps_query und gesetzter Domain in Output überspringen",
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


def _build_config(args: argparse.Namespace) -> CsvPlacesConfig:
    if not args.input.exists():
        sys.exit(f"error: input not found: {args.input}")

    output = args.output
    if output is None:
        output = args.input  # in-place update by default

    if args.delimiter is None:
        delimiter = detect_delimiter(args.input)
    else:
        delimiter = args.delimiter
        # Allow common escape for tab: --delimiter $'\t' or --delimiter tab
        if delimiter.lower() in ("tab", "\\t"):
            delimiter = "\t"
        elif len(delimiter) != 1:
            sys.exit(f"error: --delimiter must be a single character, got {delimiter!r}")

    extra_columns = [c.strip() for c in args.extra_columns.split(",") if c.strip()]

    query_template: Optional[str] = None
    query_column: Optional[str] = None
    if args.query_column:
        query_column = args.query_column.strip()
    else:
        query_template = args.query_template

    return CsvPlacesConfig(
        api_key=args.api_key.strip(),
        input_csv=args.input,
        output_csv=output,
        delimiter=delimiter,
        query_template=query_template,
        query_column=query_column,
        name_column=args.name_column.strip(),
        city_column=args.city_column.strip(),
        extra_columns=extra_columns,
        domain_column=args.domain_column.strip(),
        language_code=args.language_code.strip(),
        region_code=args.region_code.strip(),
        delay_ms=max(0, int(args.delay_ms)),
        limit=args.limit,
        offset=max(0, int(args.offset)),
        resume=args.resume,
        dry_run=args.dry_run,
        checkpoint_every=max(0, int(args.checkpoint_every)),
    )


def _validate_columns(cfg: CsvPlacesConfig, fieldnames: List[str]) -> None:
    if cfg.query_column:
        if cfg.query_column not in fieldnames:
            sys.exit(
                f"error: --query-column {cfg.query_column!r} not in CSV. "
                f"Available: {', '.join(fieldnames)}"
            )
        return

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

    fieldnames, all_rows = read_csv_rows(cfg.input_csv, cfg.delimiter)
    _validate_columns(cfg, fieldnames)
    out_fields = output_fieldnames(fieldnames, cfg.domain_column)
    in_place = cfg.output_csv.resolve() == cfg.input_csv.resolve()

    # Indices that still need a lookup (no website/domain/webseite yet).
    pending_indices = [
        i for i, row in enumerate(all_rows) if not row_has_existing_website(row)
    ]
    if cfg.offset:
        pending_indices = pending_indices[cfg.offset :]
    if cfg.limit is not None:
        pending_indices = pending_indices[: cfg.limit]

    already_have = sum(1 for r in all_rows if row_has_existing_website(r))
    est_calls = len(pending_indices)

    print("=" * 60)
    print("Google Places CSV → Official Website")
    print("=" * 60)
    print(f"Input:   {cfg.input_csv} ({len(all_rows)} rows)")
    if in_place:
        print(f"Output:  {cfg.output_csv} (in-place update)")
    else:
        print(f"Output:  {cfg.output_csv}")
    delim_display = repr(cfg.delimiter) if cfg.delimiter in ("\t", " ") else cfg.delimiter
    print(f"Delimiter: {delim_display}")
    if cfg.query_column:
        print(f"Query:   column {cfg.query_column!r}")
    else:
        print(f"Query:   template {cfg.query_template!r}")
    print(f"Domain:  websiteUri → website/domain/webseite (+ column {cfg.domain_column!r})")
    print(f"Already have website: {already_have} row(s) — will skip")
    print(f"Pending lookups this run: {est_calls}")
    list_price = est_calls * COST_PER_REQUEST_USD
    print(
        f"Est. API calls: ~{est_calls} "
        f"(list price ~${list_price:.2f} @ ${COST_PER_REQUEST_USD*1000:.0f}/1k; "
        f"first {FREE_MONTHLY_CAP} Text Search Enterprise/month are free)"
    )

    if cfg.dry_run:
        print("\n--dry-run: sample queries (pending only)")
        for n, idx in enumerate(pending_indices[:10], 1):
            q = row_query(cfg, all_rows[idx])
            print(f"  [{n}] row {idx + 1}: {q!r}")
        if len(pending_indices) > 10:
            print(f"  ... and {len(pending_indices) - 10} more")
        return 0

    client = GooglePlacesClient(cfg.api_key, field_mask=LEAN_FIELD_MASK)

    skipped_existing = already_have
    empty_query = 0
    no_results = 0
    found = 0
    no_website = 0
    looked_up = 0
    match_counts = {"high": 0, "medium": 0, "low": 0}
    found_domains: List[tuple[str, str, str, str, str, str]] = []
    lookups_since_checkpoint = 0

    try:
        for n, idx in enumerate(pending_indices, 1):
            row = all_rows[idx]
            query = row_query(cfg, row)
            input_name = _collapse_ws(row.get(cfg.name_column, ""))
            input_city = _collapse_ws(row.get(cfg.city_column, ""))

            if not query:
                enrichment = empty_enrichment("", "empty_query")
                empty_query += 1
                all_rows[idx] = merge_output_row(
                    row, enrichment, cfg.domain_column, out_fields
                )
                continue

            print(
                f"[{n}/{len(pending_indices)}] (row {idx + 1}) {query!r}...",
                end=" ",
                flush=True,
            )
            if cfg.delay_ms > 0:
                time.sleep(cfg.delay_ms / 1000.0)
            places, _ = client.text_search(query)
            looked_up += 1
            lookups_since_checkpoint += 1
            fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

            if places:
                enrichment = enrichment_from_place(
                    query, places[0], input_name, input_city, fetched_at
                )
                if enrichment["gmaps_domain"]:
                    found += 1
                    found_domains.append(
                        (
                            input_name or query,
                            input_city,
                            enrichment["gmaps_domain"],
                            enrichment["gmaps_website"],
                            enrichment["gmaps_match"],
                            enrichment["gmaps_name"],
                        )
                    )
                else:
                    no_website += 1
                if enrichment["gmaps_match"] in match_counts:
                    match_counts[enrichment["gmaps_match"]] += 1
                print(
                    f"→ {enrichment['gmaps_domain'] or '(no website)'} "
                    f"[{enrichment['gmaps_match']}]"
                )
            else:
                enrichment = empty_enrichment(query, "no_results")
                no_results += 1
                print("→ (no results)")

            all_rows[idx] = merge_output_row(
                row, enrichment, cfg.domain_column, out_fields
            )

            if (
                cfg.checkpoint_every > 0
                and lookups_since_checkpoint >= cfg.checkpoint_every
            ):
                write_csv_atomic(
                    cfg.output_csv, out_fields, all_rows, cfg.delimiter
                )
                lookups_since_checkpoint = 0
                print(f"  💾 checkpoint → {cfg.output_csv}")

    except KeyboardInterrupt:
        print("\n\nInterrupted — saving progress...")

    write_csv_atomic(cfg.output_csv, out_fields, all_rows, cfg.delimiter)

    not_found = no_website + no_results
    list_price_actual = client.request_count * COST_PER_REQUEST_USD

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Rows in file:                {len(all_rows)}")
    print(f"  Skipped (already had web):   {skipped_existing}")
    print(f"  Empty query (no name/city):  {empty_query}")
    print(f"  Looked up via Places:        {looked_up}")
    print("  " + "-" * 40)
    print(f"  New domains found:           {found}")
    print(f"  Not found:                   {not_found}")
    print(f"    - place, but no website:   {no_website}")
    print(f"    - no place at all:         {no_results}")
    if looked_up:
        print(f"  Hit rate (found/looked up):  {found / looked_up * 100:.1f}%")
    print("  " + "-" * 40)
    print(
        f"  Match quality:  high={match_counts['high']} "
        f"medium={match_counts['medium']} low={match_counts['low']}"
    )
    print("  " + "-" * 40)
    print(f"  API requests made:           {client.request_count}")
    print(
        f"  List price (if over free cap): ~${list_price_actual:.2f} "
        f"(${COST_PER_REQUEST_USD*1000:.0f}/1k Text Search Enterprise)"
    )
    print(
        f"  Free monthly cap:            {FREE_MONTHLY_CAP} "
        f"(actual bill is $0 while under this cap across the billing account)"
    )
    print(f"  Saved to:                    {cfg.output_csv}")
    if in_place:
        print("  Mode:                        in-place (rerun skips filled websites)")
    print("=" * 60)

    if found_domains:
        print()
        print("=" * 60)
        print(f"NEW DOMAINS FOUND ({len(found_domains)}) — please review")
        print("=" * 60)
        name_w = min(max(max((len(n) for n, *_ in found_domains), default=5), 5), 36)
        city_w = min(max(max((len(c) for _, c, *_ in found_domains), default=4), 4), 18)
        dom_w = min(max(max((len(d) for _, _, d, *_ in found_domains), default=6), 6), 32)
        print(
            f"  {'#':>3}  {'match':<6}  {'input':<{name_w}}  "
            f"{'city':<{city_w}}  {'domain':<{dom_w}}  place / url"
        )
        print("  " + "-" * 72)
        for idx, (iname, icity, domain, website, match, pname) in enumerate(
            found_domains, 1
        ):
            in_disp = (iname[: name_w - 1] + "…") if len(iname) > name_w else iname
            city_disp = (icity[: city_w - 1] + "…") if len(icity) > city_w else icity
            place_note = ""
            if pname and pname.lower() != iname.lower():
                place_note = pname
            extra = place_note
            if website:
                extra = f"{place_note} | {website}" if place_note else website
            print(
                f"  {idx:>3}  {match:<6}  {in_disp:<{name_w}}  "
                f"{city_disp:<{city_w}}  {domain:<{dom_w}}  {extra}"
            )
        print("=" * 60)

    return 0 if looked_up or found or skipped_existing else 1


if __name__ == "__main__":
    sys.exit(main())
