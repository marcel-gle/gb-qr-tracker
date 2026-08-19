#!/usr/bin/env python3
"""
Score a CSV list with a prompt from scripts/business/prompts.json.

Derives the website domain from the E-Mail column if no domain/website column exists.
Keeps all original columns and appends scoring fields.

Usage:
  python scripts/business/score_list.py input.csv output.csv
  python scripts/business/score_list.py input.csv output.csv --prompt webdesigner_lead_qualifizierung
  python scripts/business/score_list.py input.csv output.csv --limit 5
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from campaign_pipeline.config import CampaignConfig, ScoreConfig
from campaign_pipeline.imprint.fetch import close_browser_pool
from campaign_pipeline.io.readers import load_csv_rows
from campaign_pipeline.io.writers import write_csv_rows
from campaign_pipeline.llm import create_llm
from campaign_pipeline.models import BusinessRow, normalize_domain
from campaign_pipeline.steps.run_scoring import _load_score_config
from campaign_pipeline.steps.scoring import DomainScoringService, apply_analysis_flat
from scripts.business.prompt_manager import get_prompt

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover

    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable


logger = logging.getLogger(__name__)

GENERIC_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "hotmail.com",
    "hotmail.de",
    "outlook.com",
    "outlook.de",
    "yahoo.com",
    "yahoo.de",
    "web.de",
    "gmx.de",
    "gmx.net",
    "icloud.com",
    "me.com",
    "t-online.de",
    "freenet.de",
    "aol.com",
    "live.de",
    "msn.com",
}

COLUMN_ALIASES = {
    "unternehmens name": "company_name",
    "firmenname": "company_name",
    "e-mail": "email",
    "email": "email",
    "telefon": "phone",
    "tel": "phone",
    "webseite": "domain",
    "website": "domain",
    "url": "domain",
}


def normalize_row_keys(row: Dict[str, str]) -> Dict[str, str]:
    out = dict(row)
    for key, value in row.items():
        alias = COLUMN_ALIASES.get((key or "").strip().lower())
        if alias:
            out[alias] = value
    return out


def row_to_business(row: Dict[str, str]) -> Tuple[BusinessRow | None, str]:
    mapped = normalize_row_keys(row)
    domain = normalize_domain(
        mapped.get("domain"),
        email_fallback=mapped.get("email") or mapped.get("E-Mail"),
    )
    if not domain:
        return None, "no_domain"
    if domain in GENERIC_EMAIL_DOMAINS:
        return None, "generic_email_domain"
    try:
        business = BusinessRow.from_dict(mapped)
    except ValueError as exc:
        return None, str(exc)
    for key, value in row.items():
        if key not in business.to_dict():
            business.extra[key] = value
    return business, ""


def merge_scored_row(original: Dict[str, str], business: BusinessRow) -> Dict[str, str]:
    out = dict(original)
    scored = business.to_dict()
    for key, value in scored.items():
        if key in {"domain"}:
            out["domain"] = value
            continue
        if key not in original or key.startswith("analysis_") or key in {
            "match_score",
            "score_raw",
            "score_scale",
            "score_field",
            "passed_score_filter",
            "domain_analysis_raw",
            "analysis_result",
            "company_name",
            "email",
            "phone",
        }:
            out[key] = "" if value is None else str(value)
    return out


def score_list(
    input_path: Path,
    output_path: Path,
    *,
    prompt_name: str,
    backend: str = "local",
    max_workers: int = 10,
    skip_existing: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    prompt = get_prompt(prompt_name)
    if prompt is None:
        raise RuntimeError(f"Prompt not found: {prompt_name}")

    rows, fieldnames, delimiter = load_csv_rows(input_path)
    score_config = _load_score_config(prompt_name, ScoreConfig())
    llm = create_llm(CampaignConfig(campaign_dir=input_path.parent, base_name="score_list", backend=backend, max_workers_llm=max_workers))  # type: ignore[arg-type]
    service = DomainScoringService(llm, prompt, score_config)

    extra_fields = [
        "domain",
        "match_score",
        "passed_score_filter",
        "analysis_result",
        "analysis_status",
    ]
    for col in extra_fields:
        if col not in fieldnames:
            fieldnames.append(col)

    to_score: list[tuple[int, BusinessRow]] = []
    skip_reasons: dict[str, int] = {}
    for idx, row in enumerate(rows):
        if skip_existing and (row.get("analysis_result") or row.get("match_score")):
            skip_reasons["existing"] = skip_reasons.get("existing", 0) + 1
            continue
        business, reason = row_to_business(row)
        if business is None:
            row["analysis_status"] = reason
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            continue
        to_score.append((idx, business))

    if limit is not None and limit > 0:
        to_score = to_score[:limit]

    stats = {
        "input_rows": len(rows),
        "to_score": len(to_score),
        "scored_ok": 0,
        "scored_failed": 0,
        "skipped": skip_reasons,
        "prompt": prompt_name,
    }

    def _worker(item: tuple[int, BusinessRow]) -> tuple[int, BusinessRow, bool]:
        idx, business = item
        try:
            ok = service.score_row(business)
            if ok:
                apply_analysis_flat(business)
            business.extra["analysis_status"] = "success" if ok else "scoring_failed"
            return idx, business, ok
        except Exception as exc:
            logger.warning("Scoring failed for %s: %s", business.domain, exc)
            business.extra["analysis_status"] = f"error: {exc}"
            return idx, business, False

    started = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_worker, item): item for item in to_score}
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Scoring"):
                idx, business, ok = fut.result()
                rows[idx] = merge_scored_row(rows[idx], business)
                if ok:
                    stats["scored_ok"] += 1
                else:
                    stats["scored_failed"] += 1
    finally:
        if prompt.content_extraction_browser_fallback:
            close_browser_pool()

    all_keys: list[str] = list(fieldnames)
    for row in rows:
        for col in row.keys():
            if col not in all_keys:
                all_keys.append(col)

    write_csv_rows(output_path, rows, fieldnames=all_keys, delimiter=delimiter)
    stats["elapsed_s"] = round(time.monotonic() - started, 1)
    stats["output"] = str(output_path)
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Score a CSV list (domain from E-Mail if needed)")
    parser.add_argument("input_csv", type=Path, help="Input CSV (needs E-Mail or domain column)")
    parser.add_argument("output_csv", type=Path, help="Output CSV with scoring columns")
    parser.add_argument("--prompt", default="webdesigner_lead_qualifizierung", help="Prompt name from prompts.json")
    parser.add_argument("--backend", choices=["local", "openai"], default="local")
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--no-skip-existing", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="Only score first N rows")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")

    if not args.input_csv.exists():
        print(f"Datei nicht gefunden: {args.input_csv}")
        return 1

    try:
        stats = score_list(
            args.input_csv,
            args.output_csv,
            prompt_name=args.prompt,
            backend=args.backend,
            max_workers=args.max_workers,
            skip_existing=not args.no_skip_existing,
            limit=args.limit,
        )
    except KeyboardInterrupt:
        print("\nAbgebrochen.")
        return 1
    except Exception as exc:
        logger.exception("Scoring failed")
        print(f"Fehler: {exc}")
        return 1

    print(f"\nFertig → {stats['output']}")
    print(f"  Eingabe: {stats['input_rows']} Zeilen")
    print(f"  Gescort: {stats['scored_ok']} ok, {stats['scored_failed']} fehlgeschlagen")
    if stats["skipped"]:
        print(f"  Übersprungen: {stats['skipped']}")
    print(f"  Dauer: {stats['elapsed_s']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
