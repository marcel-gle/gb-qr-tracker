#!/usr/bin/env python3
"""
Backfill missing customer overlay phone numbers from campaign target import rows.

Source:
  campaigns/{campaignId}/targets/{targetId}.import_row

Target:
  customers/{ownerId}/businesses/{businessId}.phone

Primary phone source field in import_row:
  - "Generic Company Phones"

Examples:
  # Dry-run scoped to owner + campaign on dev
  python scripts/migrations/backfill_overlay_phones_from_targets.py \
    --env dev \
    --owner-id YOUR_OWNER_ID \
    --campaign-id YOUR_CAMPAIGN_ID \
    --dry-run

  # Write mode on prod, overwrite existing phone values
  python scripts/migrations/backfill_overlay_phones_from_targets.py \
    --env prod \
    --owner-id YOUR_OWNER_ID \
    --campaign-id YOUR_CAMPAIGN_ID \
    --overwrite-existing
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Dict, Optional

from google.cloud import firestore

DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"
BATCH_SIZE = 450


def get_project_for_env(env: str) -> str:
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    raise ValueError(f"Unknown env: {env!r}")


def _get_ci(row: Dict, *names: str) -> Optional[str]:
    lower_map = {}
    for key in row.keys():
        if isinstance(key, str):
            lower_map[key.lower()] = key
    for name in names:
        mapped = lower_map.get(name.lower())
        if mapped is not None:
            value = row.get(mapped)
            if value is None:
                return None
            return str(value)
    return None


def extract_phone(import_row: Dict) -> Optional[str]:
    """
    Extract phone with priority on the explicitly requested source field.
    Fallback aliases are included for resiliency.
    """
    generic_phones = _get_ci(import_row, "Generic Company Phones")
    if generic_phones and generic_phones.strip():
        return generic_phones.strip()

    prefix = _get_ci(
        import_row,
        "Vorwahl Telefon",
        "Vorwahl",
        "Telefon Vorwahl",
        "phone_prefix",
    )
    number = _get_ci(
        import_row,
        "Telefonnummer",
        "Telefon",
        "Phone",
        "Tel.",
        "Tel",
        "phone",
        "mobil",
        "handy",
    )
    prefix = (prefix or "").strip()
    number = (number or "").strip()
    combined = " ".join(part for part in (prefix, number) if part).strip()
    return combined or None


@dataclass
class Stats:
    targets_scanned: int = 0
    targets_missing_import_row: int = 0
    targets_missing_business_ref: int = 0
    rows_without_phone: int = 0
    overlay_missing: int = 0
    overlay_phone_already_set: int = 0
    overlays_to_update: int = 0
    overlays_updated: int = 0
    errors: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill customers/{ownerId}/businesses phone field from "
            "campaign target import_row data."
        )
    )
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument(
        "--project",
        default=None,
        help="Optional GCP project ID override (otherwise derived from --env).",
    )
    parser.add_argument("--database", default=DEFAULT_DATABASE_ID)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write any changes, only report stats and sample updates.",
    )
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Overwrite existing non-empty overlay phone values.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Number of proposed/updated rows to print as sample (default: 20).",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> int:
    project_id = args.project or get_project_for_env(args.env)
    db = firestore.Client(project=project_id, database=args.database)

    campaign_ref = db.collection("campaigns").document(args.campaign_id)
    targets = campaign_ref.collection("targets").stream()

    stats = Stats()
    sample_updates: list[str] = []
    batch = db.batch()
    pending_writes = 0

    print("=" * 80)
    print("Backfill overlay phones from campaign targets")
    print("=" * 80)
    print(f"Project:             {project_id}")
    print(f"Database:            {args.database}")
    print(f"Environment:         {args.env}")
    print(f"Owner ID:            {args.owner_id}")
    print(f"Campaign ID:         {args.campaign_id}")
    print(f"Dry run:             {args.dry_run}")
    print(f"Overwrite existing:  {args.overwrite_existing}")
    print("-" * 80)

    for target_doc in targets:
        stats.targets_scanned += 1
        target_data = target_doc.to_dict() or {}

        import_row = target_data.get("import_row")
        if not isinstance(import_row, dict):
            stats.targets_missing_import_row += 1
            continue

        business_ref = target_data.get("business_ref")
        if not business_ref or not hasattr(business_ref, "id"):
            stats.targets_missing_business_ref += 1
            continue

        phone = extract_phone(import_row)
        if not phone:
            stats.rows_without_phone += 1
            continue

        overlay_ref = (
            db.collection("customers")
            .document(args.owner_id)
            .collection("businesses")
            .document(business_ref.id)
        )

        try:
            overlay_snap = overlay_ref.get()
            if not overlay_snap.exists:
                stats.overlay_missing += 1
                continue

            overlay_data = overlay_snap.to_dict() or {}
            current_phone = str(overlay_data.get("phone") or "").strip()
            if current_phone and not args.overwrite_existing:
                stats.overlay_phone_already_set += 1
                continue

            stats.overlays_to_update += 1
            if len(sample_updates) < max(0, args.sample_limit):
                sample_updates.append(
                    f"{overlay_ref.path} | old='{current_phone}' | new='{phone}'"
                )

            if args.dry_run:
                continue

            payload = {
                "phone": phone,
                "updated_at": firestore.SERVER_TIMESTAMP,
                "phone_backfilled_from_campaign": args.campaign_id,
            }
            batch.set(overlay_ref, payload, merge=True)
            pending_writes += 1
            stats.overlays_updated += 1

            if pending_writes >= BATCH_SIZE:
                batch.commit()
                batch = db.batch()
                pending_writes = 0

        except Exception as exc:
            stats.errors += 1
            print(
                f"[ERROR] target={target_doc.id} "
                f"business_id={getattr(business_ref, 'id', '?')}: {exc}"
            )

    if not args.dry_run and pending_writes > 0:
        try:
            batch.commit()
        except Exception as exc:
            stats.errors += pending_writes
            print(f"[ERROR] final batch commit failed ({pending_writes} ops): {exc}")

    print("-" * 80)
    print("Stats")
    print(f"targets_scanned:               {stats.targets_scanned}")
    print(f"targets_missing_import_row:    {stats.targets_missing_import_row}")
    print(f"targets_missing_business_ref:  {stats.targets_missing_business_ref}")
    print(f"rows_without_phone:            {stats.rows_without_phone}")
    print(f"overlay_missing:               {stats.overlay_missing}")
    print(f"overlay_phone_already_set:     {stats.overlay_phone_already_set}")
    print(f"overlays_to_update:            {stats.overlays_to_update}")
    print(f"overlays_updated:              {stats.overlays_updated}")
    print(f"errors:                        {stats.errors}")

    print("-" * 80)
    print(f"Sample proposed updates (max {args.sample_limit}):")
    if sample_updates:
        for line in sample_updates:
            print(f"  - {line}")
    else:
        print("  (none)")
    print("=" * 80)

    # In dry-run, proposed updates are what would be written.
    if args.dry_run:
        return 0
    return 0 if stats.errors == 0 else 1


def main() -> int:
    args = parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
