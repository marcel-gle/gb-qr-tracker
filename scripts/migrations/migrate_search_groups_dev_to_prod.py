#!/usr/bin/env python3
"""
One-off migration: copy specific customers/*/search_groups/* from DEV to PROD
and rewrite campaign_id + campaign_ref in both search_groups and nested searches.

This script is intentionally small and safe:
- dry-run by default (no writes)
- requires explicit --apply to write to PROD

Example:
  python scripts/migrations/migrate_search_groups_dev_to_prod.py \
    --dev-project gb-qr-tracker-dev --dev-database test \
    --prod-project gb-qr-tracker --prod-database "(default)" \
    --dev-sa /path/to/dev-sa.json --prod-sa /path/to/prod-sa.json \
    --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from google.cloud import firestore
from google.oauth2 import service_account


DEFAULT_DEV_PROJECT = "gb-qr-tracker-dev"
DEFAULT_PROD_PROJECT = "gb-qr-tracker"
DEFAULT_DEV_DATABASE = "(default)"
DEFAULT_PROD_DATABASE = "(default)"

# Default service account key paths (matches scripts/migrations/migrate_env.py).
# You can override via CLI flags or env vars:
#   DEV_SA_PATH / PROD_SA_PATH
DEFAULT_DEV_SA_PATH = os.environ.get("DEV_SA_PATH") or "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
DEFAULT_PROD_SA_PATH = os.environ.get("PROD_SA_PATH") or "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-1b9e04b746.json"


@dataclass(frozen=True)
class Mapping:
    src_customer_id: str
    search_group_id: str
    dest_customer_id: str
    dest_campaign_id: str


DEFAULT_MAPPINGS: List[Mapping] = [
    Mapping(
        src_customer_id="xLRk37rnV7T4CbOXzW5N3saxVfy1",
        search_group_id="fm0PoxJesPgsB23N7ioa",
        dest_customer_id="xLRk37rnV7T4CbOXzW5N3saxVfy1",
        dest_campaign_id="ee3431d7-7432-4180-b7f0-c95ccb8a0a76",
    ),
    Mapping(
        src_customer_id="xLRk37rnV7T4CbOXzW5N3saxVfy1",
        search_group_id="K9AVHn48VOeoO4F04XHd",
        dest_customer_id="6sYd3fUW0LPMqGNDrvXFafKP1IT2",
        dest_campaign_id="a914bfa9-31b2-48ab-8adf-24b687e0b707",
    ),
    Mapping(
        src_customer_id="xLRk37rnV7T4CbOXzW5N3saxVfy1",
        search_group_id="Eq6ijSXzopujTwZxx2oL",
        dest_customer_id="6sYd3fUW0LPMqGNDrvXFafKP1IT2",
        dest_campaign_id="a6386089-89dd-4ff4-97af-136568269ac7",
    ),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate specific search_groups from dev to prod and rewrite campaign refs."
    )

    p.add_argument("--dev-project", default=DEFAULT_DEV_PROJECT, help="DEV GCP project id.")
    p.add_argument("--dev-database", default=DEFAULT_DEV_DATABASE, help="DEV Firestore database id.")
    p.add_argument("--prod-project", default=DEFAULT_PROD_PROJECT, help="PROD GCP project id.")
    p.add_argument("--prod-database", default=DEFAULT_PROD_DATABASE, help="PROD Firestore database id.")

    p.add_argument("--dev-sa", default=DEFAULT_DEV_SA_PATH, help="Path to DEV service account JSON.")
    p.add_argument("--prod-sa", default=DEFAULT_PROD_SA_PATH, help="Path to PROD service account JSON.")

    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually write to PROD. If omitted, script runs in dry-run mode.",
    )
    p.add_argument(
        "--merge",
        action="store_true",
        help="Use merge writes (set(..., merge=True)). Default overwrites docs (merge=False).",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=450,
        help="Write batch size (max 500).",
    )

    return p.parse_args()


def _client(project: str, database: str, sa_path: str) -> firestore.Client:
    creds = service_account.Credentials.from_service_account_file(sa_path)
    return firestore.Client(project=project, database=database, credentials=creds)


def _assert_sa_exists(label: str, path: str) -> None:
    if not path:
        raise SystemExit(f"❌ Missing {label} service account path.")
    if not os.path.exists(path):
        raise SystemExit(f"❌ {label} service account JSON not found: {path}")


def _chunked(items: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), max(1, n)):
        yield items[i : i + n]


def _ref_customers(db: firestore.Client, customer_id: str) -> firestore.DocumentReference:
    return db.collection("customers").document(customer_id)


def _ref_search_group(db: firestore.Client, customer_id: str, sg_id: str) -> firestore.DocumentReference:
    return _ref_customers(db, customer_id).collection("search_groups").document(sg_id)


def _ref_campaign(db: firestore.Client, campaign_id: str) -> firestore.DocumentReference:
    return db.collection("campaigns").document(campaign_id)


def _rewrite_campaign_fields(
    data: Dict[str, Any],
    campaign_id: str,
    campaign_ref: firestore.DocumentReference,
) -> Dict[str, Any]:
    out = dict(data or {})
    out["campaign_id"] = campaign_id
    out["campaign_ref"] = campaign_ref
    return out


def load_source_bundle(
    dev_db: firestore.Client,
    mapping: Mapping,
) -> Tuple[firestore.DocumentSnapshot, List[firestore.DocumentSnapshot]]:
    sg_ref = _ref_search_group(dev_db, mapping.src_customer_id, mapping.search_group_id)
    sg_snap = sg_ref.get()
    searches = list(sg_ref.collection("searches").stream())
    return sg_snap, searches


def plan_writes(
    prod_db: firestore.Client,
    mapping: Mapping,
    sg_snap: firestore.DocumentSnapshot,
    search_snaps: List[firestore.DocumentSnapshot],
) -> List[Tuple[firestore.DocumentReference, Dict[str, Any]]]:
    if not sg_snap.exists:
        raise RuntimeError(
            f"DEV source search_group does not exist: customers/{mapping.src_customer_id}/search_groups/{mapping.search_group_id}"
        )

    campaign_ref = _ref_campaign(prod_db, mapping.dest_campaign_id)
    dest_sg_ref = _ref_search_group(prod_db, mapping.dest_customer_id, mapping.search_group_id)

    sg_data = _rewrite_campaign_fields(sg_snap.to_dict() or {}, mapping.dest_campaign_id, campaign_ref)
    writes: List[Tuple[firestore.DocumentReference, Dict[str, Any]]] = [(dest_sg_ref, sg_data)]

    for s in search_snaps:
        dest_search_ref = dest_sg_ref.collection("searches").document(s.id)
        s_data = _rewrite_campaign_fields(s.to_dict() or {}, mapping.dest_campaign_id, campaign_ref)
        writes.append((dest_search_ref, s_data))

    return writes


def commit_writes(
    prod_db: firestore.Client,
    writes: List[Tuple[firestore.DocumentReference, Dict[str, Any]]],
    *,
    merge: bool,
    batch_size: int,
) -> None:
    if batch_size <= 0 or batch_size > 500:
        raise ValueError("--batch-size must be in range 1..500")

    for chunk in _chunked(writes, batch_size):
        batch = prod_db.batch()
        for ref, data in chunk:
            batch.set(ref, data, merge=merge)
        batch.commit()


def main() -> int:
    args = parse_args()
    dry_run = not args.apply

    _assert_sa_exists("DEV", args.dev_sa)
    _assert_sa_exists("PROD", args.prod_sa)

    dev_db = _client(args.dev_project, args.dev_database, args.dev_sa)
    prod_db = _client(args.prod_project, args.prod_database, args.prod_sa)

    print("=" * 88)
    print("Migrate search_groups DEV -> PROD (rewrite campaign_id + campaign_ref)")
    print("=" * 88)
    print(f"DEV:   project={args.dev_project} database={args.dev_database}")
    print(f"PROD:  project={args.prod_project} database={args.prod_database}")
    print(f"Mode:  {'DRY RUN' if dry_run else 'APPLY (writes to prod)'}")
    print(f"Write: merge={bool(args.merge)} batch_size={args.batch_size}")
    print()

    all_writes: List[Tuple[firestore.DocumentReference, Dict[str, Any]]] = []

    for m in DEFAULT_MAPPINGS:
        print("-" * 88)
        print(
            "Mapping:",
            f"dev customers/{m.src_customer_id}/search_groups/{m.search_group_id}",
            "->",
            f"prod customers/{m.dest_customer_id}/search_groups/{m.search_group_id}",
        )
        print(f"Rewrite campaign_id/ref -> {m.dest_campaign_id}")

        sg_snap, search_snaps = load_source_bundle(dev_db, m)
        sg_data = sg_snap.to_dict() or {}
        print(f"DEV search_group exists={sg_snap.exists} fields={sorted(list(sg_data.keys()))}")
        print(f"DEV nested searches count={len(search_snaps)}")

        writes = plan_writes(prod_db, m, sg_snap, search_snaps)
        print(f"Planned writes: {len(writes)} (1 search_group + {len(writes) - 1} searches)")

        # Minimal preview of rewritten campaign fields (do not print full documents).
        preview_sg = writes[0][1]
        print(
            "Preview rewritten fields:",
            f"campaign_id={preview_sg.get('campaign_id')}",
            f"campaign_ref.path={getattr(preview_sg.get('campaign_ref'), 'path', None)}",
        )
        all_writes.extend(writes)

    print("-" * 88)
    print(f"Total planned writes: {len(all_writes)}")

    if dry_run:
        print("\n[DRY RUN] No writes executed. Re-run with --apply to write to PROD.")
        return 0

    commit_writes(
        prod_db,
        all_writes,
        merge=bool(args.merge),
        batch_size=int(args.batch_size),
    )
    print("\n[OK] Migration complete. Wrote to PROD.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

