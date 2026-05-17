#!/usr/bin/env python3
"""
Backfill links.tenant_id from campaigns/{id}.tenant_id (if set).

Usage:
  python migrate_links_tenant_id.py --project YOUR_GCP_PROJECT [--database (default)] \\
      [--dry-run] [--default-tenant SLUG]

If a link already has tenant_id, it is skipped. If campaign has no tenant_id and
--default-tenant is omitted, the link is skipped with a warning.

Prerequisites:
  - Set GOOGLE_APPLICATION_CREDENTIALS or use gcloud application-default login
  - Optionally set tenant_id on campaign documents first, or pass --default-tenant
  - pip install tqdm (recommended for progress bar; without it, iteration has no bar)
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any, Dict, Iterable, Optional, TypeVar

from google.cloud import firestore

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    T = TypeVar("T")

    def tqdm(iterable: Iterable[T], **_kwargs: Any) -> Iterable[T]:
        return iterable


def _campaign_tenant(
    db: firestore.Client, data: Dict[str, Any]
) -> Optional[str]:
    ref = data.get("campaign_ref")
    if ref is None or not hasattr(ref, "get"):
        return None
    snap = ref.get()
    if not snap.exists:
        return None
    raw = (snap.to_dict() or {}).get("tenant_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill links.tenant_id from campaign.tenant_id")
    parser.add_argument(
        "--project",
        default=os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID"),
        help="GCP project id (default: PROJECT_ID or GCP_PROJECT env)",
    )
    parser.add_argument(
        "--database",
        default="(default)",
        help="Firestore database id (default: (default))",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions only; no writes",
    )
    parser.add_argument(
        "--default-tenant",
        default=None,
        help="Fallback tenant_id when campaign has no tenant_id",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max links to process (0 = no limit)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every link update/skip line (default: tqdm progress bar only).",
    )
    args = parser.parse_args()

    if not args.project:
        print("ERROR: --project or PROJECT_ID/GCP_PROJECT required", file=sys.stderr)
        return 1

    db = firestore.Client(project=args.project, database=args.database)
    batch = db.batch()
    ops = 0
    updated = 0
    skipped_has = 0
    skipped_no_source = 0
    processed = 0

    def flush() -> None:
        nonlocal batch, ops
        if ops and not args.dry_run:
            batch.commit()
        batch = db.batch()
        ops = 0

    def _postfix() -> dict:
        return {
            "would_upd" if args.dry_run else "updated": updated,
            "has_tid": skipped_has,
            "no_src": skipped_no_source,
        }

    stream = db.collection("links").stream()
    pbar = tqdm(
        stream,
        desc="links",
        unit="link",
        dynamic_ncols=True,
        mininterval=0.25,
    )

    for snap in pbar:
        if args.limit and processed >= args.limit:
            break
        processed += 1
        lid = snap.id
        data = snap.to_dict() or {}
        if isinstance(data.get("tenant_id"), str) and data["tenant_id"].strip():
            skipped_has += 1
            pbar.set_postfix(**_postfix())
            continue

        tid = _campaign_tenant(db, data)
        if not tid and args.default_tenant:
            tid = str(args.default_tenant).strip()

        if not tid:
            skipped_no_source += 1
            if args.verbose:
                print(f"[skip] links/{lid}: no tenant_id on campaign and no --default-tenant")
            pbar.set_postfix(**_postfix())
            continue

        ref = db.collection("links").document(lid)
        if args.verbose:
            print(f"{'[dry-run] ' if args.dry_run else ''}links/{lid} -> tenant_id={tid!r}")
        if not args.dry_run:
            batch.update(ref, {"tenant_id": tid})
            ops += 1
            updated += 1
            if ops >= 450:
                flush()
        else:
            updated += 1
        pbar.set_postfix(**_postfix())

    pbar.close()
    flush()

    print(
        f"Done. updated={updated} skipped_already_has_tenant={skipped_has} "
        f"skipped_no_source={skipped_no_source} dry_run={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
