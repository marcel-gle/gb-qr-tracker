#!/usr/bin/env python3
"""
Fix incorrect campaign_name in links documents for a specific campaign.

By default this script runs in dry-run mode and prints what would change.
Use --apply to perform writes.

Example:
  # Preview only (default)
  python scripts/migrations/fix_links_campaign_name_for_koenig_studios.py --env dev

  # Apply changes
  python scripts/migrations/fix_links_campaign_name_for_koenig_studios.py --env prod --apply
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

from google.cloud import firestore


DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"
DEFAULT_COLLECTION = "links"
DEFAULT_CAMPAIGN_ID = "5ee6e5bd-0a4d-4906-b787-445c66e88536"
DEFAULT_OLD_CAMPAIGN_NAME = "(none)"
DEFAULT_NEW_CAMPAIGN_NAME = "König Studios - Eventlocations"
BATCH_SIZE = 450  # keep below Firestore limit of 500 writes


def get_project_for_env(env: str) -> str:
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    raise ValueError(f"Unknown env: {env!r}")


def fix_campaign_name(
    db: firestore.Client,
    *,
    collection: str,
    campaign_id: str,
    old_name: str,
    new_name: str,
    limit: Optional[int],
    dry_run: bool,
) -> int:
    campaign_ref = db.collection("campaigns").document(campaign_id)

    query = (
        db.collection(collection)
        .where("campaign_ref", "==", campaign_ref)
        .where("campaign_name", "==", old_name)
    )

    changed = 0
    sample_doc_ids: list[str] = []

    if dry_run:
        for snap in query.stream():
            changed += 1
            if len(sample_doc_ids) < 20:
                sample_doc_ids.append(snap.id)
            if limit and changed >= limit:
                break

        print(f"[DRY-RUN] Matching document(s) to update: {changed}")
        if sample_doc_ids:
            print(f"[DRY-RUN] Example IDs (up to 20): {', '.join(sample_doc_ids)}")
        if limit and changed >= limit:
            print(f"[DRY-RUN] Reached limit={limit}; more matches may exist.")
        print("[DRY-RUN] No documents were modified.")
        return changed

    batch = db.batch()
    in_batch = 0
    for snap in query.stream():
        batch.update(snap.reference, {"campaign_name": new_name})
        changed += 1
        in_batch += 1

        if in_batch >= BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            in_batch = 0

        if len(sample_doc_ids) < 20:
            sample_doc_ids.append(snap.id)
        if limit and changed >= limit:
            print(f"Reached limit={limit}; stopping early.")
            break

    if in_batch > 0:
        batch.commit()

    print(f"Updated document(s): {changed}")
    if sample_doc_ids:
        print(f"Updated example IDs (up to 20): {', '.join(sample_doc_ids)}")
    return changed


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fix links.campaign_name for one campaign where the incorrect value is '(none)'. "
            "Dry-run is the default."
        )
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment (project) to target. Ignored if --project is set. Default: dev",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="Explicit GCP project ID (overrides --env).",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_DATABASE_ID,
        help=f"Firestore database ID (default: {DEFAULT_DATABASE_ID})",
    )
    parser.add_argument(
        "--collection",
        default=DEFAULT_COLLECTION,
        help=f"Collection name for links documents (default: {DEFAULT_COLLECTION})",
    )
    parser.add_argument(
        "--campaign-id",
        default=DEFAULT_CAMPAIGN_ID,
        help=f"Campaign document ID under /campaigns/{{id}} (default: {DEFAULT_CAMPAIGN_ID})",
    )
    parser.add_argument(
        "--old-name",
        default=DEFAULT_OLD_CAMPAIGN_NAME,
        help=f"Only update docs with this current campaign_name (default: {DEFAULT_OLD_CAMPAIGN_NAME!r})",
    )
    parser.add_argument(
        "--new-name",
        default=DEFAULT_NEW_CAMPAIGN_NAME,
        help="New value for campaign_name.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional safety limit: max number of docs to process.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write changes. If omitted, script runs in dry-run mode.",
    )

    args = parser.parse_args()
    dry_run = not args.apply
    project_id = args.project or get_project_for_env(args.env)

    print("=" * 72)
    print("Fix links.campaign_name for specific campaign")
    print("=" * 72)
    print(f"Project:      {project_id}")
    print(f"Database:     {args.database}")
    print(f"Collection:   {args.collection}")
    print(f"Campaign ID:  {args.campaign_id}")
    print(f"Old name:     {args.old_name!r}")
    print(f"New name:     {args.new_name!r}")
    if args.limit:
        print(f"Limit:        {args.limit}")
    print(f"Mode:         {'DRY-RUN' if dry_run else 'APPLY'}")
    print()

    try:
        db = firestore.Client(project=project_id, database=args.database)
    except Exception as exc:
        print(f"Error initializing Firestore client: {exc}", file=sys.stderr)
        return 1

    try:
        fix_campaign_name(
            db,
            collection=args.collection,
            campaign_id=args.campaign_id,
            old_name=args.old_name,
            new_name=args.new_name,
            limit=args.limit,
            dry_run=dry_run,
        )
    except Exception as exc:
        print(f"Error during migration: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
