#!/usr/bin/env python3
"""
Delete Firestore 'hits' for a single business + campaign combination.

The script runs against either the dev or prod project and deletes all
documents in the `hits` collection where:

  - campaign_ref == /campaigns/{campaign_id}
  - business_ref == /businesses/{business_id}

Usage examples:

  # Dry-run in dev: just count and list a few matching docs
  python delete_hits_by_business_and_campaign.py \\
      --env dev \\
      --campaign-id eebb74b8-6c5b-44b9-96ab-f4b6e3505206 \\
      --business-id 4-advice-GmbH-53177 \\
      --dry-run

  # Actually delete in prod (no dry-run)
  python delete_hits_by_business_and_campaign.py \\
      --env prod \\
      --campaign-id <CAMPAIGN_ID> \\
      --business-id <BUSINESS_ID>
"""

import argparse
import sys
from typing import Optional

from google.cloud import firestore


DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"

BATCH_SIZE = 450  # keep below Firestore 500 limit


def get_project_for_env(env: str) -> str:
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    raise ValueError(f"Unknown env: {env!r}")


def delete_hits_for_business_and_campaign(
    db: firestore.Client,
    campaign_id: str,
    business_id: str,
    limit: Optional[int],
    dry_run: bool,
) -> int:
    """
    Delete hits where campaign_ref == /campaigns/{campaign_id}
    and business_ref == /businesses/{business_id}.

    Returns the number of matching documents (or that would be deleted in dry-run).
    """
    campaign_ref = db.collection("campaigns").document(campaign_id)
    business_ref = db.collection("businesses").document(business_id)

    # Optional existence checks (do not fail hard if missing).
    camp_snap = campaign_ref.get()
    biz_snap = business_ref.get()
    if not camp_snap.exists:
        print(f"Warning: campaign {campaign_id!r} not found; query may still match zero docs.")
    if not biz_snap.exists:
        print(f"Warning: business {business_id!r} not found; query may still match zero docs.")

    query = (
        db.collection("hits")
        .where("campaign_ref", "==", campaign_ref)
        .where("business_ref", "==", business_ref)
    )

    total_matched = 0
    sample_ids = []

    if dry_run:
        # Just iterate once, count, and capture a few example IDs.
        for doc in query.stream():
            total_matched += 1
            if len(sample_ids) < 20:
                sample_ids.append(doc.id)
            if limit and total_matched >= limit:
                break

        print(f"[DRY-RUN] Found {total_matched} matching hit(s).")
        if sample_ids:
            print(f"[DRY-RUN] Example document IDs (up to 20): {', '.join(sample_ids)}")
        if limit and total_matched >= limit:
            print(f"[DRY-RUN] Reached limit={limit}; more matching docs may exist.")
        return total_matched

    # Actual deletion in batches.
    batch = db.batch()
    in_batch = 0

    for doc in query.stream():
        batch.delete(doc.reference)
        total_matched += 1
        in_batch += 1

        if in_batch >= BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            in_batch = 0

        if limit and total_matched >= limit:
            print(f"Reached limit={limit}; stopping early.")
            break

    if in_batch > 0:
        batch.commit()

    print(f"Deleted {total_matched} hit(s).")
    return total_matched


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete Firestore 'hits' for a specific business + campaign.",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment (project) to target. Ignored if --project is set. Default: dev",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="Explicit GCP project ID (overrides --env).",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE_ID,
        help=f"Firestore database ID (default: {DEFAULT_DATABASE_ID})",
    )
    parser.add_argument(
        "--campaign-id",
        required=True,
        help="Campaign document ID under /campaigns/{id}.",
    )
    parser.add_argument(
        "--business-id",
        required=True,
        help="Business document ID under /businesses/{id}.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional safety limit: max number of hits to delete.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write; just count matching docs and print a few IDs.",
    )

    args = parser.parse_args()

    project_id = args.project or get_project_for_env(args.env)

    print("=" * 60)
    print("Delete hits by business + campaign")
    print("=" * 60)
    print(f"Project:   {project_id}")
    print(f"Database:  {args.database}")
    print(f"Env:       {args.env} (overridden by --project if set)")
    print(f"Campaign:  {args.campaign_id}")
    print(f"Business:  {args.business_id}")
    if args.limit:
        print(f"Limit:     {args.limit}")
    print(f"Dry run:   {args.dry_run}")
    print()

    try:
        db = firestore.Client(project=project_id, database=args.database)
    except Exception as e:
        print(f"Error initializing Firestore client: {e}", file=sys.stderr)
        return 1

    try:
        delete_hits_for_business_and_campaign(
            db=db,
            campaign_id=args.campaign_id,
            business_id=args.business_id,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    except Exception as e:
        print(f"Error during deletion: {e}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("[DRY-RUN] No documents were deleted.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

