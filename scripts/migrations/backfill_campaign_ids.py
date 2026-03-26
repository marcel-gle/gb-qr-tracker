#!/usr/bin/env python3
"""
Backfill campaign_ids on customer business overlays from links.

Source of truth:
  links collection with:
    - owner_id (customer scope)
    - campaign_ref (DocumentReference campaigns/{campaignId})
    - business_ref (DocumentReference businesses/{businessId})

Target:
  customers/{customerId}/businesses/{normalizedBusinessId}
  fields:
    - campaign_ids: sorted unique string[]
    - campaign_count: len(campaign_ids)
    - campaign_updated_at: server timestamp

Examples:
  # Dry run over all links
  python scripts/migrations/backfill_campaign_ids.py --dry-run

  # Scoped test run
  python scripts/migrations/backfill_campaign_ids.py --campaign-id abc123 --limit 1000 --dry-run

  # Full run with writes
  python scripts/migrations/backfill_campaign_ids.py --batch-size 450

  # Verify only
  python scripts/migrations/backfill_campaign_ids.py --verify --customer-id someOwner
"""

import argparse
import csv
import hashlib
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import firestore


DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DEFAULT_DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")
DEFAULT_BATCH_SIZE = 500
DEFAULT_WORKERS = 4
DEFAULT_BUCKETS = 64
DIFF_SAMPLE_LIMIT = 20


def sanitize_id(value: str) -> str:
    """
    Normalize ID exactly like the backend sanitize helper used by upload flow.
    """
    if value is None:
        return ""
    v = str(value).strip()
    v = re.sub(r"[^A-Za-z0-9äöüÄÖÜß]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    v = v.lower()
    return v


def normalize_business_id(business_id: str) -> str:
    return sanitize_id(business_id)


@dataclass
class Metrics:
    links_scanned: int = 0
    links_skipped_invalid: int = 0
    unique_pairs_derived: int = 0
    overlays_updated: int = 0
    overlays_created: int = 0
    overlays_unchanged: int = 0
    write_errors: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill campaign_ids on customers/{customerId}/businesses overlays from links."
    )
    parser.add_argument("--dry-run", action="store_true", help="No writes; only report metrics and diffs.")
    parser.add_argument("--verify", action="store_true", help="Read-only verification mode.")
    parser.add_argument("--limit", type=int, default=None, help="Process max N links from links collection.")
    parser.add_argument("--customer-id", type=str, default=None, help="Optional owner/customer scope.")
    parser.add_argument("--campaign-id", type=str, default=None, help="Optional campaign ID scope.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Write batch size (max 500).")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers for reads/diffing.")
    parser.add_argument(
        "--skip-missing-overlays",
        action="store_true",
        help="Skip writes for missing overlay docs instead of creating minimal docs.",
    )
    parser.add_argument("--project", type=str, default=DEFAULT_PROJECT_ID, help="GCP project id.")
    parser.add_argument("--database", type=str, default=DEFAULT_DATABASE_ID, help="Firestore database id.")
    parser.add_argument(
        "--buckets",
        type=int,
        default=DEFAULT_BUCKETS,
        help="Temporary bucket file count for bounded-memory aggregation.",
    )
    return parser.parse_args()


def is_valid_links_doc(data: Dict) -> bool:
    campaign_ref = data.get("campaign_ref")
    business_ref = data.get("business_ref")
    owner_id = data.get("owner_id")
    if not owner_id or not isinstance(owner_id, str):
        return False
    if not campaign_ref or not hasattr(campaign_ref, "id"):
        return False
    if not business_ref or not hasattr(business_ref, "id"):
        return False
    return True


def _bucket_index(customer_id: str, business_id: str, bucket_count: int) -> int:
    digest = hashlib.sha1(f"{customer_id}\t{business_id}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % bucket_count


def stream_links_to_buckets(
    db: firestore.Client,
    customer_id: Optional[str],
    campaign_id: Optional[str],
    limit: Optional[int],
    tmp_dir: str,
    bucket_count: int,
) -> Tuple[Metrics, List[str]]:
    metrics = Metrics()
    bucket_paths = [os.path.join(tmp_dir, f"bucket_{idx:03d}.tsv") for idx in range(bucket_count)]
    handles = [open(path, "w", encoding="utf-8", newline="") for path in bucket_paths]

    seen_pairs: Set[Tuple[str, str]] = set()

    try:
        links_query = db.collection("links")
        scanned = 0
        for doc in links_query.stream():
            if limit is not None and scanned >= limit:
                break
            scanned += 1
            metrics.links_scanned += 1

            data = doc.to_dict() or {}
            if not is_valid_links_doc(data):
                metrics.links_skipped_invalid += 1
                continue

            owner_id = data["owner_id"]
            if customer_id and owner_id != customer_id:
                continue

            campaign_ref = data["campaign_ref"]
            business_ref = data["business_ref"]

            cid = campaign_ref.id
            if campaign_id and cid != campaign_id:
                continue

            normalized_bid = normalize_business_id(business_ref.id)
            if not normalized_bid:
                metrics.links_skipped_invalid += 1
                continue

            key = (owner_id, normalized_bid)
            seen_pairs.add(key)

            idx = _bucket_index(owner_id, normalized_bid, bucket_count)
            handles[idx].write(f"{owner_id}\t{normalized_bid}\t{cid}\n")
    finally:
        for h in handles:
            h.close()

    metrics.unique_pairs_derived = len(seen_pairs)
    return metrics, bucket_paths


def _diff_overlay(
    db: firestore.Client,
    customer_id: str,
    normalized_business_id: str,
    expected_campaign_ids: List[str],
) -> Tuple[firestore.DocumentReference, bool, List[str], bool]:
    overlay_ref = (
        db.collection("customers")
        .document(customer_id)
        .collection("businesses")
        .document(normalized_business_id)
    )
    snap = overlay_ref.get()
    data = snap.to_dict() if snap.exists else {}
    existing = data.get("campaign_ids") if data else None
    if not isinstance(existing, list):
        existing = []
    existing_clean = sorted({str(v) for v in existing if isinstance(v, str)})
    changed = existing_clean != expected_campaign_ids
    return overlay_ref, snap.exists, existing_clean, changed


def process_bucket(
    db: firestore.Client,
    bucket_path: str,
    dry_run: bool,
    batch_size: int,
    skip_missing_overlays: bool,
    workers: int,
    sample_diffs: List[Dict],
) -> Metrics:
    metrics = Metrics()

    aggregated: Dict[Tuple[str, str], Set[str]] = {}
    with open(bucket_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh, delimiter="\t")
        for row in reader:
            if len(row) != 3:
                continue
            customer_id, normalized_business_id, campaign_id = row
            key = (customer_id, normalized_business_id)
            if key not in aggregated:
                aggregated[key] = set()
            aggregated[key].add(campaign_id)

    if not aggregated:
        return metrics

    metrics.unique_pairs_derived = len(aggregated)

    work_items = [
        (customer_id, business_id, sorted(campaign_ids))
        for (customer_id, business_id), campaign_ids in aggregated.items()
    ]

    batch = db.batch()
    pending_writes = 0

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        future_map = {
            pool.submit(_diff_overlay, db, c, b, expected): (c, b, expected)
            for c, b, expected in work_items
        }
        for future in as_completed(future_map):
            customer_id, business_id, expected = future_map[future]
            try:
                overlay_ref, exists, existing_ids, changed = future.result()
            except Exception:
                metrics.write_errors += 1
                continue

            if not changed:
                metrics.overlays_unchanged += 1
                continue

            if len(sample_diffs) < DIFF_SAMPLE_LIMIT:
                sample_diffs.append(
                    {
                        "overlay": overlay_ref.path,
                        "before": existing_ids,
                        "after": expected,
                    }
                )

            if (not exists) and skip_missing_overlays:
                metrics.overlays_unchanged += 1
                continue

            update_payload = {
                "campaign_ids": expected,
                "campaign_count": len(expected),
                "campaign_updated_at": firestore.SERVER_TIMESTAMP,
            }

            if not dry_run:
                try:
                    batch.set(overlay_ref, update_payload, merge=True)
                    pending_writes += 1
                    if pending_writes >= batch_size:
                        batch.commit()
                        batch = db.batch()
                        pending_writes = 0
                except Exception:
                    metrics.write_errors += 1
                    continue

            if exists:
                metrics.overlays_updated += 1
            else:
                metrics.overlays_created += 1

    if not dry_run and pending_writes > 0:
        try:
            batch.commit()
        except Exception:
            metrics.write_errors += pending_writes

    return metrics


def run_backfill(args: argparse.Namespace, db: firestore.Client) -> int:
    start = time.time()
    tmp_dir = tempfile.mkdtemp(prefix="campaign_backfill_")
    sample_diffs: List[Dict] = []

    print("=" * 72)
    print("Backfill campaign_ids on customer business overlays")
    print("=" * 72)
    print(f"Project:              {args.project}")
    print(f"Database:             {args.database}")
    print(f"Dry run:              {args.dry_run}")
    print(f"Limit:                {args.limit}")
    print(f"Customer scope:       {args.customer_id}")
    print(f"Campaign scope:       {args.campaign_id}")
    print(f"Skip missing overlays:{args.skip_missing_overlays}")
    print(f"Batch size:           {args.batch_size}")
    print(f"Workers:              {args.workers}")
    print(f"Buckets:              {args.buckets}")
    print()

    first_pass_metrics, bucket_paths = stream_links_to_buckets(
        db=db,
        customer_id=args.customer_id,
        campaign_id=args.campaign_id,
        limit=args.limit,
        tmp_dir=tmp_dir,
        bucket_count=max(1, args.buckets),
    )

    total = Metrics(
        links_scanned=first_pass_metrics.links_scanned,
        links_skipped_invalid=first_pass_metrics.links_skipped_invalid,
        unique_pairs_derived=0,
        overlays_updated=0,
        overlays_created=0,
        overlays_unchanged=0,
        write_errors=0,
    )

    for path in bucket_paths:
        bucket_metrics = process_bucket(
            db=db,
            bucket_path=path,
            dry_run=args.dry_run,
            batch_size=max(1, min(args.batch_size, 500)),
            skip_missing_overlays=args.skip_missing_overlays,
            workers=max(1, args.workers),
            sample_diffs=sample_diffs,
        )
        total.unique_pairs_derived += bucket_metrics.unique_pairs_derived
        total.overlays_updated += bucket_metrics.overlays_updated
        total.overlays_created += bucket_metrics.overlays_created
        total.overlays_unchanged += bucket_metrics.overlays_unchanged
        total.write_errors += bucket_metrics.write_errors

    elapsed = time.time() - start

    print()
    print("Migration metrics")
    print("-" * 72)
    print(f"links scanned:                    {total.links_scanned}")
    print(f"links skipped invalid:            {total.links_skipped_invalid}")
    print(f"unique (customer,business) pairs: {total.unique_pairs_derived}")
    print(f"overlays updated:                 {total.overlays_updated}")
    print(f"overlays created:                 {total.overlays_created}")
    print(f"overlays unchanged:               {total.overlays_unchanged}")
    print(f"write errors:                     {total.write_errors}")
    print(f"elapsed time (sec):               {elapsed:.2f}")

    if args.dry_run:
        print()
        print("Dry-run mismatch sample (first 20)")
        print("-" * 72)
        if not sample_diffs:
            print("No diffs found.")
        else:
            for idx, row in enumerate(sample_diffs[:DIFF_SAMPLE_LIMIT], start=1):
                print(f"{idx:02d}. {row['overlay']}")
                print(f"    before: {row['before']}")
                print(f"    after:  {row['after']}")

    for path in bucket_paths:
        try:
            os.remove(path)
        except OSError:
            pass
    try:
        os.rmdir(tmp_dir)
    except OSError:
        pass

    return 0 if total.write_errors == 0 else 1


def run_verify(args: argparse.Namespace, db: firestore.Client) -> int:
    start = time.time()
    tmp_dir = tempfile.mkdtemp(prefix="campaign_verify_")
    sample_mismatches: List[Dict] = []

    print("=" * 72)
    print("Verify overlay campaign_ids against links source of truth")
    print("=" * 72)

    base_metrics, bucket_paths = stream_links_to_buckets(
        db=db,
        customer_id=args.customer_id,
        campaign_id=args.campaign_id,
        limit=args.limit,
        tmp_dir=tmp_dir,
        bucket_count=max(1, args.buckets),
    )

    mismatch_count = 0
    checked = 0

    for bucket_path in bucket_paths:
        aggregated: Dict[Tuple[str, str], Set[str]] = {}
        with open(bucket_path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh, delimiter="\t")
            for row in reader:
                if len(row) != 3:
                    continue
                customer_id, business_id, campaign_id = row
                key = (customer_id, business_id)
                if key not in aggregated:
                    aggregated[key] = set()
                aggregated[key].add(campaign_id)

        for (customer_id, business_id), campaign_ids in aggregated.items():
            checked += 1
            expected = sorted(campaign_ids)
            overlay_ref = (
                db.collection("customers")
                .document(customer_id)
                .collection("businesses")
                .document(business_id)
            )
            try:
                snap = overlay_ref.get()
                data = snap.to_dict() if snap.exists else {}
                actual = data.get("campaign_ids") if data else []
                if not isinstance(actual, list):
                    actual = []
                actual_norm = sorted({str(v) for v in actual if isinstance(v, str)})
                if actual_norm != expected:
                    mismatch_count += 1
                    if len(sample_mismatches) < DIFF_SAMPLE_LIMIT:
                        sample_mismatches.append(
                            {
                                "overlay": overlay_ref.path,
                                "expected": expected,
                                "actual": actual_norm,
                                "exists": snap.exists,
                            }
                        )
            except Exception:
                mismatch_count += 1
                if len(sample_mismatches) < DIFF_SAMPLE_LIMIT:
                    sample_mismatches.append(
                        {
                            "overlay": overlay_ref.path,
                            "expected": expected,
                            "actual": ["<read-error>"],
                            "exists": False,
                        }
                    )

    elapsed = time.time() - start

    print()
    print("Verification metrics")
    print("-" * 72)
    print(f"links scanned:                    {base_metrics.links_scanned}")
    print(f"links skipped invalid:            {base_metrics.links_skipped_invalid}")
    print(f"unique (customer,business) pairs: {base_metrics.unique_pairs_derived}")
    print(f"pairs checked:                    {checked}")
    print(f"mismatch count:                   {mismatch_count}")
    print(f"elapsed time (sec):               {elapsed:.2f}")

    print()
    print("Mismatch sample (first 20)")
    print("-" * 72)
    if not sample_mismatches:
        print("No mismatches.")
    else:
        for idx, row in enumerate(sample_mismatches[:DIFF_SAMPLE_LIMIT], start=1):
            print(f"{idx:02d}. {row['overlay']} (exists={row['exists']})")
            print(f"    expected: {row['expected']}")
            print(f"    actual:   {row['actual']}")

    for path in bucket_paths:
        try:
            os.remove(path)
        except OSError:
            pass
    try:
        os.rmdir(tmp_dir)
    except OSError:
        pass

    return 0 if mismatch_count == 0 else 2


def main() -> int:
    args = parse_args()

    if args.batch_size <= 0 or args.batch_size > 500:
        print("Error: --batch-size must be in range 1..500", file=sys.stderr)
        return 1
    if args.workers <= 0:
        print("Error: --workers must be >= 1", file=sys.stderr)
        return 1
    if args.buckets <= 0:
        print("Error: --buckets must be >= 1", file=sys.stderr)
        return 1

    db = firestore.Client(project=args.project, database=args.database)
    if args.verify:
        return run_verify(args, db)
    return run_backfill(args, db)


if __name__ == "__main__":
    sys.exit(main())
