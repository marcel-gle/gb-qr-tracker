#!/usr/bin/env python3
"""
Cleanup script for erroneously recorded tracking-link hits (e.g. from letter processing).

Deletes selected hits for a single user and campaign, then corrects:
- Campaign totals.hits and totals.unique_ips (and unique_ips subcollection)
- Links hit_count and last_hit_at
- customers/{uid}/businesses hit_count and last_hit_at

For security, every run is restricted to one --owner-id and one --campaign-id.

Usage:
  # By date: delete all hits before a date for that user/campaign
  python cleanup_tracking_hits.py --owner-id UID --campaign-id CID --before-date 2025-02-01 --env dev --dry-run

  # By businesses: delete hits only for specific businesses
  python cleanup_tracking_hits.py --owner-id UID --campaign-id CID --business-ids id1,id2 --env dev --dry-run

  # With database and limit
  python cleanup_tracking_hits.py --owner-id UID --campaign-id CID --before-date 2025-02-01 --env dev --database "(default)" --limit 100 --dry-run
"""

import sys
import argparse
from typing import List, Optional, Set, Dict, Any
from collections import defaultdict
from datetime import datetime, timezone

from google.cloud import firestore

# Default configuration (aligned with other scripts)
DEFAULT_PROJECT_DEV = "gb-qr-tracker-dev"
DEFAULT_PROJECT_PROD = "gb-qr-tracker"
DEFAULT_DATABASE_ID = "(default)"

BATCH_SIZE = 450  # Firestore batch limit 500, leave headroom


def get_project_for_env(env: str) -> str:
    """Map environment name to GCP project ID."""
    if env == "prod":
        return DEFAULT_PROJECT_PROD
    if env == "dev":
        return DEFAULT_PROJECT_DEV
    raise ValueError(f"Unknown env: {env!r}")


def parse_before_date(s: str) -> datetime:
    """Parse ISO date or datetime string to timezone-aware datetime (UTC)."""
    s = s.strip()
    if not s:
        raise ValueError("Empty before-date")
    # Try with time
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s.replace("Z", "+00:00"), fmt.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Cannot parse before-date: {s!r}")


def _ts_to_datetime(ts: Any) -> Optional[datetime]:
    """Convert Firestore timestamp or datetime to timezone-aware datetime."""
    if ts is None:
        return None
    if hasattr(ts, "timestamp"):
        return datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc)
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
    return None


# --- Hit record for in-memory use (we store ref + fields we need) ---
class HitRecord:
    def __init__(
        self,
        ref: firestore.DocumentReference,
        link_id: str,
        campaign_id: str,
        business_id: Optional[str],
        owner_id: str,
        ts: Optional[datetime],
        ip_hash: Optional[str],
    ):
        self.ref = ref
        self.link_id = link_id
        self.campaign_id = campaign_id
        self.business_id = business_id
        self.owner_id = owner_id
        self.ts = ts
        self.ip_hash = ip_hash


def collect_hits(
    db: firestore.Client,
    owner_id: str,
    campaign_ref: firestore.DocumentReference,
    campaign_id: str,
    before_date: Optional[datetime],
    business_ids: Optional[Set[str]],
    limit: Optional[int],
) -> List[HitRecord]:
    """
    Query hits for owner_id + campaign_ref, filter by before_date or business_ids.
    Returns list of HitRecord (ref + metadata).
    """
    hits_ref = db.collection("hits")
    query = hits_ref.where("owner_id", "==", owner_id).where(
        "campaign_ref", "==", campaign_ref
    )
    records: List[HitRecord] = []
    seen_ids: Set[str] = set()
    page_size = 500
    last_doc = None

    while True:
        q = query.limit(page_size)
        if last_doc:
            q = q.start_after(last_doc)
        page = list(q.stream())
        if not page:
            break

        for doc in page:
            if doc.id in seen_ids:
                continue
            data = doc.to_dict() or {}
            link_id = data.get("link_id") or ""
            biz_ref = data.get("business_ref")
            if isinstance(biz_ref, firestore.DocumentReference):
                business_id = biz_ref.id
            else:
                business_id = None
            ts = _ts_to_datetime(data.get("ts"))
            ip_hash = data.get("ip_hash") or None

            if before_date is not None:
                if ts is None or ts >= before_date:
                    continue
            if business_ids is not None:
                if business_id is None or business_id not in business_ids:
                    continue

            seen_ids.add(doc.id)
            records.append(
                HitRecord(
                    ref=doc.reference,
                    link_id=link_id,
                    campaign_id=campaign_id,
                    business_id=business_id,
                    owner_id=data.get("owner_id") or owner_id,
                    ts=ts,
                    ip_hash=ip_hash,
                )
            )
            if limit and len(records) >= limit:
                return records

        if len(page) < page_size:
            break
        last_doc = page[-1]

    return records


def delete_hits_batch(
    db: firestore.Client,
    records: List[HitRecord],
    dry_run: bool,
) -> int:
    """Delete hit documents in batches. Returns number deleted (or would-be in dry-run)."""
    if not records:
        return 0
    if dry_run:
        return len(records)
    deleted = 0
    batch = db.batch()
    for i, rec in enumerate(records):
        batch.delete(rec.ref)
        deleted += 1
        if (i + 1) % BATCH_SIZE == 0 or (i + 1) == len(records):
            batch.commit()
            batch = db.batch()
    return deleted


def correct_campaign_totals(
    db: firestore.Client,
    campaign_id: str,
    campaign_ref: firestore.DocumentReference,
    deleted_count: int,
    deleted_ip_hashes: Set[str],
    dry_run: bool,
) -> None:
    """
    Set campaign totals.hits and totals.unique_ips after deletions.
    For each ip_hash in deleted set, if no remaining hit has that ip_hash in this campaign,
    decrement unique_ips and delete campaigns/{id}/unique_ips/{ip_hash}.
    """
    campaign_doc = db.collection("campaigns").document(campaign_id)
    snap = campaign_doc.get()
    if not snap.exists:
        if dry_run:
            print(f"[DRY-RUN] Campaign {campaign_id} not found, skip totals update")
        return
    data = snap.to_dict() or {}
    totals = data.get("totals") or {}
    current_hits = int(totals.get("hits") or 0)
    current_unique = int(totals.get("unique_ips") or 0)

    new_hits = max(0, current_hits - deleted_count)

    # Which IPs from deleted set have no remaining hit in this campaign?
    ips_to_decrement: List[str] = []
    hits_ref = db.collection("hits")
    for ip_hash in deleted_ip_hashes:
        if not ip_hash:
            continue
        q = (
            hits_ref.where("campaign_ref", "==", campaign_ref)
            .where("ip_hash", "==", ip_hash)
            .limit(1)
        )
        if next(q.stream(), None) is None:
            ips_to_decrement.append(ip_hash)

    new_unique = max(0, current_unique - len(ips_to_decrement))

    prefix = "[DRY-RUN] " if dry_run else ""
    print(
        f"  {prefix}campaigns/{campaign_id}: totals.hits {current_hits} -> {new_hits}, "
        f"totals.unique_ips {current_unique} -> {new_unique} (decrement by {len(ips_to_decrement)} IPs)"
    )
    if ips_to_decrement:
        for ip_hash in ips_to_decrement[:5]:  # show first 5
            print(f"    {prefix}campaigns/{campaign_id}/unique_ips/{ip_hash} {'would delete' if dry_run else 'deleted'}")
        if len(ips_to_decrement) > 5:
            print(f"    ... and {len(ips_to_decrement) - 5} more unique_ips docs")

    if dry_run:
        return

    campaign_doc.set(
        {
            "totals.hits": new_hits,
            "totals.unique_ips": new_unique,
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    # Delete unique_ips subcollection docs for IPs that have no remaining hits
    for ip_hash in ips_to_decrement:
        campaign_doc.collection("unique_ips").document(ip_hash).delete()


def correct_links(
    db: firestore.Client,
    link_ids: Set[str],
    dry_run: bool,
) -> None:
    """Recompute hit_count and last_hit_at for each link from remaining hits."""
    hits_ref = db.collection("hits")
    links_ref = db.collection("links")
    prefix = "[DRY-RUN] " if dry_run else ""
    for link_id in sorted(link_ids):
        q = hits_ref.where("link_id", "==", link_id)
        count = 0
        last_ts: Optional[datetime] = None
        for doc in q.stream():
            count += 1
            data = doc.to_dict() or {}
            ts = _ts_to_datetime(data.get("ts"))
            if ts:
                last_ts = max(last_ts, ts) if last_ts else ts

        link_snap = links_ref.document(link_id).get()
        old_data = link_snap.to_dict() or {} if link_snap.exists else {}
        old_count = int(old_data.get("hit_count") or 0)
        old_ts = _ts_to_datetime(old_data.get("last_hit_at"))
        old_ts_str = old_ts.isoformat() if old_ts else "(none)"
        new_ts_str = last_ts.isoformat() if last_ts else "(none)"
        print(f"  {prefix}links/{link_id}: hit_count {old_count} -> {count}, last_hit_at {old_ts_str} -> {new_ts_str}")

        payload = {"hit_count": count}
        if last_ts:
            payload["last_hit_at"] = last_ts
        if not dry_run:
            links_ref.document(link_id).set(payload, merge=True)


def correct_customer_businesses(
    db: firestore.Client,
    owner_id: str,
    business_ids: Set[str],
    dry_run: bool,
) -> None:
    """Recompute hit_count and last_hit_at for customers/{uid}/businesses from remaining hits."""
    hits_ref = db.collection("hits")
    businesses_ref = db.collection("customers").document(owner_id).collection(
        "businesses"
    )
    prefix = "[DRY-RUN] " if dry_run else ""
    for business_id in sorted(business_ids):
        biz_ref = db.collection("businesses").document(business_id)
        q = (
            hits_ref.where("owner_id", "==", owner_id).where(
                "business_ref", "==", biz_ref
            )
        )
        count = 0
        last_ts: Optional[datetime] = None
        for doc in q.stream():
            count += 1
            data = doc.to_dict() or {}
            ts = _ts_to_datetime(data.get("ts"))
            if ts:
                last_ts = max(last_ts, ts) if last_ts else ts

        overlay_snap = businesses_ref.document(business_id).get()
        old_data = overlay_snap.to_dict() or {} if overlay_snap.exists else {}
        old_count = int(old_data.get("hit_count") or 0)
        old_ts = _ts_to_datetime(old_data.get("last_hit_at"))
        old_ts_str = old_ts.isoformat() if old_ts else "(none)"
        new_ts_str = last_ts.isoformat() if last_ts else "(none)"
        print(
            f"  {prefix}customers/{owner_id}/businesses/{business_id}: hit_count {old_count} -> {count}, last_hit_at {old_ts_str} -> {new_ts_str}"
        )

        payload = {
            "hit_count": count,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        if last_ts:
            payload["last_hit_at"] = last_ts
        if not dry_run:
            businesses_ref.document(business_id).set(payload, merge=True)


def run_cleanup(
    db: firestore.Client,
    owner_id: str,
    campaign_id: str,
    before_date: Optional[datetime],
    business_ids: Optional[List[str]],
    limit: Optional[int],
    dry_run: bool,
) -> Dict[str, Any]:
    campaign_ref = db.collection("campaigns").document(campaign_id)
    snap = campaign_ref.get()
    if not snap.exists:
        raise SystemExit(f"Campaign not found: {campaign_id}")

    business_set = set(business_ids) if business_ids else None

    # Phase 1: collect and delete hits
    mode = "before-date" if before_date is not None else "business-ids"
    print(f"Phase 1: Collecting hits (mode={mode})...")
    records = collect_hits(
        db=db,
        owner_id=owner_id,
        campaign_ref=campaign_ref,
        campaign_id=campaign_id,
        before_date=before_date,
        business_ids=business_set,
        limit=limit,
    )
    print(f"  Found {len(records)} hits to delete.")

    if not records:
        print("No hits to delete. Done.")
        return {"deleted": 0, "campaign_updated": False, "links_updated": 0, "overlays_updated": 0}

    # Per-business breakdown
    by_business: Dict[str, int] = defaultdict(int)
    hits_without_business = 0
    for r in records:
        if r.business_id:
            by_business[r.business_id] += 1
        else:
            hits_without_business += 1
    print()
    print("  Businesses affected (hits deleted per business):")
    if by_business:
        for biz_id in sorted(by_business.keys()):
            print(f"    {biz_id}: {by_business[biz_id]} hits")
    if hits_without_business:
        print(f"    (no business): {hits_without_business} hits")
    print(f"  Total: {len(records)} hits across {len(by_business)} businesses" + (" (+ hits without business)" if hits_without_business else ""))
    print()

    if dry_run:
        print(f"[DRY-RUN] Would delete {len(records)} hits.")
    else:
        deleted = delete_hits_batch(db, records, dry_run=False)
        print(f"  Deleted {deleted} hits.")

    # Metadata from deleted set for corrections
    link_ids: Set[str] = {r.link_id for r in records if r.link_id}
    overlay_business_ids: Set[str] = {r.business_id for r in records if r.business_id}
    deleted_ip_hashes: Set[str] = {r.ip_hash for r in records if r.ip_hash}

    # Phase 2: campaign totals
    print("Phase 2: Correcting campaign totals...")
    correct_campaign_totals(
        db=db,
        campaign_id=campaign_id,
        campaign_ref=campaign_ref,
        deleted_count=len(records),
        deleted_ip_hashes=deleted_ip_hashes,
        dry_run=dry_run,
    )

    # Phase 3: links
    print("Phase 3: Correcting links...")
    correct_links(db=db, link_ids=link_ids, dry_run=dry_run)

    # Phase 4: customer businesses
    print("Phase 4: Correcting customers/{uid}/businesses...")
    correct_customer_businesses(
        db=db,
        owner_id=owner_id,
        business_ids=overlay_business_ids,
        dry_run=dry_run,
    )

    return {
        "deleted": len(records),
        "campaign_updated": True,
        "links_updated": len(link_ids),
        "overlays_updated": len(overlay_business_ids),
        "by_business": dict(by_business),
        "hits_without_business": hits_without_business,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean up tracking hits for one user and campaign; correct campaign, links, and customer business aggregates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment (default: dev). Ignored if --project is set.",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=None,
        help="GCP project ID (overrides --env).",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=DEFAULT_DATABASE_ID,
        help=f"Firestore database ID (default: {DEFAULT_DATABASE_ID})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing.",
    )
    parser.add_argument(
        "--owner-id",
        type=str,
        required=True,
        help="Customer UID (required; always restrict to one user).",
    )
    parser.add_argument(
        "--campaign-id",
        type=str,
        required=True,
        help="Campaign ID (required; always restrict to one campaign).",
    )
    parser.add_argument(
        "--before-date",
        type=str,
        default=None,
        help="Mode 1: delete hits with ts before this date yyyy-mm-dd (ISO date or datetime, e.g. 2025-02-01 or 2025-02-01T12:00:00Z).",
    )
    parser.add_argument(
        "--business-ids",
        type=str,
        default=None,
        help="Mode 2: comma-separated business IDs; delete only hits for these businesses.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of hits to delete (for testing).",
    )

    args = parser.parse_args()

    # Validation: exactly one of before-date or business-ids
    has_date = args.before_date is not None and args.before_date.strip() != ""
    has_biz = args.business_ids is not None and args.business_ids.strip() != ""
    if has_date and has_biz:
        print("Error: use exactly one of --before-date or --business-ids.", file=sys.stderr)
        return 1
    if not has_date and not has_biz:
        print("Error: must provide either --before-date or --business-ids.", file=sys.stderr)
        return 1

    before_date: Optional[datetime] = None
    business_ids: Optional[List[str]] = None
    if has_date:
        try:
            before_date = parse_before_date(args.before_date)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    else:
        business_ids = [b.strip() for b in args.business_ids.split(",") if b.strip()]
        if not business_ids:
            print("Error: --business-ids must contain at least one ID.", file=sys.stderr)
            return 1

    project_id = args.project or get_project_for_env(args.env)
    print("=" * 60)
    print("Cleanup tracking hits")
    print("=" * 60)
    print(f"Project:   {project_id}")
    print(f"Database:  {args.database}")
    print(f"Dry run:   {args.dry_run}")
    print(f"Owner ID:  {args.owner_id}")
    print(f"Campaign:  {args.campaign_id}")
    print(f"Mode:      {'before-date' if before_date else 'business-ids'}")
    if before_date:
        print(f"Before:    {before_date.isoformat()}")
    else:
        print(f"Businesses: {len(business_ids)} IDs")
    if args.limit:
        print(f"Limit:     {args.limit}")
    print()

    try:
        db = firestore.Client(project=project_id, database=args.database)
    except Exception as e:
        print(f"Error initializing Firestore: {e}", file=sys.stderr)
        return 1

    try:
        stats = run_cleanup(
            db=db,
            owner_id=args.owner_id,
            campaign_id=args.campaign_id,
            before_date=before_date,
            business_ids=business_ids,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        print()
        print("Summary:")
        print(f"  Deleted:           {stats['deleted']} hits")
        print(f"  Links updated:      {stats['links_updated']}")
        print(f"  Overlays updated:   {stats['overlays_updated']}")
        if stats.get("by_business"):
            print("  By business:")
            for biz_id in sorted(stats["by_business"].keys()):
                print(f"    {biz_id}: {stats['by_business'][biz_id]} hits deleted")
        if stats.get("hits_without_business", 0):
            print(f"  (no business):     {stats['hits_without_business']} hits deleted")
        if args.dry_run:
            print("[DRY-RUN] No changes were written.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
