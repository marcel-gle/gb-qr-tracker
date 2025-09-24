#!/usr/bin/env python3
"""
Migrate Firestore 'hits' and update aggregate counters on links and businesses.

Source:
  project  = gb-qr-tracking
  database = (default)
  collection = hits
  filter    = where('campaign', '==', --source-campaign)

Destination:
  project  = gb-qr-tracker-dev
  database = test-2
  collection = hits
  campaign_ref = /campaigns/{--dest-campaign-id}
  target_ref   = /campaigns/{--dest-campaign-id}/targets/{id}  (auto by link_id if present; optional fallback)

Counters updated in DEST:
  - /links/{link_id}:       hit_count += N, last_hit_at = max(existing, max_ts_for_link)
  - /businesses/{business_id}: hit_count += N, last_hit_at = max(existing, max_ts_for_business)
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
from typing import Set


from google.cloud import firestore
from google.api_core.exceptions import PermissionDenied, AlreadyExists, GoogleAPICallError, RetryError, NotFound
from google.oauth2 import service_account
from google.cloud.firestore import Increment



# ---------- Helpers ----------

def init_client(project_id: str, database_id: str, credentials_path: Optional[str]) -> firestore.Client:
    kwargs = {"project": project_id, "database": database_id}
    if credentials_path:
        creds = service_account.Credentials.from_service_account_file(credentials_path)
        kwargs["credentials"] = creds
    return firestore.Client(**kwargs)

def load_mapping(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")
    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text())
        if not isinstance(data, dict):
            raise ValueError("JSON mapping must be an object of {link_id: target_id}.")
        return {str(k): str(v) for k, v in data.items()}
    if p.suffix.lower() == ".csv":
        out = {}
        with p.open(newline="") as f:
            for row in csv.DictReader(f):
                lid = (row.get("link_id") or "").strip()
                tid = (row.get("target_id") or "").strip()
                if lid and tid:
                    out[lid] = tid
        return out
    raise ValueError("Unsupported mapping format. Use .json or .csv")

def build_target_lookup_by_link_id(dest_client: firestore.Client, dest_campaign_id: str, link_field: str = "link_id"
) -> Dict[str, firestore.DocumentReference]:
    lookup: Dict[str, firestore.DocumentReference] = {}
    targets_col = dest_client.collection(f"campaigns/{dest_campaign_id}/targets")
    for target_snap in targets_col.stream():
        data = target_snap.to_dict() or {}
        lid = data.get(link_field)
        if lid:
            lookup[str(lid)] = target_snap.reference
    return lookup

def verify_doc_exists(doc_ref: firestore.DocumentReference) -> bool:
    try:
        return doc_ref.get().exists
    except NotFound:
        return False

def preflight_read(client: firestore.Client, label: str, collection_hint: Optional[str] = None, campaign_id: Optional[str] = None):
    try:
        if collection_hint:
            _ = next(client.collection(collection_hint).limit(1).stream(), None)
        if campaign_id:
            _ = client.document(f"campaigns/{campaign_id}").get()
    except PermissionDenied:
        print(
            f"[FATAL] PermissionDenied on {label} "
            f"(project={client.project}, db={client._database_string.split('/')[-1]}).\n"
            "Fixes:\n"
            " - Enable firestore.googleapis.com on the project.\n"
            " - Grant IAM (source: roles/datastore.viewer, dest: roles/datastore.user/editor).\n"
            " - Ensure the database name exists (Console → Firestore → Databases).\n",
            file=sys.stderr,
        )
        raise


# ---------- Counter updates ----------

def update_counters_increment(
    dest_client: firestore.Client,
    link_counts: Dict[str, Tuple[int, Optional[firestore.SERVER_TIMESTAMP]]],
    business_counts: Dict[str, Tuple[int, Optional[firestore.SERVER_TIMESTAMP]]],
    link_last_ts: Dict[str, Optional[datetime]],
    business_last_ts: Dict[str, Optional[datetime]],
    dry_run: bool,
):
    """Fast, no-reads: FieldValue.increment and set last_hit_at to our computed max."""
    from google.cloud.firestore_v1 import Increment
    batch = dest_client.batch()
    batch_count = 0
    BATCH_SIZE = 450

    # links
    for link_id, n in ((lid, cnt) for lid, cnt in link_counts.items()):
        ref = dest_client.collection("links").document(link_id)
        payload = {"hit_count": Increment(n)}
        if link_last_ts.get(link_id):
            payload["last_hit_at"] = link_last_ts[link_id]
        if dry_run:
            print(f"[DRY-RUN] increment links/{link_id} by {n}, last_hit_at={link_last_ts.get(link_id)}")
        else:
            batch.set(ref, payload, merge=True)
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                batch.commit(); batch = dest_client.batch(); batch_count = 0

    # businesses
    for business_id, n in ((bid, cnt) for bid, cnt in business_counts.items()):
        ref = dest_client.collection("businesses").document(business_id)
        payload = {"hit_count": Increment(n)}
        if business_last_ts.get(business_id):
            payload["last_hit_at"] = business_last_ts[business_id]
        if dry_run:
            print(f"[DRY-RUN] increment businesses/{business_id} by {n}, last_hit_at={business_last_ts.get(business_id)}")
        else:
            batch.set(ref, payload, merge=True)
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                batch.commit(); batch = dest_client.batch(); batch_count = 0

    if not dry_run and batch_count > 0:
        batch.commit()

def update_counters_transactional(
    dest_client: firestore.Client,
    link_counts: Dict[str, int],
    business_counts: Dict[str, int],
    link_last_ts: Dict[str, Optional[datetime]],
    business_last_ts: Dict[str, Optional[datetime]],
    dry_run: bool,

    # campaign aggregates (pass from your migrate loop)
    campaign_id: Optional[str] = None,
    campaign_hits: int = 0,
    campaign_unique_links: int = 0,
    campaign_unique_ips: int = 0,
    campaign_unique_targets: int = 0,   # pass 0 if none
    campaign_last_ts: Optional[datetime] = None,
) -> None:
    """Simple read -> compute -> set(merge=True) updates; no transactions, no Increments."""

    # ---- LINKS ----
    for link_id, inc in link_counts.items():
        ref = dest_client.collection("links").document(link_id)
        last_ts = link_last_ts.get(link_id)

        if dry_run:
            print(f"[DRY-RUN] links/{link_id}: hit_count += {inc}, last_hit_at <= {last_ts}")
            continue

        snap = ref.get()
        data = snap.to_dict() or {}
        current = int(data.get("hit_count") or 0)
        updated = current + inc

        existing_last = data.get("last_hit_at")
        final_last = max(existing_last, last_ts) if (existing_last and last_ts) else (last_ts or existing_last)

        payload = {"hit_count": updated}
        if final_last:
            payload["last_hit_at"] = final_last

        ref.set(payload, merge=True)

    # ---- BUSINESSES ----
    for business_id, inc in business_counts.items():
        ref = dest_client.collection("businesses").document(business_id)
        last_ts = business_last_ts.get(business_id)

        if dry_run:
            print(f"[DRY-RUN] businesses/{business_id}: hit_count += {inc}, last_hit_at <= {last_ts}")
            continue

        snap = ref.get()
        data = snap.to_dict() or {}
        current = int(data.get("hit_count") or 0)
        updated = current + inc

        existing_last = data.get("last_hit_at")
        final_last = max(existing_last, last_ts) if (existing_last and last_ts) else (last_ts or existing_last)

        payload = {"hit_count": updated}
        if final_last:
            payload["last_hit_at"] = final_last

        ref.set(payload, merge=True)

    # ---- CAMPAIGN TOTALS ----
    if campaign_id:
        ref = dest_client.document(f"campaigns/{campaign_id}")

        if dry_run:
            print(
                f"[DRY-RUN] campaigns/{campaign_id}: "
                f"totals.hits += {campaign_hits}, totals.links += {campaign_unique_links}, "
                f"totals.unique_ips += {campaign_unique_ips}, totals.targets += {campaign_unique_targets}, "
                f"last_hit_at <= {campaign_last_ts}"
            )
            return

        snap = ref.get()
        data = snap.to_dict() or {}
        totals = data.get("totals") or {}

        new_totals = {
            "hits":       int(totals.get("hits") or 0) + campaign_hits,
            "links":      int(totals.get("links") or 0) + campaign_unique_links,
            "unique_ips": int(totals.get("unique_ips") or 0) + campaign_unique_ips,
            "targets":    int(totals.get("targets") or 0) + int(campaign_unique_targets or 0),
        }

        existing_last = data.get("last_hit_at")
        final_last = max(existing_last, campaign_last_ts) if (existing_last and campaign_last_ts) else (campaign_last_ts or existing_last)

        payload = {
            "totals": new_totals,                          # merges per-key inside 'totals'
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        if final_last:
            payload["last_hit_at"] = final_last

        ref.set(payload, merge=True)

# ---------- Migration ----------

def migrate_hits(
    source_client: firestore.Client,
    dest_client: firestore.Client,
    source_campaign: str,
    dest_campaign_id: str,
    owner_id: Optional[str],
    src_collection: str,
    dst_collection: str,
    limit: Optional[int],
    dry_run: bool,
    preserve_doc_ids: bool,
    verify_refs: bool,
    target_lookup_by_link: Dict[str, firestore.DocumentReference],
    fallback_link_to_targetid: Dict[str, str],
    counter_mode: str,  # "increment" | "transactional"
):
    # Filter source by campaign == source_campaign, src_collection=hits
    src_query = source_client.collection(src_collection).where("campaign", "==", source_campaign)
    docs_iter = src_query.stream()

    batch = dest_client.batch()
    BATCH_SIZE = 400  # keep margin for large payloads
    batch_count = 0

    processed = 0
    matched_targets = 0
    matched_via_fallback = 0
    missing_target = 0
    missing_business = 0

    # aggregation
    link_counts: Dict[str, int] = defaultdict(int)
    business_counts: Dict[str, int] = defaultdict(int)
    link_last_ts: Dict[str, Optional[datetime]] = defaultdict(lambda: None)
    business_last_ts: Dict[str, Optional[datetime]] = defaultdict(lambda: None)

    # campaign-level aggregation
    campaign_hits = 0
    campaign_unique_links: Set[str] = set()
    campaign_unique_ips: Set[str] = set()
    campaign_last_ts: Optional[datetime] = None
    campaign_unique_targets: Set[str] = set()

    campaign_ref = dest_client.document(f"campaigns/{dest_campaign_id}")
    if verify_refs and not dry_run and not verify_doc_exists(campaign_ref):
        print(f"[WARN] campaign_ref {campaign_ref.path} does not exist in destination.", file=sys.stderr)

    for snap in docs_iter: #interates though each "old" hit document
        data = snap.to_dict() or {}

        business_id = data.get("business_id")
        if not business_id:
            print(f"[WARN] Skipping {snap.id}: missing business_id", file=sys.stderr)
            missing_business += 1
            continue

        link_id = data.get("link_id")
        template_old = data.get("template") or data.get("template_id")
        ts = data.get("ts")  # Firestore timestamp

        # Destination refs
        business_ref = dest_client.document(f"businesses/{business_id}")
        if verify_refs and not dry_run and not verify_doc_exists(business_ref):
            print(f"[WARN] business_ref {business_ref.path} does not exist in destination.", file=sys.stderr)

        # target_ref resolution (in DEST campaign)
        target_ref = None
        if link_id and link_id in target_lookup_by_link:
            target_ref = target_lookup_by_link[link_id]
            matched_targets += 1
        elif link_id and link_id in fallback_link_to_targetid:
            target_id = fallback_link_to_targetid[link_id]
            if target_id:
                target_ref = dest_client.document(f"campaigns/{dest_campaign_id}/targets/{target_id}")
                matched_via_fallback += 1
        else:
            if link_id:
                missing_target += 1

        if ts:
            campaign_last_ts = max(filter(None, [campaign_last_ts, ts]))
        campaign_hits += 1
        if link_id:
            campaign_unique_links.add(link_id)
        ip_hash = data.get("ip_hash")
        if ip_hash:
            campaign_unique_ips.add(ip_hash)
        if target_ref:
            campaign_unique_targets.add(target_ref.id)  # or target_ref.path if you prefer


        # Build new hit doc
        new_doc = {
            "business_ref": business_ref,
            "campaign_ref": campaign_ref,
            "device_type": data.get("device_type"),
            "geo_city": data.get("geo_city"),
            "geo_country": data.get("geo_country"),
            "geo_lat": data.get("geo_lat"),
            "geo_lon": data.get("geo_lon"),
            "geo_region": data.get("geo_region"),
            "geo_source": data.get("geo_source"),
            "ip_hash": data.get("ip_hash"),
            "link_id": link_id,
            "template_id": template_old,
            "ts": ts,
            "ua_browser": data.get("ua_browser"),
            "ua_os": data.get("ua_os"),
            "user_agent": data.get("user_agent"),
        }
        if owner_id:
            new_doc["owner_id"] = owner_id
        if target_ref:
            new_doc["target_ref"] = target_ref

        # Destination hit doc
        dst_ref = (
            dest_client.collection(dst_collection).document(snap.id)
            if preserve_doc_ids else
            dest_client.collection(dst_collection).document()
        )

        if dry_run:
            print(f"[DRY-RUN] hit {snap.id} -> {dst_ref.id} (link={link_id}, business={business_id})")
        else:
            batch.set(dst_ref, new_doc)
            batch_count += 1
            if batch_count >= BATCH_SIZE:
                try:
                    batch.commit()
                except (GoogleAPICallError, RetryError, AlreadyExists) as e:
                    print(f"[ERROR] Batch commit failed: {e}", file=sys.stderr)
                    raise
                batch = dest_client.batch()
                batch_count = 0

        # aggregate counters
        if link_id:
            link_counts[link_id] += 1
            if ts:
                link_last_ts[link_id] = max(filter(None, [link_last_ts[link_id], ts]))
        business_counts[business_id] += 1
        if ts:
            business_last_ts[business_id] = max(filter(None, [business_last_ts[business_id], ts]))

        processed += 1
        if limit and processed >= limit:
            break

    if not dry_run and batch_count > 0:
        try:
            batch.commit()
        except (GoogleAPICallError, RetryError, AlreadyExists) as e:
            print(f"[ERROR] Final batch commit failed: {e}", file=sys.stderr)
            raise

    # ---- COUNTER UPDATES ----
    if counter_mode == "increment":
        update_counters_increment(dest_client, link_counts, business_counts, link_last_ts, business_last_ts, dry_run)
    else:
        update_counters_transactional(
            dest_client=dest_client,
            link_counts=link_counts,
            business_counts=business_counts,
            link_last_ts=link_last_ts,
            business_last_ts=business_last_ts,
            dry_run=dry_run,

            # NEW: campaign params
            campaign_id=dest_campaign_id,
            campaign_hits=processed,
            campaign_unique_links=len(campaign_unique_links),
            campaign_unique_ips=len(campaign_unique_ips),
            campaign_unique_targets=len(campaign_unique_targets) if campaign_unique_targets else None,
            campaign_last_ts=campaign_last_ts,
        )

    print(
        f"[DONE] hits processed={processed} | target_ref(by link_id)={matched_targets} "
        f"| target_ref(via fallback)={matched_via_fallback} | missing_target={missing_target} "
        f"| missing_business_id={missing_business} | links_touched={len(link_counts)} | businesses_touched={len(business_counts)}"
        f"{' | dry-run' if dry_run else ''}"
    )


# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(description="Migrate Firestore 'hits' and update counters (links, businesses).")

    # Source (prod)
    parser.add_argument("--source-project", default="gb-qr-tracker")
    parser.add_argument("--source-db", default="backup-1")
    parser.add_argument("--src-collection", default="hits")
    parser.add_argument("--source-credentials", required=True, help="Path to source SA JSON (read access).")
    parser.add_argument("--source-campaign", required=True, help="Value of old 'campaign' field to filter (e.g., 'groessig-01').")

    # Destination (dev)
    parser.add_argument("--dest-project", default="gb-qr-tracker")
    parser.add_argument("--dest-db", default="(default)")
    parser.add_argument("--dst-collection", default="hits")
    parser.add_argument("--dest-credentials", required=True, help="Path to dest SA JSON (write access).")
    parser.add_argument("--dest-campaign-id", required=True, help="Destination campaign doc id under /campaigns/{id}.")

    # Optional owner_id
    parser.add_argument("--owner-id", help="Value for owner_id (optional).")

    # Optional fallback mapping for target_ref
    parser.add_argument("--target-mapping", help="JSON or CSV mapping link_id -> target_id (optional).")

    # Behavior
    parser.add_argument("--counter-mode", choices=["increment", "transactional"], default="transactional",
                        help="How to update counters: fast increments (default) or read-modify-write transactions.")
    parser.add_argument("--limit", type=int, help="Only migrate up to N hits (testing).")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing.")
    parser.add_argument("--no-preserve-ids", action="store_true", help="Do not reuse source document IDs.")
    parser.add_argument("--verify-refs", action="store_true", help="Warn if campaign/business refs are missing.")

    args = parser.parse_args()

    # Init clients with distinct SAs
    source_client = init_client(args.source_project, args.source_db, args.source_credentials)
    dest_client   = init_client(args.dest_project, args.dest_db, args.dest_credentials)

    # Preflights
    preflight_read(source_client, label="SOURCE", collection_hint=args.src_collection)
    preflight_read(dest_client,   label="DEST",   campaign_id=args.dest_campaign_id)

    # Targets lookup from DEST
    target_lookup = build_target_lookup_by_link_id(dest_client, args.dest_campaign_id, link_field="link_id")
    print(f"[INFO] Loaded {len(target_lookup)} targets from /campaigns/{args.dest_campaign_id}/targets (by link_id).")

    # Optional fallback mapping
    fallback_mapping = load_mapping(args.target_mapping) if args.target_mapping else {}

    migrate_hits(
        source_client=source_client,
        dest_client=dest_client,
        source_campaign=args.source_campaign,
        dest_campaign_id=args.dest_campaign_id,
        owner_id=args.owner_id,
        src_collection=args.src_collection,
        dst_collection=args.dst_collection,
        limit=args.limit,
        dry_run=args.dry_run,
        preserve_doc_ids=not args.no_preserve_ids,
        verify_refs=args.verify_refs,
        target_lookup_by_link=target_lookup,
        fallback_link_to_targetid=fallback_mapping,
        counter_mode=args.counter_mode,
    )

if __name__ == "__main__":
    main()
