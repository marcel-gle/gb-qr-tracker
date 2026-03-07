#!/usr/bin/env python3
"""
Export customer businesses to CSV from Firestore.

The script mirrors the web export logic:

- Inputs:
  - user_id (link owner)
  - optional campaign_ids (filter links)
  - optional customer_id (defaults to user_id)

- Steps:
  1) Query `links` with owner_id == user_id
     - If campaign_ids is given, restrict via campaign_ref IN [campaignRefs] (<=10 at a time).
     - Collect normalized business_ref.id values and per-business campaign IDs.
  2) Load overlays from `customers/{customer_id}/businesses`.
     - For each overlay, resolve its canonical `business_ref` under `/businesses/{id}`.
     - Merge canonical + overlay fields (overlay wins on conflicts).
     - Keep only businesses referenced by links from step 1.
  3) Compute derived fields:
     - "Letzter Kontakt": most recent `date_postal_office` from the campaigns touching
       each business, formatted as "dd.mm.yyyy, HH:MM" (24‑hour).
     - "Blacklisted": read `customers/{customer_id}/blacklist` and mark businesses whose
       normalized ID matches a blacklist document ID.
  4) Emit CSV with columns:
     ["Unternehmens Name", "Kontaktperson", "Email", "Telefon",
      "Stadt", "Postleitzahl", "Adresse", "Aufrufe",
      "Letzter Kontakt", "Blacklisted"].

The core function `export_businesses_csv` returns CSV bytes with UTF‑8 BOM,
ready to be sent as a file download in a web context:

    response_body = export_businesses_csv(user_id, campaign_ids, customer_id)
    # Flask/FastAPI headers:
    #   Content-Type: text/csv; charset=utf-8
    #   Content-Disposition: attachment; filename="kontakte_export_YYYY-MM-DD.csv"
"""

from __future__ import annotations

import argparse
import csv
import io
import pickle
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import DeadlineExceeded

try:
    # Python 3.9+ standard library
    from zoneinfo import ZoneInfo  # type: ignore
except ImportError:  # pragma: no cover - fallback for older runtimes
    ZoneInfo = None  # type: ignore

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm is optional
    def tqdm(x, **kwargs):
        return x


# Hard-coded service account paths (aligned with other scripts)
SERVICE_ACCOUNT_PATH_PROD = (
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/"
    "gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"
)
SERVICE_ACCOUNT_PATH_DEV = (
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/"
    "gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
)


def _init_firebase(env: str = "dev") -> None:
    """
    Initialize Firebase Admin SDK once using the env-specific service account JSON.

    env: "dev" | "prod" (defaults to "dev" for safety when running scripts).
    """
    if firebase_admin._apps:  # type: ignore[attr-defined]
        return

    if env == "prod":
        service_account_path = SERVICE_ACCOUNT_PATH_PROD
    else:
        service_account_path = SERVICE_ACCOUNT_PATH_DEV

    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)


def _normalize_business_id(value: Any) -> str:
    """
    Normalize a business ID exactly like the TS normalizeBusinessId:

    - Cast to string and trim whitespace
    - Lowercase
    - Replace any character not in [a-z0-9äöüß] with '-'
    - Collapse multiple hyphens into one
    - Trim leading/trailing hyphens
    """
    if value is None:
        return ""
    v = str(value).strip()
    # Allow A-Z, a-z, 0-9, and German umlauts (ä, ö, ü, ß)
    # Replace everything else with hyphens, then normalize and lowercase.
    v = re.sub(r"[^A-Za-z0-9äöüÄÖÜß]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    return v.lower()


def _extract_normalized_business_id(business_ref: Any) -> Optional[str]:
    """
    Extract and normalize a business ID from a Firestore reference or string.

    Supports:
    - DocumentReference to `/businesses/{id}`
    - String ID
    - String path like `/businesses/{id}` or `businesses/{id}`
    """
    if business_ref is None:
        return None

    # DocumentReference (preferred)
    if hasattr(business_ref, "id"):
        raw_id = getattr(business_ref, "id", None)
        return _normalize_business_id(raw_id)

    # Path-like string
    if isinstance(business_ref, str):
        if "/businesses/" in business_ref:
            raw_id = business_ref.split("/businesses/")[-1]
        else:
            raw_id = business_ref
        return _normalize_business_id(raw_id)

    return None


def _ts_to_datetime(ts: Any) -> Optional[datetime]:
    """
    Convert Firestore Timestamp or datetime to timezone-aware datetime.

    Prefer Europe/Berlin for human-facing export; fall back to UTC if zoneinfo
    is not available.
    """
    if ts is None:
        return None

    # Firestore Timestamp has .timestamp()
    if hasattr(ts, "timestamp"):
        base = datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc)
    elif isinstance(ts, datetime):
        base = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    else:
        return None

    if ZoneInfo is None:
        return base

    try:
        return base.astimezone(ZoneInfo("Europe/Berlin"))  # type: ignore[arg-type]
    except Exception:
        return base


def _format_last_contact(ts: Optional[datetime]) -> str:
    """Format datetime as dd.mm.yyyy, HH:MM or '—' if missing."""
    if not ts:
        return "—"
    return ts.strftime("%d.%m.%Y, %H:%M")


def _chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    """Yield fixed-size chunks from a sequence."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def export_businesses_csv(
    user_id: str,
    campaign_ids: Optional[Sequence[str]] = None,
    customer_id: Optional[str] = None,
    env: str = "prod",
    pickle_path: Optional[str] = None,
) -> bytes:
    """
    Export merged canonical + overlay businesses for a given user/customer as CSV bytes.

    - `user_id`: owner_id used on `links` documents.
    - `campaign_ids`: optional list of campaign IDs to restrict links.
    - `customer_id`: Firestore customer ID whose overlays/blacklist should be used;
      if omitted, falls back to `user_id`.
    - `env`: "dev" or "prod"; controls which Firestore project is used when the
      script initializes Firebase itself. Defaults to "prod" for backwards
      compatibility with the original behavior.
    """
    if not user_id:
        raise ValueError("user_id is required")

    # --- Initialization / context ---
    print("=" * 60)
    print("Exporting businesses CSV")
    print("=" * 60)
    print(f"  env          = {env}")
    print(f"  user_id      = {user_id}")
    print(f"  customer_id  = {customer_id or user_id}")
    if pickle_path:
        print(f"  pickle_path  = {pickle_path}")
    if campaign_ids:
        print(f"  campaign_ids = {list(campaign_ids)}")
    else:
        print("  campaign_ids = ALL campaigns for this user")
    print()

    _init_firebase(env=env)
    db = firestore.client()

    # Determine which customer document to use for overlays/blacklist.
    customer_id = customer_id or user_id

    # ------------------------------------------------------------------
    # Step 1: Determine which businesses/targets to include.
    #
    # - If campaign_ids is provided, we export ALL targets for those campaigns:
    #   one CSV row per target (contact), including ones without links.
    # - If no campaign_ids is provided, we fall back to the original behavior
    #   of using links (owner_id == user_id) across all campaigns, and export
    #   one row per unique business.
    # ------------------------------------------------------------------
    business_ids: Set[str] = set()
    biz_to_campaign_ids: Dict[str, Set[str]] = defaultdict(set)
    target_records: List[Dict[str, Any]] = []

    if campaign_ids:
        print("[Step 1] Loading targets for campaign(s)...")
        target_count_by_business: Dict[str, int] = defaultdict(int)
        total_targets = 0
        for cid in campaign_ids:
            campaign_ref = db.collection("campaigns").document(cid)
            targets_ref = campaign_ref.collection("targets")

            page_size = 1000
            last_doc = None

            print(f"[Step 1]  Campaign {cid}: scanning targets...")
            while True:
                q = targets_ref.limit(page_size)
                if last_doc is not None:
                    q = q.start_after(last_doc)

                try:
                    batch = list(q.stream())
                except DeadlineExceeded:
                    if page_size > 200:
                        page_size = max(200, page_size // 2)
                        print(
                            f"[Step 1]  Deadline exceeded on targets for {cid}; "
                            f"retrying with page_size={page_size}..."
                        )
                        continue
                    raise

                if not batch:
                    break

                for snap in tqdm(
                    batch, desc=f"Targets {cid}", unit="target", leave=False
                ):
                    data = snap.to_dict() or {}
                    biz_ref = data.get("business_ref")
                    norm_id = _extract_normalized_business_id(biz_ref)
                    if not norm_id:
                        continue

                    total_targets += 1
                    business_ids.add(norm_id)
                    biz_to_campaign_ids[norm_id].add(cid)
                    target_count_by_business[norm_id] += 1

                    target_records.append(
                        {
                            "norm_id": norm_id,
                            "business_ref": biz_ref,
                            "campaign_id": cid,
                        }
                    )

                if len(batch) < page_size:
                    break
                last_doc = batch[-1]

        print(f"[Step 1] Total targets scanned: {total_targets}")
        print(f"[Step 1] Unique business IDs from targets: {len(business_ids)}")
        print(
            f"[Step 1] Target records recorded (one per row/contact): "
            f"{len(target_records)}"
        )

        # Duplicate business diagnostics for this campaign export.
        duplicate_businesses = {
            bid: cnt for bid, cnt in target_count_by_business.items() if cnt > 1
        }
        print(
            f"[Step 1] Businesses appearing in multiple targets: "
            f"{len(duplicate_businesses)}"
        )
        if duplicate_businesses:
            example_dupes = list(sorted(duplicate_businesses.items(), key=lambda x: -x[1]))[:5]
            print("[Step 1]  Example duplicates (business_id -> target_count):")
            for bid, cnt in example_dupes:
                print(f"[Step 1]    {bid}: {cnt}")
        print()

        if not target_records:
            # Nothing to export for these campaigns.
            output = io.StringIO()
            writer = csv.writer(output, delimiter=";")
            writer.writerow(
                [
                    "Unternehmens Name",
                    "Kontaktperson",
                    "Email",
                    "Telefon",
                    "Stadt",
                    "Postleitzahl",
                    "Adresse",
                    "Aufrufe",
                    "Letzter Kontakt",
                    "Blacklisted",
                ]
            )
            return ("\ufeff" + output.getvalue()).encode("utf-8")

    else:
        # Original behavior: use links across all campaigns, one row per business.
        links_ref = db.collection("links")
        link_count = 0

        q = links_ref.where("owner_id", "==", user_id)
        for snap in q.stream():
            data = snap.to_dict() or {}
            link_count += 1
            biz_ref = data.get("business_ref")
            norm_id = _extract_normalized_business_id(biz_ref)
            if not norm_id:
                continue
            business_ids.add(norm_id)

            camp_ref = data.get("campaign_ref")
            camp_id = getattr(camp_ref, "id", None)
            if camp_id:
                biz_to_campaign_ids[norm_id].add(camp_id)

        print(f"[Step 1] Loaded {link_count} link(s) for owner_id={user_id}")
        print(f"[Step 1] Found {len(business_ids)} unique business IDs from links")
        print(
            f"[Step 1] Have campaign mappings for "
            f"{len(biz_to_campaign_ids)} business IDs"
        )
        print()

        if not business_ids:
            output = io.StringIO()
            writer = csv.writer(output, delimiter=";")
            writer.writerow(
                [
                    "Unternehmens Name",
                    "Kontaktperson",
                    "Email",
                    "Telefon",
                    "Stadt",
                    "Postleitzahl",
                    "Adresse",
                    "Aufrufe",
                    "Letzter Kontakt",
                    "Blacklisted",
                ]
            )
            return ("\ufeff" + output.getvalue()).encode("utf-8")

    # ------------------------------------------------------------------
    # Step 2: Load overlays for this customer and merge with canonical
    #         `/businesses/{id}` documents, filtered by business_ids.
    #         Results are stored per normalized business ID.
    # ------------------------------------------------------------------
    overlays_ref = (
        db.collection("customers").document(customer_id).collection("businesses")
    )

    # Per-business merged view (canonical + overlay); keyed by normalized ID.
    merged_by_business: Dict[str, Dict[str, Any]] = {}

    # Paginate overlays to avoid timeouts on very large collections and show progress.
    page_size = 1000
    last_doc = None
    overlay_scanned = 0
    overlay_matched = 0

    print(f"[Step 2] Loading customer overlays for {customer_id}...")
    while True:
        query = overlays_ref.limit(page_size)
        if last_doc is not None:
            query = query.start_after(last_doc)

        try:
            batch = list(query.stream())
        except DeadlineExceeded:
            # If a page still times out, reduce page size and retry once.
            if page_size > 200:
                page_size = max(200, page_size // 2)
                print(
                    f"[Step 2] Deadline exceeded when loading overlays; "
                    f"retrying with page_size={page_size}..."
                )
                continue
            raise

        if not batch:
            break

        for overlay_snap in tqdm(
            batch, desc="Overlays", unit="biz", leave=False
        ):
            overlay_scanned += 1
            overlay_id = overlay_snap.id
            norm_overlay_id = _normalize_business_id(overlay_id)
            if norm_overlay_id not in business_ids:
                continue
            overlay_matched += 1

            overlay_data = overlay_snap.to_dict() or {}

            # For now, store overlay data keyed by normalized overlay ID; canonical
            # enrichment will happen separately (per business_id) to avoid duplicate
            # lookups.
            merged_by_business.setdefault(norm_overlay_id, {}).update(overlay_data)

        if len(batch) < page_size:
            break
        last_doc = batch[-1]

    print(f"[Step 2] Overlays scanned: {overlay_scanned}")
    print(f"[Step 2] Overlays matched to business_ids: {overlay_matched}")
    print(f"[Step 2] Businesses with overlay data: {len(merged_by_business)}")

    # Diagnostics: which business_ids had no overlay?
    missing_overlays = sorted(business_ids - set(merged_by_business.keys()))
    print(f"[Step 2] Businesses without overlay data: {len(missing_overlays)}")
    if missing_overlays:
        print("[Step 2]  Example business_ids without overlay:")
        for bid in missing_overlays[:5]:
            print(f"[Step 2]    {bid}")
    print()

    # Enrich merged_by_business with canonical data for all business_ids we saw
    # (some may not have overlays, e.g. excluded targets).
    if business_ids:
        canonical_refs = [
            db.collection("businesses").document(bid) for bid in sorted(business_ids)
        ]
        canonical_found: Set[str] = set()
        print("[Step 2] Loading canonical businesses for all business_ids...")
        for ref_batch in _chunked(canonical_refs, 500):
            snaps = db.get_all(list(ref_batch))
            for snap in snaps:
                if not getattr(snap, "exists", False):
                    continue
                canonical_data = snap.to_dict() or {}
                norm_id = _normalize_business_id(snap.id)
                canonical_found.add(norm_id)
                # Merge canonical into any existing overlay-backed entry; otherwise
                # create canonical-only entry.
                base = merged_by_business.get(norm_id, {})
                merged = {**canonical_data, **base}
                merged["_normalized_id"] = norm_id
                merged_by_business[norm_id] = merged

        print(
            f"[Step 2] Businesses with merged canonical+overlay data: "
            f"{len(merged_by_business)}"
        )

        # Diagnostics: which business_ids have no canonical data?
        missing_canonical = sorted(business_ids - canonical_found)
        print(f"[Step 2] Businesses without canonical data: {len(missing_canonical)}")
        if missing_canonical:
            print("[Step 2]  Example business_ids without canonical data:")
            for bid in missing_canonical[:5]:
                print(f"[Step 2]    {bid}")
        print()

    # ------------------------------------------------------------------
    # Step 3: Compute "Letzter Kontakt" from campaigns.
    # ------------------------------------------------------------------
    # Collect all campaign IDs we actually need to look up.
    merged_ids = set(merged_by_business.keys())
    needed_campaign_ids: Set[str] = set()
    for norm_id, camp_ids in biz_to_campaign_ids.items():
        if norm_id in merged_ids:
            needed_campaign_ids.update(camp_ids)

    campaign_dates: Dict[str, datetime] = {}
    if needed_campaign_ids:
        campaign_refs = [
            db.collection("campaigns").document(cid) for cid in sorted(needed_campaign_ids)
        ]
        for ref_batch in _chunked(campaign_refs, 500):
            snaps = db.get_all(list(ref_batch))
            for snap in snaps:
                if not getattr(snap, "exists", False):
                    continue
                data = snap.to_dict() or {}
                ts = data.get("date_postal_office")
                dt = _ts_to_datetime(ts)
                if dt:
                    campaign_dates[snap.id] = dt

    num_with_last_contact = sum(
        1 for _id in merged_ids if any(c in campaign_dates for c in biz_to_campaign_ids.get(_id, ()))
    )
    print(f"[Step 3] Campaigns with date_postal_office loaded: {len(campaign_dates)}")
    print(f"[Step 3] Businesses with at least one contact date: {num_with_last_contact}")
    print()

    last_contact_by_business: Dict[str, Optional[datetime]] = {}
    for norm_id in merged_by_business.keys():
        best_dt: Optional[datetime] = None
        for cid in biz_to_campaign_ids.get(norm_id, ()):
            dt = campaign_dates.get(cid)
            if dt and (best_dt is None or dt > best_dt):
                best_dt = dt
        last_contact_by_business[norm_id] = best_dt

    # ------------------------------------------------------------------
    # Step 4: Load blacklist for this customer to flag businesses.
    # ------------------------------------------------------------------
    blacklist_ids: Set[str] = set()
    blacklist_ref = (
        db.collection("customers").document(customer_id).collection("blacklist")
    )
    for snap in blacklist_ref.stream():
        # Document ID represents (normalized) business ID
        blacklist_ids.add(_normalize_business_id(snap.id))

    print(f"[Step 4] Blacklist entries for customer {customer_id}: {len(blacklist_ids)}")
    print()

    # ------------------------------------------------------------------
    # Step 5: Build CSV rows in the requested order.
    #
    # - If campaign_ids were provided, we emit one row per target record.
    # - Otherwise (legacy mode), we emit one row per business (using
    #   merged_by_business.values()).
    # ------------------------------------------------------------------
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    headers = [
        "Unternehmens Name",
        "Kontaktperson",
        "Email",
        "Telefon",
        "Stadt",
        "Postleitzahl",
        "Adresse",
        "Aufrufe",
        "Letzter Kontakt",
        "Blacklisted",
    ]
    writer.writerow(headers)

    def _emit_row_for_business(norm_id: str, biz: Dict[str, Any]) -> None:
        company_name = (
            biz.get("business_name")
            or biz.get("name")  # fallback if canonical name missing
            or ""
        )
        contact_person = biz.get("name") or ""
        email = biz.get("email") or ""
        phone = biz.get("phone") or ""
        city = biz.get("city") or ""
        postcode = biz.get("postcode") or ""

        address = biz.get("address") or ""
        if not address:
            street = biz.get("street") or ""
            house_number = biz.get("house_number") or ""
            address = f"{street} {house_number}".strip()

        hit_count = biz.get("hit_count") or 0
        try:
            hit_count_int = int(hit_count)
        except (TypeError, ValueError):
            hit_count_int = 0

        last_contact_dt = last_contact_by_business.get(norm_id)
        last_contact_str = _format_last_contact(last_contact_dt)

        blacklisted_str = "Ja" if norm_id in blacklist_ids else "Nein"

        writer.writerow(
            [
                company_name,
                contact_person,
                email,
                phone,
                city,
                postcode,
                address,
                hit_count_int,
                last_contact_str,
                blacklisted_str,
            ]
        )

    if campaign_ids:
        # One row per target record (contact). Multiple targets that reference the
        # same business will result in multiple rows, as requested.
        print("[Step 5] Emitting rows per target record...")
        for rec in target_records:
            norm_id = rec["norm_id"]
            biz = merged_by_business.get(norm_id, {})
            _emit_row_for_business(norm_id, biz)
    else:
        # Legacy mode: one row per unique business.
        print("[Step 5] Emitting rows per unique business (legacy mode)...")
        for norm_id in sorted(merged_by_business.keys()):
            biz = merged_by_business[norm_id]
            _emit_row_for_business(norm_id, biz)

    # Prepend UTF‑8 BOM for Excel compatibility.
    csv_text = output.getvalue()
    csv_rows = len(csv_text.splitlines()) - 1  # exclude header
    print(f"[Step 5] CSV rows (excluding header): {csv_rows}")

    # Optionally persist final results so a later run can reuse or inspect them.
    if pickle_path:
        try:
            with open(pickle_path, "wb") as pf:
                pickle.dump(
                    {
                        "merged_businesses": merged_by_business,
                        "last_contact_by_business": last_contact_by_business,
                        "blacklist_ids": list(blacklist_ids),
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "user_id": user_id,
                        "customer_id": customer_id,
                        "campaign_ids": list(campaign_ids or []),
                        "env": env,
                    },
                    pf,
                )
            print(f"[Step 5] Pickle saved to: {pickle_path}")
        except Exception as e:
            print(f"[Step 5] Warning: failed to write pickle to {pickle_path}: {e}")

    return ("\ufeff" + csv_text).encode("utf-8")


def _build_default_filename() -> str:
    today = datetime.now().date().isoformat()
    return f"kontakte_export_{today}.csv"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export merged customer businesses for a user as CSV, "
            "mirroring the web export behavior."
        )
    )
    parser.add_argument(
        "--user-id",
        required=True,
        help="User ID whose links (owner_id) should be used to filter businesses.",
    )
    parser.add_argument(
        "--customer-id",
        help="Customer ID for overlays/blacklist (defaults to --user-id).",
    )
    parser.add_argument(
        "--campaign-id",
        action="append",
        dest="campaign_ids",
        help=(
            "Optional campaign ID to restrict links; can be passed multiple times. "
            "If omitted, all campaigns for this user are included."
        ),
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment / Firestore project to use (default: dev).",
    )
    parser.add_argument(
        "--output",
        help=(
            "Output CSV file path. Defaults to kontakte_export_YYYY-MM-DD.csv "
            "in the current directory."
        ),
    )
    parser.add_argument(
        "--pickle-output",
        help=(
            "Optional path to write a pickle with the final merged data "
            "(for re-use if a later run fails)."
        ),
    )

    args = parser.parse_args()

    csv_bytes = export_businesses_csv(
        user_id=args.user_id,
        campaign_ids=args.campaign_ids,
        customer_id=args.customer_id,
        env=args.env,
        pickle_path=args.pickle_output,
    )

    output_path = args.output or _build_default_filename()
    with open(output_path, "wb") as f:
        f.write(csv_bytes)

    print(f"✅ Export completed: {output_path}")


if __name__ == "__main__":
    main()

