import argparse
import csv
import sys
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

from google.cloud import firestore as gcf
from google.cloud.firestore_v1 import ArrayUnion
from google.oauth2 import service_account as gsa


# Default service account paths (adjust if your paths change)
DEV_SERVICE_ACCOUNT = (
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/"
    "gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
)
PROD_SERVICE_ACCOUNT = (
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/"
    "gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"
)


def get_ci(row: dict, *names: str) -> Optional[str]:
    """
    Case-insensitive getter for a CSV row.
    Mirrors functions/upload_processor/main.py:get_ci.
    """
    lower_map = {}
    for k in row.keys():
        if isinstance(k, str):
            lower_map[k.lower()] = k
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            return row.get(key)
    return None


def make_business_id(business_name: Optional[str], plz: Optional[str]) -> str:
    """
    Re-implementation of make_business_id from upload_processor.
    """
    def sanitize_id(value: str) -> str:
        import re

        if value is None:
            return ""
        v = str(value).strip()
        v = re.sub(r"[^A-Za-z0-9äöüÄÖÜß]+", "-", v)
        v = re.sub(r"-{2,}", "-", v).strip("-")
        return v.lower()

    base = sanitize_id(business_name or "")
    if plz:
        base = f"{base}-{sanitize_id(plz)}" if base else sanitize_id(plz)
    return base


def dedupe_key_for_row(row: dict) -> str:
    """
    Re-implementation of dedupe_key_for_row from upload_processor.
    """
    import re

    name = (get_ci(row, "Namenszeile") or get_ci(row, "business_name", "company") or "").lower().strip()
    street = (
        get_ci(row, "Straße", "Strasse", "Str", "Str.") or ""
    ).lower().strip().replace("ß", "ss")
    house = (get_ci(row, "Hausnummer", "HNr", "Hnr", "Nr") or "").lower().strip()
    plz = (get_ci(row, "PLZ", "Postleitzahl") or "").lower().strip()
    city = (get_ci(row, "Ort", "Stadt", "City") or "").lower().strip()

    def slug(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", s)

    return f"{slug(name)}|{slug(street)}-{slug(house)}|{plz}|{slug(city)}"


def compose_full_address(
    street: Optional[str],
    house_no: Optional[str],
    plz: Optional[str],
    city: Optional[str],
    country: str = "Germany",
) -> str:
    """Build full address string; mirrors upload_processor.compose_full_address."""
    parts: List[str] = []
    if street:
        parts.append(street.strip())
    if house_no:
        if parts:
            parts[-1] = f"{parts[-1]} {house_no.strip()}"
        else:
            parts.append(house_no.strip())
    line2 = " ".join(p for p in [plz, city] if p)
    if line2:
        parts.append(line2.strip())
    if country:
        parts.append(country)
    return ", ".join(parts)


def build_business_payloads_from_row(
    row: dict, run_timestamp: Optional[datetime] = None
) -> Tuple[str, Dict, Dict]:
    """
    Build canonical business and customer overlay payloads from a CSV row.
    Mirrors upload_processor.upsert_business_payload_from_row (no geocoding).
    If run_timestamp is set, use it for updated_at; else gcf.SERVER_TIMESTAMP.
    Returns: (biz_id, canonical_payload, customer_payload)
    """
    business_name = get_ci(row, "Namenszeile") or get_ci(row, "business_name", "company")
    street = get_ci(row, "Straße", "Strasse", "Str", "Str.")
    house_no = get_ci(row, "Hausnummer", "HNr", "Hnr", "Nr")
    plz = get_ci(row, "PLZ", "Postleitzahl")
    city = get_ci(row, "Ort", "Stadt", "City")
    fname = get_ci(row, "Entscheider 1 Vorname", "Vorname", "Anrede Vorname")
    lname = get_ci(row, "Entscheider 1 Nachname", "Nachname")
    prefix_tel = get_ci(row, "Vorwahl Telefon", "Vorwahl", "Telefon Vorwahl")
    tel = get_ci(row, "Telefonnummer", "Telefon", "Phone")
    email = get_ci(row, "E-Mail-Adresse", "Email", "E-Mail", "Mail")
    salutation = get_ci(row, "Entscheider 1 Anrede", "Salutation")

    contact_name = " ".join(p for p in [fname, lname] if p)
    phone = " ".join(p for p in [prefix_tel, tel] if p)
    full_addr = compose_full_address(street, house_no, plz, city, "Germany")

    biz_id = make_business_id(business_name, plz)

    canonical_payload = {
        "business_name": business_name,
        "street": street,
        "house_number": house_no,
        "postcode": plz,
        "city": city,
        "address": full_addr or None,
        "business_id": biz_id,
    }

    customer_payload = {
        "phone": phone or None,
        "email": email or None,
        "name": contact_name or None,
        "salutation": salutation or None,
        "hit_count": 0,
        "last_hit_at": None,
        "updated_at": run_timestamp if run_timestamp is not None else gcf.SERVER_TIMESTAMP,
    }

    return biz_id, canonical_payload, customer_payload


def snapshot_mailing_from_row(row: dict, fallback_business_name: Optional[str]) -> Dict:
    """
    Re-implementation of snapshot_mailing_from_row from upload_processor.
    """
    def compose_full_address(
        street: Optional[str],
        house_no: Optional[str],
        plz: Optional[str],
        city: Optional[str],
        country: str = "Germany",
    ) -> str:
        parts: List[str] = []
        if street:
            parts.append(street.strip())
        if house_no:
            if parts:
                parts[-1] = f"{parts[-1]} {house_no.strip()}"
            else:
                parts.append(house_no.strip())
        line2 = " ".join(p for p in [plz, city] if p)
        if line2:
            parts.append(line2.strip())
        if country:
            parts.append(country)
        return ", ".join(parts)

    street = get_ci(row, "Straße", "Strasse", "Str", "Str.")
    house_no = get_ci(row, "Hausnummer", "HNr", "Hnr", "Nr")
    plz = get_ci(row, "PLZ", "Postleitzahl")
    city = get_ci(row, "Ort", "Stadt", "City")
    country = get_ci(row, "Country", "Land") or "DE"

    address_lines: List[str] = []
    if street or house_no:
        line1 = " ".join([p for p in [street, house_no] if p])
        if line1:
            address_lines.append(line1)

    mailing = {
        "business_name": get_ci(row, "Namenszeile")
        or get_ci(row, "business_name", "company")
        or fallback_business_name,
        "recipient_name": None,
        "address_lines": address_lines,
        "postcode": plz or None,
        "city": city or None,
        "country": country,
    }
    return mailing


def template_with_qr_suffix(template: Optional[str]) -> Optional[str]:
    """
    Uses the *current* upload_processor behavior:
    - If empty/whitespace -> None
    - If already ends with .pdf -> return as-is
    - Else append .pdf
    """
    import os

    if not template:
        return None
    s = str(template).strip()
    if not s:
        return None
    if s.lower().endswith(".pdf"):
        return s
    # Older behavior used *_qr_track.pdf but current function is simpler,
    # and repair should match the state after your recent changes.
    base, _ext = os.path.splitext(s)
    return f"{base}.pdf"


def load_csv_rows_by_tracking_id(csv_path: str) -> Tuple[Dict[str, dict], int]:
    rows_by_tid: Dict[str, dict] = {}
    total_rows = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            tid = (row.get("tracking_id") or "").strip()
            if not tid:
                continue
            rows_by_tid[tid] = row
    return rows_by_tid, total_rows


def find_existing_links_for_campaign(db: gcf.Client, campaign_id: str) -> Tuple[gcf.DocumentReference, Dict[str, dict]]:
    campaign_ref = db.collection("campaigns").document(campaign_id)
    existing: Dict[str, dict] = {}
    for doc in db.collection("links").where("campaign_ref", "==", campaign_ref).stream():
        existing[doc.id] = doc.to_dict() or {}
    return campaign_ref, existing


def infer_owner_and_defaults(
    existing_links: Dict[str, dict],
    campaign_ref: gcf.DocumentReference,
    db: gcf.Client,
    explicit_owner_id: Optional[str],
) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Infer owner_id, destination, and campaign_name from any existing link or the campaign doc.
    """
    sample_link_data: Optional[dict] = None
    if existing_links:
        # Just pick one arbitrary sample
        sample_link_data = next(iter(existing_links.values()))

    owner_id = explicit_owner_id
    if not owner_id:
        if sample_link_data and sample_link_data.get("owner_id"):
            owner_id = sample_link_data["owner_id"]
        else:
            raise RuntimeError("owner_id is not provided and could not be inferred from existing links.")

    default_dest = None
    if sample_link_data:
        default_dest = sample_link_data.get("destination")

    campaign_name = None
    if sample_link_data and sample_link_data.get("campaign_name"):
        campaign_name = sample_link_data["campaign_name"]
    else:
        snap = campaign_ref.get()
        data = snap.to_dict() or {}
        campaign_name = data.get("campaign_name")

    return owner_id, default_dest, campaign_name


def build_link_and_target_payloads(
    tid: str,
    row: dict,
    campaign_ref: gcf.DocumentReference,
    owner_id: str,
    default_dest: Optional[str],
    campaign_name: Optional[str],
    db: gcf.Client,
    run_timestamp: Optional[datetime] = None,
) -> Tuple[Dict, Dict, str, gcf.DocumentReference, gcf.DocumentReference]:
    """
    Build link + target payloads for a single missing tracking_id.
    Returns (link_payload, target_payload, business_id, biz_ref, target_ref).
    """
    # Destination logic: prefer per-row destination/url columns, fallback to inferred default.
    dest = (get_ci(row, "destination", "url") or "").strip()
    if not dest:
        dest = (default_dest or "").strip()
    if not dest:
        raise RuntimeError(f"No destination found for tracking_id={tid}")

    business_name = get_ci(row, "Namenszeile") or get_ci(row, "business_name", "company")
    plz = get_ci(row, "PLZ", "Postleitzahl")
    biz_id = make_business_id(business_name, plz)

    biz_ref = db.collection("businesses").document(biz_id)

    # Template handling
    template_raw = get_ci(row, "Template", "template")
    template_id = template_with_qr_suffix(template_raw)

    snapshot = snapshot_mailing_from_row(row, business_name)

    # References
    link_ref = db.collection("links").document(tid)
    target_ref = campaign_ref.collection("targets").document()

    ts = run_timestamp if run_timestamp is not None else gcf.SERVER_TIMESTAMP
    link_payload = {
        "campaign_ref": campaign_ref,
        "business_ref": biz_ref,
        "target_ref": target_ref,
        "destination": dest,
        "template_id": template_id,
        "active": True,
        "hit_count": 0,
        "created_at": ts,
        "last_hit_at": None,
        "owner_id": owner_id,
        "snapshot_mailing": snapshot,
        "campaign_name": campaign_name,
        "short_code": tid,
    }

    target_payload = {
        "business_ref": biz_ref,
        "status": "linked",
        "reason_excluded": None,
        "link_ref": link_ref,
        "import_row": row,
        "dedupe_key": dedupe_key_for_row(row),
        "created_at": ts,
        "updated_at": ts,
    }

    return link_payload, target_payload, biz_id, biz_ref, target_ref


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-off script to repair missing links/targets for a campaign based on a _with_links CSV."
    )
    parser.add_argument(
        "--service-account",
        required=False,
        help=(
            "Optional explicit path to Firebase service account JSON. "
            "If omitted, derived from --env (dev/prod)."
        ),
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help=(
            "Environment to target (controls default service account). "
            "Default: dev."
        ),
    )
    parser.add_argument(
        "--database",
        metavar="ID",
        help=(
            "Firestore database ID (e.g. '(default)' or 'test'). "
            "If omitted: dev -> 'test', prod -> '(default)'. "
            "Use --database '(default)' to hit default DB in dev."
        ),
    )
    parser.add_argument(
        "--campaign-id",
        required=True,
        help="Campaign document ID (campaigns/{campaignId}).",
    )
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to the *_with_links.csv file used to generate links.",
    )
    parser.add_argument(
        "--owner-id",
        help="Owner ID (customer UID). If omitted, inferred from existing links for the campaign.",
    )
    parser.add_argument(
        "--destination",
        help="Default destination URL for links when not in CSV or existing links.",
    )
    parser.add_argument(
        "--timestamp",
        metavar="ISO8601",
        help="ISO timestamp for created_at/updated_at (e.g. 2026-02-18T12:00:00Z). Default: now (UTC).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit on number of missing links to repair (0 = no limit).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, only print what would be done without writing to Firestore.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-link diagnostics.",
    )

    args = parser.parse_args()

    # Resolve service account path based on env + optional override
    if args.service_account:
        sa_path = args.service_account
    else:
        sa_path = DEV_SERVICE_ACCOUNT if args.env == "dev" else PROD_SERVICE_ACCOUNT

    print(f"Environment:     {args.env}")
    print(f"Service account: {sa_path}")

    # Choose Firestore database ID: CLI override, else dev->test, prod->(default)
    if args.database is not None and (args.database or "").strip():
        database_id = args.database.strip()
    else:
        database_id = "test" if args.env == "dev" else "(default)"
    print(f"Firestore database: {database_id}")

    # Initialize Firestore client directly with service account credentials
    creds = gsa.Credentials.from_service_account_file(sa_path)
    project_id = creds.project_id
    if not project_id:
        raise RuntimeError("Could not determine project_id from service account.")

    db = gcf.Client(project=project_id, credentials=creds, database=database_id)

    print(f"Using campaign: {args.campaign_id}")
    print(f"CSV path:       {args.csv_path}")
    print(f"Dry-run:        {args.dry_run}")
    print()

    # Load CSV
    rows_by_tid, total_rows = load_csv_rows_by_tracking_id(args.csv_path)
    expected_ids = set(rows_by_tid.keys())
    print(f"Total CSV rows:             {total_rows}")
    print(f"Distinct tracking_ids in CSV: {len(expected_ids)}")

    # Load existing links for campaign
    campaign_ref, existing_links = find_existing_links_for_campaign(db, args.campaign_id)
    existing_ids = set(existing_links.keys())
    print(f"Existing links for campaign: {len(existing_ids)}")

    # Compute missing
    missing_ids = sorted(expected_ids - existing_ids)
    print(f"Missing link docs:           {len(missing_ids)}")
    if args.limit and len(missing_ids) > args.limit:
        print(f"Limiting repair to first {args.limit} missing IDs.")
        missing_ids = missing_ids[: args.limit]

    if not missing_ids:
        print("Nothing to repair. Exiting.")
        return 0

    if args.timestamp:
        try:
            run_timestamp = datetime.fromisoformat(
                args.timestamp.replace("Z", "+00:00")
            )
            if run_timestamp.tzinfo is None:
                run_timestamp = run_timestamp.replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise RuntimeError(
                f"Invalid --timestamp (use ISO format, e.g. 2026-02-18T12:00:00Z): {e}"
            )
    else:
        run_timestamp = datetime.now(timezone.utc)
    print(f"Run timestamp (for created_at/updated_at): {run_timestamp.isoformat()}")

    # Infer owner_id, destination, campaign_name from existing data
    owner_id, default_dest, campaign_name = infer_owner_and_defaults(
        existing_links, campaign_ref, db, args.owner_id
    )
    # Use CLI --destination when no default from existing links
    if not (default_dest and str(default_dest).strip()) and args.destination:
        default_dest = (args.destination or "").strip() or None
    print(f"Inferred owner_id:    {owner_id}")
    print(f"Default destination:  {default_dest!r}")
    print(f"Campaign name:        {campaign_name!r}")
    print()

    # Coherence checks and creation counters
    anomalies = {
        "missing_in_csv": 0,
        "business_missing": 0,
        "customer_business_missing": 0,
        "link_already_exists": 0,
        "target_already_exists": 0,
        "created_businesses": 0,
        "created_customer_overlays": 0,
    }

    batch = db.batch()
    ops = 0
    repaired = 0

    def flush():
        nonlocal batch, ops
        if not ops or args.dry_run:
            return
        batch.commit()
        batch = db.batch()
        ops = 0

    print("Starting repair of missing links/targets...")

    for idx, tid in enumerate(missing_ids, start=1):
        row = rows_by_tid.get(tid)
        if not row:
            anomalies["missing_in_csv"] += 1
            print(f"[WARN] tracking_id={tid} not found in CSV rows; skipping.")
            continue

        # Sanity: skip if link somehow exists now (race / manual fix)
        link_ref = db.collection("links").document(tid)
        if link_ref.get().exists:
            anomalies["link_already_exists"] += 1
            if args.verbose:
                print(f"[SKIP] Link {tid} already exists.")
            continue

        # Check if a target already exists referencing this link (unlikely but safe)
        existing_targets = list(
            campaign_ref.collection("targets")
            .where("link_ref", "==", link_ref)
            .limit(2)
            .stream()
        )
        if existing_targets:
            anomalies["target_already_exists"] += 1
            if args.verbose:
                print(f"[WARN] Found existing target(s) for link {tid}; skipping to avoid duplicates.")
            continue

        try:
            link_payload, target_payload, biz_id, biz_ref, target_ref = build_link_and_target_payloads(
                tid=tid,
                row=row,
                campaign_ref=campaign_ref,
                owner_id=owner_id,
                default_dest=default_dest,
                campaign_name=campaign_name,
                db=db,
                run_timestamp=run_timestamp,
            )
        except Exception as e:
            print(f"[ERROR] Failed to build payloads for {tid}: {e}")
            continue

        # Build business + customer payloads for creating missing docs
        _, canonical_payload, customer_payload = build_business_payloads_from_row(
            row, run_timestamp=run_timestamp
        )

        biz_snap = biz_ref.get()
        customer_business_ref = (
            db.collection("customers")
            .document(owner_id)
            .collection("businesses")
            .document(biz_id)
        )
        overlay_snap = customer_business_ref.get()

        if not biz_snap.exists:
            anomalies["business_missing"] += 1
            if not args.dry_run:
                batch.set(
                    biz_ref,
                    {**canonical_payload, "created_at": run_timestamp},
                    merge=True,
                )
                ops += 1
                anomalies["created_businesses"] += 1
            elif args.verbose:
                print(f"[WOULD CREATE] Business {biz_ref.path} for tracking_id={tid}.")

        if not overlay_snap.exists:
            anomalies["customer_business_missing"] += 1
            if not args.dry_run:
                # Ensure business has this owner (whether we just created it or it existed)
                batch.set(biz_ref, {"ownerIds": ArrayUnion([owner_id])}, merge=True)
                ops += 1
                batch.set(
                    customer_business_ref,
                    {"business_ref": biz_ref, **customer_payload},
                    merge=True,
                )
                ops += 1
                anomalies["created_customer_overlays"] += 1
            elif args.verbose:
                print(
                    f"[WOULD CREATE] Customer overlay {customer_business_ref.path} for tracking_id={tid}."
                )

        if args.verbose:
            print(f"[REPAIR] Creating link {tid} and a new target for business_id={biz_id}")

        if not args.dry_run:
            batch.set(link_ref, link_payload)
            ops += 1
            batch.set(target_ref, target_payload)
            ops += 1
            repaired += 1

            if ops >= 400:
                flush()

    flush()

    # ---------- Detailed summary ----------
    print()
    print("=" * 60)
    print("REPAIR SUMMARY")
    print("=" * 60)
    print(f"  Campaign ID:           {args.campaign_id}")
    print(f"  Dry-run:               {args.dry_run}")
    print(f"  Missing links (input): {len(missing_ids)}")
    print()
    print("Documents created (this run):")
    print(f"  businesses                     {anomalies['created_businesses']}")
    print(f"  customers/.../businesses       {anomalies['created_customer_overlays']} (customer overlays)")
    print(f"  links                          {repaired}")
    print(f"  campaigns/.../targets          {repaired}")
    print()
    print("Anomalies / skipped:")
    print(f"  missing_in_csv                 {anomalies['missing_in_csv']} (tracking_id in diff but not in CSV)")
    print(f"  business_missing               {anomalies['business_missing']} (were missing, created if not dry-run)")
    print(f"  customer_business_missing      {anomalies['customer_business_missing']} (were missing, created if not dry-run)")
    print(f"  link_already_exists            {anomalies['link_already_exists']} (skipped, no duplicate link)")
    print(f"  target_already_exists          {anomalies['target_already_exists']} (skipped, target for link already present)")
    print()
    if not args.dry_run and (repaired > 0 or anomalies["created_businesses"] or anomalies["created_customer_overlays"]):
        print("Total document writes this run:")
        total_writes = (
            anomalies["created_businesses"]
            + anomalies["created_customer_overlays"] * 2  # overlay doc + ownerIds on business
            + repaired * 2  # link + target per repaired row
        )
        print(f"  {total_writes} (businesses + overlays + ownerIds + links + targets)")
    print("=" * 60)

    # Final coherence snapshot (counts in Firestore after run)
    final_links = sum(
        1
        for _ in db.collection("links")
        .where("campaign_ref", "==", campaign_ref)
        .stream()
    )
    final_targets = sum(1 for _ in campaign_ref.collection("targets").stream())

    print()
    print("Final coherence (counts in Firestore for this campaign):")
    print(f"  links for campaign:   {final_links}")
    print(f"  targets for campaign: {final_targets}")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())

