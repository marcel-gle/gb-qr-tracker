""" 
Seed links script for expanded schema
"""

"""
seed_links.py (targets-first schema)

- Creates or reuses a campaign (campaigns/{campaignId})
- Upserts businesses/{bizId} from your XLSX/CSV (keeps ownerIds array)
- Creates campaigns/{campaignId}/targets/{targetId} for each imported row
- Creates links/{linkId} with campaign_ref, business_ref, target_ref + snapshot_mailing
- Writes back tracking_link (and adjusted Template) into the output file
- Optional Mapbox geocoding for businesses

deps: pip install pandas openpyxl requests google-cloud-firestore
"""

import argparse
import csv
import os
import re
from typing import Optional, Tuple, List, Set, Dict
from google.cloud.firestore_v1 import ArrayUnion

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import requests
except Exception:
    requests = None

from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists

db = firestore.Client()
COL_LINKS = db.collection('links')
COL_BUSINESSES = db.collection('businesses')
COL_CAMPAIGNS = db.collection('campaigns')


# ---------------------------
# Utilities
# ---------------------------
def build_tracking_link(base_url: str, doc_id: str) -> str:
    """Return the public tracking URL by joining base_url and the link document id."""
    return f"{base_url.rstrip('/')}/{doc_id}"


def sanitize_id(value: str) -> str:
    """Normalize a string to a URL-safe id slug: alphanumerics joined by single dashes."""
    if value is None:
        return ""
    v = str(value).strip()
    v = re.sub(r"[^A-Za-z0-9]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    return v


def template_with_qr_suffix(template: Optional[str]) -> Optional[str]:
    """Ensure a template filename ends with '_qr_track.pdf' (idempotent)."""
    if not template:
        return None
    base, _ext = os.path.splitext(str(template))
    if base.endswith('_qr_track'):
        return f"{base}.pdf"
    return f"{base}_qr_track.pdf"


def get_ci(row: dict, *names: str) -> Optional[str]:
    """Case-insensitive getter for the first matching column name in a row."""
    lower_map = {k.lower(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            return row.get(key)
    return None


def get_ci_key(row: dict, *names: str) -> Optional[str]:
    """Case-insensitive lookup that returns the exact column key name from the row if present."""
    lower_map = {k.lower(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            return key
    return None


def csv_fieldnames_union(rows: List[dict]) -> List[str]:
    """Compute a stable union of CSV column names, ensuring 'tracking_link' is last."""
    if not rows:
        return ['tracking_link']
    seen: Set[str] = set()
    ordered: List[str] = []
    for k in rows[0].keys():
        if k not in seen:
            ordered.append(k); seen.add(k)
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen and k != 'tracking_link':
                ordered.append(k); seen.add(k)
    if 'tracking_link' in seen:
        ordered = [k for k in ordered if k != 'tracking_link']
    ordered.append('tracking_link')
    return ordered


# ---------------------------
# Schema helpers
# ---------------------------
def compose_full_address(street: Optional[str], house_no: Optional[str],
                         plz: Optional[str], city: Optional[str], country: str = "Germany") -> str:
    """Compose a human-readable address string from individual columns."""
    parts = []
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


def snapshot_mailing_from_row(row: dict, fallback_business_name: Optional[str]) -> Dict:
    """Build the immutable mailing snapshot (copied onto links) from the CSV row."""
    street = get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.')
    house_no = get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr')
    plz = get_ci(row, 'PLZ', 'Postleitzahl')
    city = get_ci(row, 'Ort', 'Stadt', 'City')
    country = get_ci(row, 'Country', 'Land') or "DE"
    address_lines = []
    if street or house_no:
        line1 = " ".join([p for p in [street, house_no] if p])
        if line1:
            address_lines.append(line1)
    mailing = {
        "business_name": get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company') or fallback_business_name,
        "recipient_name": None,
        "address_lines": address_lines,
        "postcode": plz or None,
        "city": city or None,
        "country": country
    }
    return mailing


def make_business_id(business_name: Optional[str], plz: Optional[str]) -> str:
    """Create a stable business document id from business name and postcode."""
    base = sanitize_id(business_name or "")
    if plz:
        base = f"{base}-{sanitize_id(plz)}" if base else sanitize_id(plz)
    return base or "biz"


def dedupe_key_for_row(row: dict) -> str:
    """Return a normalized dedupe key built from name + address parts (lowercased, dashed)."""
    name = (get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company') or '').lower().strip()
    street = (get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.') or '').lower().strip().replace('ß', 'ss')
    house = (get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr') or '').lower().strip()
    plz = (get_ci(row, 'PLZ', 'Postleitzahl') or '').lower().strip()
    city = (get_ci(row, 'Ort', 'Stadt', 'City') or '').lower().strip()
    return f"{re.sub(r'[^a-z0-9]+','-',name)}|{re.sub(r'[^a-z0-9]+','-',street)}-{re.sub(r'[^a-z0-9]+','-',house)}|{plz}|{re.sub(r'[^a-z0-9]+','-',city)}"


def geocode_mapbox(address: str, token: str, country_hint: Optional[str] = "DE") -> Optional[Dict[str, float]]:
    """Look up lat/lon for an address using Mapbox (returns {'lat','lon','source'} or None)."""
    if not token:
        return None
    if requests is None:
        print("[warn] requests not installed; skipping geocoding")
        return None
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json"
    params = {"access_token": token, "limit": 1}
    if country_hint:
        params["country"] = country_hint
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        feats = data.get("features") or []
        if not feats:
            return None
        center = feats[0].get("center")
        if not center or len(center) != 2:
            return None
        return {"lon": float(center[0]), "lat": float(center[1]), "source": "mapbox"}
    except Exception as e:
        print(f"[warn] Geocoding failed for '{address}': {e}")
        return None


# ---------------------------
# Firestore writes
# ---------------------------
def get_or_create_campaign(owner_id: str, code: Optional[str], name: Optional[str]) -> firestore.DocumentReference:
    """Fetch a campaign by code if provided, otherwise create a new campaign and return its reference."""
    if code:
        q = COL_CAMPAIGNS.where('code', '==', code).limit(1).stream()
        for doc in q:
            print(f"[campaign] Reusing existing campaign '{code}' → {doc.id}")
            return doc.reference

    payload = {
        "name": name or (code or "Untitled Campaign"),
        "code": code or None,
        "owner_id": owner_id,
        "status": "draft",
        "totals": { "targets": 0, "links": 0, "hits": 0, "unique_ips": 0 },
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP
    }
    ref = COL_CAMPAIGNS.document()
    ref.set(payload)
    print(f"[campaign] Created campaign → {ref.id} (code={code})")
    return ref


def upsert_business_from_row(row: dict, ownerId: str, mapbox_token: Optional[str]) -> firestore.DocumentReference:
    """Create or update a business from a CSV row; ensure ownerId is present in ownerIds; return doc ref."""
    business_name = get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company')
    street = get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.')
    house_no = get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr')
    plz = get_ci(row, 'PLZ', 'Postleitzahl')
    city = get_ci(row, 'Ort', 'Stadt', 'City')
    fname = get_ci(row, 'Entscheider 1 Vorname', 'Vorname', 'Anrede Vorname')
    lname = get_ci(row, 'Entscheider 1 Nachname', 'Nachname')
    prefix_tel = get_ci(row, 'Vorwahl Telefon', 'Vorwahl', 'Telefon Vorwahl')
    tel = get_ci(row, 'Telefonnummer', 'Telefon', 'Phone')
    email = get_ci(row, 'E-Mail-Adresse', 'Email', 'E-Mail', 'Mail')
    salutation = get_ci(row, 'Entscheider 1 Anrede', 'Salutation')

    contact_name = " ".join(p for p in [fname, lname] if p)
    phone = " ".join(p for p in [prefix_tel, tel] if p)
    full_addr = compose_full_address(street, house_no, plz, city, "Germany")
    coordinate = geocode_mapbox(full_addr, mapbox_token) if mapbox_token else None

    biz_id = make_business_id(business_name, plz)
    biz_ref = COL_BUSINESSES.document(biz_id)

    payload = {
        "business_name": business_name,
        "street": street,
        "house_number": house_no,
        "postcode": plz,
        "city": city,
        "name": contact_name or None,
        "phone": phone or None,
        "email": email or None,
        "address": full_addr or None,
        "salutation": salutation or None,
        "updated_at": firestore.SERVER_TIMESTAMP,
        "hit_count": 0,
        "business_id": biz_id,
    }
    if coordinate:
        payload["coordinate"] = coordinate

    try:
        create_payload = dict(payload)
        create_payload["created_at"] = firestore.SERVER_TIMESTAMP
        create_payload["ownerIds"] = [ownerId]
        biz_ref.create(create_payload)
    except AlreadyExists:
        biz_ref.set(payload, merge=True)
        biz_ref.set({"ownerIds": ArrayUnion([ownerId])}, merge=True)

    return biz_ref


def create_target(campaign_ref: firestore.DocumentReference,
                  biz_ref: firestore.DocumentReference,
                  row: dict,
                  status: str) -> firestore.DocumentReference:
    """Create a target (imported audience row) under a campaign and return its reference."""
    targets = campaign_ref.collection('targets')
    target_ref = targets.document()  # auto-id
    payload = {
        "business_ref": biz_ref,
        "status": status,
        "reason_excluded": None,
        "link_ref": None,
        "import_row": row,
        "dedupe_key": dedupe_key_for_row(row),
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP
    }
    target_ref.set(payload)
    COL_CAMPAIGNS.document(campaign_ref.id).set(
        {"totals.targets": firestore.Increment(1), "updated_at": firestore.SERVER_TIMESTAMP},
        merge=True
    )
    return target_ref


def create_or_merge_link_new(link_id: str,
                             dest: str,
                             owner_id: str,
                             business_ref: firestore.DocumentReference,
                             campaign_ref: firestore.DocumentReference,
                             target_ref: firestore.DocumentReference,
                             business_name_for_snapshot: Optional[str],
                             template_raw: Optional[str],
                             active: bool = True):
    """Create a link for a target (or merge if exists); update target.status/link_ref and campaign totals."""
    # Read target row for snapshot (one read per link; acceptable for imports)
    target_doc = target_ref.get()
    target_row = target_doc.to_dict().get('import_row', {}) if target_doc.exists else {}

    payload = {
        "campaign_ref": campaign_ref,
        "business_ref": business_ref,
        "target_ref": target_ref,
        "destination": dest,
        "template_id": template_with_qr_suffix(template_raw),
        "short_code": link_id,
        "active": bool(active),
        "hit_count": 0,
        "created_at": firestore.SERVER_TIMESTAMP,
        "last_hit_at": None,
        "owner_id": owner_id,
        "snapshot_mailing": snapshot_mailing_from_row(target_row, business_name_for_snapshot)
    }

    ref = COL_LINKS.document(link_id)
    try:
        ref.create(payload)
    except AlreadyExists:
        merge_fields = {
            "campaign_ref": campaign_ref,
            "business_ref": business_ref,
            "target_ref": target_ref,
            "destination": dest,
            "template_id": payload["template_id"],
            "owner_id": owner_id
        }
        ref.set(merge_fields, merge=True)
        raise

    target_ref.set({"link_ref": ref, "status": "linked", "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
    COL_CAMPAIGNS.document(campaign_ref.id).set(
        {"totals.links": firestore.Increment(1), "updated_at": firestore.SERVER_TIMESTAMP},
        merge=True
    )
    return ref


# ---------------------------
# File IO helpers
# ---------------------------
def derive_fields_for_business_row(row: dict, dest_default: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Extract common fields from a row: (link_id, destination, business_name, template_raw, template_col_key)."""
    doc_id_from_row = get_ci(row, 'id', 'link_id')
    dest = get_ci(row, 'destination', 'url') or dest_default
    business_name = get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company')
    template_key = get_ci_key(row, 'Template', 'template')
    template_raw = row.get(template_key) if template_key else None
    return doc_id_from_row, dest, business_name, template_raw, template_key


def write_back_csv(input_path: str, rows: list, suffix="_with_links") -> str:
    """Write updated rows to a CSV file (keeps original columns; appends 'tracking_link' last)."""
    base, _ = os.path.splitext(input_path)
    out_path = f"{base}{suffix}.csv"
    fieldnames = csv_fieldnames_union(rows)
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return out_path


def write_back_excel(input_path: str, df, suffix="_with_links") -> str:
    """Write updated rows to an Excel file; moves 'tracking_link' column to the end if present."""
    base, _ = os.path.splitext(input_path)
    out_path = f"{base}{suffix}.xlsx"
    cols = list(df.columns)
    if 'tracking_link' in cols:
        cols = [c for c in cols if c != 'tracking_link'] + ['tracking_link']
        df = df[cols]
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return out_path


# ---------------------------
# Main flow
# ---------------------------
def assign_links_from_business_file(path: str, base_url: str,
                                    dest_default: str,
                                    campaign_code: str,
                                    campaign_name: str,
                                    ownerId: str,
                                    limit: int,
                                    mapbox_token: str):
    """End-to-end import: read CSV/XLSX → upsert businesses → create campaign targets → create links → write output file."""
    created, skipped, errors = 0, 0, 0
    ext = os.path.splitext(path)[1].lower()
    is_excel = ext in ('.xlsx', '.xls')

    print("assign_links_from_business_file ownerId:", ownerId)

    if is_excel:
        if pd is None:
            raise RuntimeError("Excel input requires pandas and openpyxl. Install with: pip install pandas openpyxl")
        df = pd.read_excel(path, dtype=str, keep_default_na=False)
        df.columns = [str(c) for c in df.columns]
        rows = df.to_dict(orient='records')
    else:
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

    # Prepare / reuse campaign
    campaign_ref = get_or_create_campaign(ownerId, campaign_code, campaign_name)

    total_rows = len(rows)

    for i, row in enumerate(rows):
        in_limit = (limit <= 0) or (i < limit)
        if not in_limit:
            row['tracking_link'] = ''
            continue

        try:
            doc_id_from_row, dest, business_name, template_raw, template_key = \
                derive_fields_for_business_row(row, dest_default)

            # Business upsert first → get a stable ref
            biz_ref = upsert_business_from_row(row, ownerId, mapbox_token)

            # Create a target for the imported row
            target_status = "validated" if dest else "excluded"
            target_ref = create_target(campaign_ref, biz_ref, row, target_status)

            # Resolve Link doc id
            doc_id = doc_id_from_row or f"{(campaign_code or 'L').upper()}-{i+1:04d}"

            if not dest:
                print(f"[skip] No destination (column or --dest) for row {i+1}: {row}")
                skipped += 1
                row['tracking_link'] = ''
                continue

            try:
                create_or_merge_link_new(
                    link_id=doc_id,
                    dest=dest,
                    owner_id=ownerId,
                    business_ref=biz_ref,
                    campaign_ref=campaign_ref,
                    target_ref=target_ref,
                    business_name_for_snapshot=business_name,
                    template_raw=template_raw,
                    active=True
                )
                created += 1
            except AlreadyExists:
                print(f"[skip-duplicate] {doc_id} already exists")
                skipped += 1
            except Exception as e:
                print(f"[error] {doc_id}: {e}")
                errors += 1

            # Write back to output row
            row['tracking_link'] = build_tracking_link(base_url, doc_id)
            adjusted_template = template_with_qr_suffix(template_raw)
            if adjusted_template:
                if template_key:
                    row[template_key] = adjusted_template
                else:
                    row['Template'] = adjusted_template

        except Exception as e:
            print(f"[error] row {i+1}: {e}")
            errors += 1
            row['tracking_link'] = ''

    # Write updated file
    if is_excel:
        out_df = pd.DataFrame(rows)
        out_path = write_back_excel(path, out_df)
    else:
        out_path = write_back_csv(path, rows)

    print(f"Done. created={created} skipped={skipped} errors={errors}")
    print(f"Processed/uploaded up to limit={limit if limit>0 else 'ALL'} of {total_rows} rows.")
    print(f"Campaign ID: {campaign_ref.id}")
    print(f"Wrote updated file with 'tracking_link': {out_path}")


# ---------------------------
# CLI
# ---------------------------
if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Import CSV/XLSX → businesses + campaign targets + links; write file with tracking links.')

    p.add_argument('--business-file', help='CSV or XLSX to import')
    p.add_argument('--dest', help='Default destination URL (used if row lacks destination/url)')
    p.add_argument('--campaign-code', help='human code to reuse/create a campaign (e.g., ADM-01)')
    p.add_argument('--campaign-name', help='campaign display name (falls back to code or "Untitled Campaign")')
    p.add_argument('--base-url', help='Base URL for tracking links, e.g. https://qr.example.com')
    p.add_argument('--ownerId', help='UID of the user who owns this import/campaign')
    p.add_argument('--limit', type=int, default=0, help='Only process the first X rows (0 = all)')
    p.add_argument('--mapbox-token', default=os.environ.get("MAPBOX_TOKEN"),
                   help='Mapbox API token for geocoding (or set env MAPBOX_TOKEN).')

    args = p.parse_args()

    if args.business_file:
        if not args.base_url:
            p.error('--base-url is required when using --business-file')
        if not args.ownerId:
            p.error('--ownerId is required when using --business-file')
        assign_links_from_business_file(
            path=args.business_file,
            base_url=args.base_url,
            dest_default=args.dest,
            campaign_code=args.campaign_code,
            campaign_name=args.campaign_name,
            ownerId=args.ownerId,
            limit=args.limit,
            mapbox_token=args.mapbox_token,
        )
    else:
        p.error('Provide: --business-file + --base-url + --ownerId (optional: --dest, --campaign-code, --campaign-name, --limit).')