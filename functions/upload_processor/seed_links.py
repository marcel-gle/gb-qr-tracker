# seed_links.py
# - Upserts businesses/{bizId} from your XLSX/CSV
# - Creates links/{linkId} with a `business` DocumentReference to businesses/{bizId}
# - Adjusts per-row Template to "<base>_qr_track.pdf" when adding tracking_link, uploads that
# - Writes back tracking_link (and adjusted Template) into the output file
# - Optional Mapbox geocoding for businesses
#
# deps: pip install pandas openpyxl requests

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
links = db.collection('links')
businesses = db.collection('businesses')


# ---------------------------
# Utilities
# ---------------------------
def build_tracking_link(base_url: str, doc_id: str) -> str:
    return f"{base_url.rstrip('/')}/{doc_id}"


def sanitize_id(value: str) -> str:
    if value is None:
        return ""
    v = str(value).strip()
    v = re.sub(r"[^A-Za-z0-9]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    return v


def template_with_qr_suffix(template: Optional[str]) -> Optional[str]:
    if not template:
        return None
    base, _ext = os.path.splitext(str(template))
    if base.endswith('_qr_track'):
        return f"{base}.pdf"
    return f"{base}_qr_track.pdf"


def get_ci(row: dict, *names: str) -> Optional[str]:
    lower_map = {k.lower(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            return row.get(key)
    return None


def get_ci_key(row: dict, *names: str) -> Optional[str]:
    lower_map = {k.lower(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            return key
    return None


def csv_fieldnames_union(rows: List[dict]) -> List[str]:
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
# Firestore writes
# ---------------------------
def create_or_merge_link(doc_id: str, destination: str,
                         #customer: str,
                         owner_id: str,
                         active: bool = True,
                         business_name: Optional[str] = None,
                         campaign: Optional[str] = None,
                         template: Optional[str] = None,
                         business_ref: Optional[firestore.DocumentReference] = None,
                         business_id: Optional[str] = None):
    """
    Create links/{doc_id}. If it already exists, merge business/template back so association isn't lost.
    """
    payload = {
        'destination': destination,
        'active': bool(active),
        'hit_count': 0,
        'created_at': firestore.SERVER_TIMESTAMP,
        'last_hit_at': None,
        'owner_id': owner_id,
    }
    if owner_id == '' or owner_id is None:
        print("Error no owner_id provided")
        return

    #if customer:
    #    payload['customer'] = str(customer)
    if business_name:
        payload['business_name'] = business_name
    if campaign:
        payload['campaign'] = campaign
    if template:
        payload['template'] = template
    if business_ref:
        payload['business'] = business_ref
    if business_id:
        payload['business_id'] = business_id

    ref = links.document(doc_id)
    try:
        ref.create(payload)
    except AlreadyExists:
        # Merge in case we need to attach/refresh the business ref or template on an existing link
        merge_fields = {}
        if business_ref:
            merge_fields['business'] = business_ref
        if template:
            merge_fields['template'] = template
        if campaign:
            merge_fields['campaign'] = campaign
        if business_name:
            merge_fields['business_name'] = business_name
        if business_id:
            merge_fields['business_id'] = business_id
        if merge_fields:
            ref.set(merge_fields, merge=True)
        raise  # let caller handle counting/logging


def make_business_id(business_name: Optional[str], plz: Optional[str]) -> str:
    base = sanitize_id(business_name or "")
    if plz:
        base = f"{base}-{sanitize_id(plz)}" if base else sanitize_id(plz)
    return base or "biz"


def compose_full_address(street: Optional[str], house_no: Optional[str],
                         plz: Optional[str], city: Optional[str], country: str = "Germany") -> str:
    parts = []
    if street:
        parts.append(street.strip())
    if house_no:
        parts[-1] = f"{parts[-1]} {house_no.strip()}" if parts else house_no.strip()
    line2 = " ".join(p for p in [plz, city] if p)
    if line2:
        parts.append(line2.strip())
    if country:
        parts.append(country)
    return ", ".join(parts)


def geocode_mapbox(address: str, token: str, country_hint: Optional[str] = "DE") -> Optional[Dict[str, float]]:
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


def upsert_business_from_row(row: dict, ownerId: str, mapbox_token: Optional[str]) -> firestore.DocumentReference:
    """
    Create or update businesses/{bizId}. Returns the DocumentReference.
    No letters array; links are discoverable by querying links where business == bizRef.
    """
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
    biz_ref = businesses.document(biz_id)

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
        "hit_count": 0,  # aggregate hit count for this business,
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


# ---------------------------
# Business-file mode helpers & flow
# ---------------------------
def derive_fields_for_business_row(row: dict, dest_default: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (doc_id_from_row, destination, business_name, template_raw, template_col_key)
    """
    doc_id_from_row = get_ci(row, 'id', 'link_id')
    dest = get_ci(row, 'destination', 'url') or dest_default
    business_name = get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company')
    template_key = get_ci_key(row, 'Template', 'template')
    template_raw = row.get(template_key) if template_key else None
    return doc_id_from_row, dest, business_name, template_raw, template_key


def write_back_csv(input_path: str, rows: list, suffix="_with_links") -> str:
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
    base, _ = os.path.splitext(input_path)
    out_path = f"{base}{suffix}.xlsx"
    cols = list(df.columns)
    if 'tracking_link' in cols:
        cols = [c for c in cols if c != 'tracking_link'] + ['tracking_link']
        df = df[cols]
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return out_path


def assign_links_from_business_file(path: str, base_url: str,
                                    dest_default: Optional[str], campaign: Optional[str],
                                    prefix: Optional[str],
                                    ownerId: str,
                                    limit: int,
                                    mapbox_token: Optional[str]):
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

    next_counter = 1
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
            biz_id = biz_ref.id

            # Link doc id resolution
            doc_id = doc_id_from_row
            if not doc_id and prefix:
                doc_id = f"{prefix}{next_counter}"
                next_counter += 1
            if not doc_id:
                base_slug = sanitize_id(business_name or "")
                if not base_slug:
                    base_slug = "ID"
                doc_id = f"{base_slug}-{i+1}"

            if not dest:
                print(f"[skip] No destination (column or --dest) for row {i+1}: {row}")
                skipped += 1
                row['tracking_link'] = ''
                continue

            adjusted_template = template_with_qr_suffix(template_raw)

            try:
                create_or_merge_link(
                    doc_id, dest,
                    ownerId, True, business_name, campaign, adjusted_template,
                    business_ref=biz_ref,
                    business_id=biz_id
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
    print(f"Wrote updated file with 'tracking_link': {out_path}")


# ---------------------------
# CLI
# ---------------------------
if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Seed Firestore links and businesses; update file with tracking links.')

    p.add_argument('--business-file', help='CSV or XLSX of businesses to assign tracking links to')
    p.add_argument('--dest', help='Destination URL used for all generated IDs (or as default for business-file mode)')
    p.add_argument('--campaign', help='Campaign label to attach to generated links')
    p.add_argument('--base-url', help='Base URL for tracking links, e.g. https://qr.example.com')
    p.add_argument('--ownerId', help='Customer identifier to attach to ALL rows in the business file')
    p.add_argument('--limit', type=int, default=0, help='Only process/upload the first X rows from the file (0 = all)')
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
            campaign=args.campaign,
            ownerId=args.ownerId,
            limit=args.limit,
            mapbox_token=args.mapbox_token,
        )
    else:
        p.error('Provide: --business-file + --base-url + --ownerId (optional: --dest, --campaign, --prefix, --limit).')
