"""
Fast seed links script (batched, tqdm progress)

- Creates or reuses a campaign (campaigns/{campaignId})
- Upserts businesses/{bizId} (idempotent; keeps ownerIds array)
- Creates campaigns/{campaignId}/targets/{targetId} (single write; final status & link_ref)
- Creates/merges links/{linkId} with campaign_ref, business_ref, target_ref + snapshot_mailing
- Writes back tracking_link (and adjusted Template) into the output file
- Optional Mapbox geocoding for businesses with deduped lookups
- Optional pre-scan to SKIP existing link IDs using bulk get_all (fast-ish)
- Progress bar via tqdm

deps: pip install pandas openpyxl requests google-cloud-firestore tqdm
"""

import argparse
import csv
import os
import re
from typing import Optional, Tuple, List, Set, Dict, Iterable

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import requests
except Exception:
    requests = None

try:
    from tqdm import tqdm
except Exception:
    # Fallback: no-op wrapper if tqdm is missing
    def tqdm(x, **kwargs):
        return x

from google.cloud import firestore
from google.cloud.firestore_v1 import ArrayUnion
from google.api_core.exceptions import AlreadyExists

PROJECT_ID  = "gb-qr-tracker-dev"
DATABASE_ID = "(default)"  # secondary DB name

db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
COL_LINKS = db.collection('links')
COL_BUSINESSES = db.collection('businesses')
COL_CAMPAIGNS = db.collection('campaigns')

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
# Schema helpers
# ---------------------------
def compose_full_address(street: Optional[str], house_no: Optional[str],
                         plz: Optional[str], city: Optional[str], country: str = "Germany") -> str:
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
    base = sanitize_id(business_name or "")
    if plz:
        base = f"{base}-{sanitize_id(plz)}" if base else sanitize_id(plz)
    return base or "biz"

def dedupe_key_for_row(row: dict) -> str:
    name = (get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company') or '').lower().strip()
    street = (get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.') or '').lower().strip().replace('ß', 'ss')
    house = (get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr') or '').lower().strip()
    plz = (get_ci(row, 'PLZ', 'Postleitzahl') or '').lower().strip()
    city = (get_ci(row, 'Ort', 'Stadt', 'City') or '').lower().strip()
    return f"{re.sub(r'[^a-z0-9]+','-',name)}|{re.sub(r'[^a-z0-9]+','-',street)}-{re.sub(r'[^a-z0-9]+','-',house)}|{plz}|{re.sub(r'[^a-z0-9]+','-',city)}"

def geocode_mapbox(address: str, token: str, country_hint: Optional[str] = "DE") -> Optional[Dict[str, float]]:
    #print("Geocoding address:", address)
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
# Firestore helpers
# ---------------------------
def get_or_create_campaign(owner_id: str, code: Optional[str], name: Optional[str]) -> firestore.DocumentReference:
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
        "totals": {"targets": 0, "links": 0, "hits": 0, "unique_ips": 0},
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP
    }
    ref = COL_CAMPAIGNS.document()
    ref.set(payload)
    print(f"[campaign] Created campaign → {ref.id} (code={code})")
    return ref

def upsert_business_payload_from_row(row: dict, ownerId: str,
                                     coordinate: Optional[Dict[str, float]]) -> Tuple[str, Dict]:
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

    biz_id = make_business_id(business_name, plz)
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
    return biz_id, payload

# ---------------------------
# File IO helpers
# ---------------------------
def derive_fields_for_business_row(row: dict, dest_default: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
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

# ---------------------------
# Bulk utilities
# ---------------------------
def chunked(iterable: Iterable, size: int) -> Iterable[List]:
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def bulk_get_existing(doc_refs: List[firestore.DocumentReference]) -> Set[str]:
    """Bulk get docs; return set of existing doc ids."""
    existing: Set[str] = set()
    for chunk in chunked(doc_refs, 300):
        for snap in db.get_all(chunk):
            if snap.exists:
                existing.add(snap.id)
    return existing

# ---------------------------
# Main flow (batched & fast) with tqdm
# ---------------------------
def assign_links_from_business_file(path: str, base_url: str,
                                    dest_default: Optional[str],
                                    campaign_code: Optional[str],
                                    campaign_name: Optional[str],
                                    ownerId: str,
                                    limit: int,
                                    mapbox_token: Optional[str],
                                    skip_existing: bool,
                                    geocode: bool = True):
    created_links, created_targets = 0, 0
    skipped, errors = 0, 0
    ext = os.path.splitext(path)[1].lower()
    is_excel = ext in ('.xlsx', '.xls')

    print("assign_links_from_business_file ownerId:", ownerId)
    print("Geocode:", geocode, "Mapbox token:", "yes" if mapbox_token else "no")

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

    total_rows = len(rows)
    campaign_ref = get_or_create_campaign(ownerId, campaign_code, campaign_name)

    # Optional: unique address geocoding cache
    geo_cache: Dict[str, Dict] = {}
    def maybe_geocode(row: dict) -> Optional[Dict]:
        if not geocode or not mapbox_token:
            return None
        street = get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.')
        house_no = get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr')
        plz = get_ci(row, 'PLZ', 'Postleitzahl')
        city = get_ci(row, 'Ort', 'Stadt', 'City')
        addr = compose_full_address(street, house_no, plz, city, "Germany")
        if not addr:
            return None
        if addr not in geo_cache:
            geo_cache[addr] = geocode_mapbox(addr, mapbox_token) or None
        return geo_cache[addr]

    # Pre-derive doc ids and intended writes so we can bulk-check existing links once
    precomputed: List[Dict] = []
    for i, row in enumerate(rows):
        in_limit = (limit <= 0) or (i < limit)
        if not in_limit:
            precomputed.append({"in_limit": False, "row": row})
            continue

        doc_id_from_row, dest, business_name, template_raw, template_key = derive_fields_for_business_row(row, dest_default)
        doc_id = doc_id_from_row or f"{(campaign_code or 'L').upper()}-{i+1}"
        precomputed.append({
            "in_limit": True,
            "row": row,
            "dest": dest,
            "doc_id": doc_id,
            "business_name": business_name,
            "template_raw": template_raw,
            "template_key": template_key
        })

    # Optional: bulk-scan existing links to skip them (fast-ish)
    existing_ids: Set[str] = set()
    if skip_existing:
        link_refs = [COL_LINKS.document(item["doc_id"]) for item in precomputed
                     if item.get("in_limit") and item.get("dest")]
        existing_ids = bulk_get_existing(link_refs)
        if existing_ids:
            print(f"[pre-scan] Found {len(existing_ids)} existing link ids (will skip creating those).")

    # Batched writes
    batch = db.batch()
    ops = 0
    def flush():
        nonlocal batch, ops
        if ops:
            batch.commit()
            batch = db.batch()
            ops = 0

    # tqdm progress bar
    pbar = tqdm(precomputed, desc="Processing rows", unit="row")
    try:
        for item in pbar:
            row = item["row"]

            # rows beyond limit: write empty tracking_link and continue
            if not item["in_limit"]:
                row['tracking_link'] = ''
                pbar.set_postfix(cl=created_links, ct=created_targets, sk=skipped, er=errors)
                continue

            dest = item.get("dest")
            doc_id = item.get("doc_id")
            business_name = item.get("business_name")
            template_raw = item.get("template_raw")
            template_key = item.get("template_key")

            try:
                # Business upsert (idempotent, single batched set)
                coordinate = maybe_geocode(row)
                biz_id, biz_payload = upsert_business_payload_from_row(row, ownerId, coordinate)
                biz_ref = COL_BUSINESSES.document(biz_id)

                # Ensure created_at + ownerIds (both merged, idempotent)
                batch.set(biz_ref, {**biz_payload, "created_at": firestore.SERVER_TIMESTAMP}, merge=True); ops += 1
                batch.set(biz_ref, {"ownerIds": ArrayUnion([ownerId])}, merge=True); ops += 1

                # Prepare target & link refs
                target_ref = campaign_ref.collection('targets').document()
                link_ref = COL_LINKS.document(doc_id)

                # Determine status
                status = "validated" if dest else "excluded"

                # Build snapshot directly from current row (no target read)
                snapshot = snapshot_mailing_from_row(row, business_name)

                # Target initial (final) payload — single write
                target_payload = {
                    "business_ref": biz_ref,
                    "status": "linked" if dest else status,
                    "reason_excluded": None if dest else "No destination",
                    "link_ref": link_ref if dest else None,
                    "import_row": row,
                    "dedupe_key": dedupe_key_for_row(row),
                    "created_at": firestore.SERVER_TIMESTAMP,
                    "updated_at": firestore.SERVER_TIMESTAMP
                }
                batch.set(target_ref, target_payload); ops += 1
                created_targets += 1

                # Link payload (only if dest provided and not skipping an existing)
                if dest:
                    if skip_existing and doc_id in existing_ids:
                        skipped += 1
                    else:
                        link_payload = {
                            "campaign_ref": campaign_ref,
                            "business_ref": biz_ref,
                            "target_ref": target_ref,
                            "destination": dest,
                            "template_id": template_with_qr_suffix(template_raw),
                            "short_code": doc_id,
                            "active": True,
                            "hit_count": 0,
                            "created_at": firestore.SERVER_TIMESTAMP,
                            "last_hit_at": None,
                            "owner_id": ownerId,
                            "snapshot_mailing": snapshot
                        }
                        batch.set(link_ref, link_payload, merge=True); ops += 1
                        created_links += 1
                else:
                    skipped += 1

                # Write back to output row
                row['tracking_link'] = build_tracking_link(base_url, doc_id) if dest else ''
                adjusted_template = template_with_qr_suffix(template_raw)
                if adjusted_template:
                    if template_key:
                        row[template_key] = adjusted_template
                    else:
                        row['Template'] = adjusted_template

                # Periodic flush
                if ops >= 400:
                    flush()

            except Exception as e:
                print(f"[error] row: {e}")
                errors += 1
                row['tracking_link'] = ''

            # Update progress bar postfix
            pbar.set_postfix(cl=created_links, ct=created_targets, sk=skipped, er=errors)

    finally:
        # Final commit & close pbar
        flush()
        try:
            pbar.close()
        except Exception:
            pass

    # Aggregate counters once
    COL_CAMPAIGNS.document(campaign_ref.id).set(
        {"totals.targets": firestore.Increment(created_targets),
         "totals.links": firestore.Increment(created_links),
         "updated_at": firestore.SERVER_TIMESTAMP},
        merge=True
    )

    # Write updated file
    if is_excel:
        out_df = pd.DataFrame(rows)
        out_path = write_back_excel(path, out_df)
    else:
        out_path = write_back_csv(path, rows)

    print(f"Done. created_links={created_links} created_targets={created_targets} skipped={skipped} errors={errors}")
    print(f"Processed/uploaded up to limit={limit if limit>0 else 'ALL'} of {total_rows} rows.")
    print(f"Campaign ID: {campaign_ref.id}")
    print(f"Wrote updated file with 'tracking_link': {out_path}")

# ---------------------------
# CLI
# ---------------------------
if __name__ == '__main__':
    p = argparse.ArgumentParser(description='FAST Import CSV/XLSX → businesses + campaign targets + links; write file with tracking links (with tqdm).')

    p.add_argument('--business-file', help='CSV or XLSX to import')
    p.add_argument('--dest', help='Default destination URL (used if row lacks destination/url)')
    p.add_argument('--campaign-code', help='human code to reuse/create a campaign (e.g., ADM-01)')
    p.add_argument('--campaign-name', help='campaign display name (falls back to code or "Untitled Campaign")')
    p.add_argument('--base-url', help='Base URL for tracking links, e.g. https://qr.example.com')
    p.add_argument('--ownerId', help='UID of the user who owns this import/campaign')
    p.add_argument('--limit', type=int, default=0, help='Only process the first X rows (0 = all)')
    p.add_argument('--mapbox-token', default=os.environ.get("MAPBOX_TOKEN"),
                   help='Mapbox API token for geocoding (or set env MAPBOX_TOKEN).')
    p.add_argument('--skip-existing', action='store_true',
                   help='Fast pre-scan to skip creating links whose IDs already exist.')
    p.add_argument('--geocode', action='store_true',
                   help='Enable Mapbox geocoding (deduped per unique address).')

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
            skip_existing=args.skip_existing,
            geocode=args.geocode,
        )
    else:
        p.error('Provide: --business-file + --base-url + --ownerId (optional: --dest, --campaign-code, --campaign-name, --limit, --skip-existing, --geocode).')
