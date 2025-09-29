#https://chatgpt.com/c/68c5312f-837c-832d-9b61-ce41f39c037d
#https://chatgpt.com/c/68d45e54-1310-8328-8d29-f4441e52fc80

# main.py
# Cloud Function (Gen 2) for processing a CSV/XLSX upload in GCS (Firebase Storage)
# - Triggers on finalize of objects with a given prefix/suffix (configured at deploy time)
# - Looks for a sibling manifest.json (or uses object custom metadata / env vars)
# - Downloads the business file to /tmp, runs assign_links_from_business_file, uploads the output

import os
import json
import csv
import re
from typing import Optional, Tuple, List, Set, Dict, Iterable

from google.cloud import storage
from google.cloud import firestore
from google.cloud.firestore_v1 import ArrayUnion
from google.api_core.exceptions import AlreadyExists
import functions_framework  # <- add this import


# ---------------------------
# Config (env vars with sensible defaults)
# ---------------------------
PROJECT_ID  = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")
DEFAULT_BASE_URL = os.environ.get("BASE_URL")                   # optional fallback
DEFAULT_MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN")          # optional fallback

# Instantiate Firestore client once (outside handler)
db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
COL_LINKS = db.collection('links')
COL_BUSINESSES = db.collection('businesses')
COL_CAMPAIGNS = db.collection('campaigns')

# Optional deps (pandas, requests, tqdm)
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
    def tqdm(x, **kwargs):
        return x

# ---------------------------
# Utilities
# ---------------------------
def build_tracking_link(base_url: str, doc_id: str) -> str:
    return f"{base_url.rstrip('/')}/?id={doc_id}"

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
    lower_map = {}
    for k in row.keys():
        if isinstance(k, str):
            lower_map[k.lower()] = k
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None:
            return row.get(key)
    return None

def get_ci_key(row: dict, *names: str) -> Optional[str]:
    lower_map = {}
    for k in row.keys():
        if isinstance(k, str):
            lower_map[k.lower()] = k
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
    print("make_business_id", business_name, plz)
    base = sanitize_id(business_name or "")
    if plz:
        base = f"{base}-{sanitize_id(plz)}" if base else sanitize_id(plz)
    return base

def dedupe_key_for_row(row: dict) -> str:
    name = (get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company') or '').lower().strip()
    street = (get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.') or '').lower().strip().replace('ß', 'ss')
    house = (get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr') or '').lower().strip()
    plz = (get_ci(row, 'PLZ', 'Postleitzahl') or '').lower().strip()
    city = (get_ci(row, 'Ort', 'Stadt', 'City') or '').lower().strip()
    return f"{re.sub(r'[^a-z0-9]+','-',name)}|{re.sub(r'[^a-z0-9]+','-',street)}-{re.sub(r'[^a-z0-9]+','-',house)}|{plz}|{re.sub(r'[^a-z0-9]+','-',city)}"

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
    existing: Set[str] = set()
    for chunk in chunked(doc_refs, 300):
        for snap in db.get_all(chunk):
            if snap.exists:
                existing.add(snap.id)
    return existing


def get_or_create_campaign(owner_id: str,
                           campaign_id: str,
                           name: Optional[str],
                           code: Optional[str] = None) -> firestore.DocumentReference:
    """
    Always use the provided campaignId as Firestore doc ID.
    If the doc doesn't exist yet, create it with base fields.
    """
    if not campaign_id:
        raise RuntimeError("campaignId is required but missing")

    ref = COL_CAMPAIGNS.document(campaign_id)
    snap = ref.get()

    if not snap.exists:
        payload = {
            "campaign_name": name or "Untitled Campaign",
            "code": code or None,
            "owner_id": owner_id,
            "status": "draft",
            "totals": {"targets": 0, "links": 0, "hits": 0, "unique_ips": 0},
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP
        }
        ref.set(payload)
        print(f"[campaign] Created campaign with ID {campaign_id}")
    else:
        print(f"[campaign] Using existing campaign with ID {campaign_id}")

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
    if pd is None:
        raise RuntimeError("Excel output requires pandas/openpyxl.")
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return out_path

# ---------------------------
# Core flow (unchanged logic, minus argparse)
# ---------------------------
def assign_links_from_business_file(path: str, base_url: str,
                                    destination: Optional[str],
                                    campaign_code: Optional[str],
                                    campaign_name: Optional[str],
                                    campaign_id: Optional[str],
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
        #CSV Helpers
        def _open_text(path: str):
            # Handle BOM + normalize newlines
            return open(path, "r", encoding="utf-8-sig", newline="")

        def _detect_delimiter(sample: str) -> str:
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
                return dialect.delimiter
            except Exception:
                # Fallback: detect by counts
                counts = {d: sample.count(d) for d in [",", ";", "\t", "|"]}
                return max(counts, key=counts.get) if max(counts.values()) > 0 else ","

        with _open_text(path) as f:
            sample = f.read(4096)
            f.seek(0)
            delimiter = _detect_delimiter(sample)
            reader = csv.DictReader(f, delimiter=delimiter, restkey="_extra", restval="")
            rows = []
            for r in reader:
                # force string keys, drop spillover if you don’t need it
                r = { (k if isinstance(k, str) else str(k)): v for k, v in r.items() }
                r.pop("_extra", None)
                rows.append(r)
        print(f"[csv] delimiter='{delimiter}' rows={len(rows)}")
        print("2 ROWS", rows[:2])


    total_rows = len(rows)
    campaign_ref = get_or_create_campaign(ownerId, campaign_id, campaign_name, campaign_code)

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

    precomputed: List[Dict] = []
    for i, row in enumerate(rows):
        in_limit = (limit <= 0) or (i < limit)
        if not in_limit:
            precomputed.append({"in_limit": False, "row": row})
            continue

        doc_id_from_row = get_ci(row, 'id', 'link_id')
        dest = get_ci(row, 'destination', 'url') or destination
        business_name = get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company')
        template_key = get_ci_key(row, 'Template', 'template')
        template_raw = row.get(template_key) if template_key else None

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

    existing_ids: Set[str] = set()
    if skip_existing:
        link_refs = [COL_LINKS.document(item["doc_id"]) for item in precomputed
                     if item.get("in_limit") and item.get("dest")]
        existing_ids = bulk_get_existing(link_refs)
        if existing_ids:
            print(f"[pre-scan] Found {len(existing_ids)} existing link ids (will skip creating those).")

    batch = db.batch()
    ops = 0
    def flush():
        nonlocal batch, ops
        if ops:
            batch.commit()
            batch = db.batch()
            ops = 0

    pbar = tqdm(precomputed, desc="Processing rows", unit="row")
    try:
        for item in pbar:
            row = item["row"]
            if not item["in_limit"]:
                row['tracking_link'] = ''
                continue

            dest = item.get("dest")
            doc_id = item.get("doc_id")
            business_name = item.get("business_name")
            template_raw = item.get("template_raw")
            template_key = item.get("template_key")

            try:
                coordinate = maybe_geocode(row)
                biz_id, biz_payload = upsert_business_payload_from_row(row, ownerId, coordinate)
                biz_ref = COL_BUSINESSES.document(biz_id)

                batch.set(biz_ref, {**biz_payload, "created_at": firestore.SERVER_TIMESTAMP}, merge=True); ops += 1
                batch.set(biz_ref, {"ownerIds": ArrayUnion([ownerId])}, merge=True); ops += 1

                target_ref = campaign_ref.collection('targets').document()
                link_ref = COL_LINKS.document(doc_id)
                status = "validated" if dest else "excluded"
                snapshot = snapshot_mailing_from_row(row, business_name)

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

                if dest:
                    if skip_existing and doc_id in existing_ids:
                        # skip creating link, keep target status as linked
                        pass
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
                            "snapshot_mailing": snapshot,
                            "campaign_name": campaign_name,
                        }
                        batch.set(link_ref, link_payload, merge=True); ops += 1
                        created_links += 1
                else:
                    # excluded
                    pass

                row['tracking_link'] = build_tracking_link(base_url, doc_id) if dest else ''
                adjusted_template = template_with_qr_suffix(template_raw)
                if adjusted_template:
                    if template_key:
                        row[template_key] = adjusted_template
                    else:
                        row['Template'] = adjusted_template

                if ops >= 400:
                    flush()

            except Exception as e:
                print(f"[error] row: {e}")
                errors += 1
                row['tracking_link'] = ''

    finally:
        flush()
        try:
            pbar.close()
        except Exception:
            pass

    COL_CAMPAIGNS.document(campaign_ref.id).set(
        {"totals.targets": firestore.Increment(created_targets),
         "totals.links": firestore.Increment(created_links),
         "updated_at": firestore.SERVER_TIMESTAMP},
        merge=True
    )

    if is_excel:
        if pd is None:
            raise RuntimeError("Excel output requires pandas/openpyxl")
        out_df = pd.DataFrame(rows)
        out_path = write_back_excel(path, out_df)
    else:
        out_path = write_back_csv(path, rows)

    print(f"Done. created_links={created_links} created_targets={created_targets} skipped={skipped} errors={errors}")
    return out_path

# ---------------------------
# GCS helpers
# ---------------------------
def _download_blob(bucket: storage.Bucket, blob_name: str, local_path: str):
    blob = bucket.blob(blob_name)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    blob.download_to_filename(local_path)

def _upload_blob(bucket: storage.Bucket, local_path: str, dest_blob_name: str, content_type: Optional[str] = None):
    blob = bucket.blob(dest_blob_name)
    if content_type:
        blob.content_type = content_type
    blob.upload_from_filename(local_path)

def _load_manifest(bucket: storage.Bucket, prefix_dir: str) -> Dict:
    """Try prefix_dir/manifest.json; returns {} if none."""
    manifest_blob = bucket.blob(f"{prefix_dir.rstrip('/')}/manifest.json")
    if not manifest_blob.exists():
        return {}
    data = json.loads(manifest_blob.download_as_text())
    if not isinstance(data, dict):
        return {}
    return data

def _content_type_for(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return "text/csv"
    if ext in (".xlsx", ".xls"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return "application/octet-stream"

# ---------------------------
# CloudEvent entry point (Gen 2)
# ---------------------------
@functions_framework.cloud_event   # <- add this decorator
def process_business_upload(cloud_event):
    """
    Triggered by: google.cloud.storage.object.v1.finalized
    Event data shape: https://cloud.google.com/eventarc/docs/cloudevents#storage
    """
    # Ignore our own artifacts

    data = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]               # e.g., uploads/job-123/businesses.xlsx
    content_type = data.get("contentType", "")
    metadata = data.get("metadata", {}) or {}
    #name = (data.get("name") or "").lower()

    #logging start
    print("⚡ trigger")
    print("bucket_name", bucket_name, "object_name", object_name, "content_type", content_type, "metadata", metadata)
    meta = {
        "id": getattr(cloud_event, "id", None),
        "source": getattr(cloud_event, "source", None),
        "type": getattr(cloud_event, "type", None),
        "subject": getattr(cloud_event, "subject", None),
    }
    try:
        data = cloud_event.data or {}
    except Exception:
        data = cloud_event.get("data", {}) if isinstance(cloud_event, dict) else {}

    print(json.dumps({"meta": meta, "bucket": data.get("bucket"),
                      "name": data.get("name"), "contentType": data.get("contentType"),
                      "metadata": data.get("metadata")}, ensure_ascii=False))

    name = (data.get("name") or "").lower()
    if not name.startswith("uploads/dev/"):
        print(f"[skip] outside watched prefix: {name}")
        return
    if not (name.endswith(".csv") or name.endswith(".xlsx")):
        print(f"[skip] not CSV/XLSX: {name}")
        return
    
    # Ignore our own artifacts
    if "_with_links" in name:
        print(f"[skip] artifact filename: {name}")
        return
    #logging end


    # Only process uploads under uploads/dev/
    #if not name.startswith("uploads/dev/"):
    #    print(f"[skip] outside watched prefix: {name}")
    #    return

    # Ignore non CSV/XLSX files (e.g., manifest.json uploads)
    if not object_name.lower().endswith((".csv", ".xlsx", ".xls")):
        print(f"[skip] Not a CSV/XLSX: {object_name}")
        return

    print(f"[start] bucket={bucket_name} object={object_name} content_type={content_type}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Prefix directory (sibling place for manifest.json and output)
    prefix_dir = os.path.dirname(object_name)

    # Load manifest.json if present
    manifest = _load_manifest(bucket, prefix_dir)
    print(f"[manifest] {manifest}")

    # Pull inputs from manifest → object metadata → env vars
    ownerId = manifest.get("ownerId") or metadata.get("ownerId")
    base_url = manifest.get("base_url") or metadata.get("base_url") or DEFAULT_BASE_URL
    if not base_url:
        raise RuntimeError("base_url missing (provide via manifest.base_url, object metadata, or env BASE_URL).")
    if not ownerId:
        raise RuntimeError("ownerId missing (provide via manifest.ownerId or object metadata).")

    params = {
        "destination": manifest.get("destination") or metadata.get("destination"),
        "campaign_code": manifest.get("campaign_code") or metadata.get("campaign_code"),
        "campaign_name": manifest.get("campaign_name") or metadata.get("campaign_name"),
        "campaign_id": manifest.get("campaignId") or metadata.get("campaign_id"),
        "limit": int(manifest.get("limit", 0) or metadata.get("limit", 0) or 0),
        "skip_existing": bool(manifest.get("skip_existing", False) or (metadata.get("skip_existing") in ("1", "true", "True"))),
        "geocode": bool(manifest.get("geocode", False) or (metadata.get("geocode") in ("1", "true", "True"))),
        "mapbox_token": manifest.get("mapbox_token") or metadata.get("mapbox_token") or DEFAULT_MAPBOX_TOKEN,
    }

    # Download uploaded file to /tmp
    local_in = os.path.join("/tmp", os.path.basename(object_name))
    _download_blob(bucket, object_name, local_in)

    # Process
    out_path = assign_links_from_business_file(
        path=local_in,
        base_url=base_url,
        destination=params["destination"],
        campaign_code=params["campaign_code"],
        campaign_name=params["campaign_name"],
        campaign_id=params["campaign_id"],
        ownerId=ownerId,
        limit=params["limit"],
        mapbox_token=params["mapbox_token"],
        skip_existing=params["skip_existing"],
        geocode=params["geocode"],
    )

    # Upload output next to input (same folder), with suffix
    out_name = os.path.basename(out_path)
    dest_blob = f"{prefix_dir}/{out_name}" if prefix_dir else out_name
    _upload_blob(bucket, out_path, dest_blob, _content_type_for(out_path))

    print(f"[done] Wrote: gs://{bucket_name}/{dest_blob}")
