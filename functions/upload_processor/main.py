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
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import firestore
from google.cloud.firestore_v1 import ArrayUnion
from google.api_core.exceptions import AlreadyExists
import functions_framework  # <- add this import
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    # Preferred public path in recent releases
    from google.cloud.firestore_v1.field_path import FieldPath
except Exception:  # fallback for older/packaged versions
    FieldPath = None


import tldextract



# ---------------------------
# Config (env vars with sensible defaults)
# ---------------------------
COMMON_EMAIL_PROVIDERS = {
    "gmail", "gmx", "aol", "yahoo", "hotmail", "outlook",
    "icloud", "t-online", "web", "live", "msn", "mail"
}
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
class DuplicateCampaignCodeError(RuntimeError):
    pass

def build_tracking_link(base_url: str, doc_id: str) -> str:
    return f"{base_url.rstrip('/')}/?id={doc_id}"

def sanitize_id(value: str) -> str:
    if value is None:
        return ""
    v = str(value).strip()
    v = re.sub(r"[^A-Za-z0-9]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    return v


def existing_variants_for_base(COL_LINKS, base_id: str) -> set[str]:
    """
    Return all doc IDs that start with base_id: { base_id, base_id-1, base_id-2, ... }.
    Uses a range query on document ID (aka __name__).
    - Compares against DocumentReferences, not strings.
    - Falls back to '__name__' if FieldPath isn't importable.
    - Guards empty base to avoid scanning whole collection.
    """
    base_id = (base_id or "").strip()
    if not base_id:
        return set()

    # Build DocumentReference bounds
    start_ref = COL_LINKS.document(base_id)
    end_ref = COL_LINKS.document(base_id + u"\uf8ff")

    # Field path for document id
    fp = FieldPath.document_id() if FieldPath else "__name__"

    # Query only IDs (tiny payload)
    q = (
        COL_LINKS
        .where(fp, ">=", start_ref)
        .where(fp, "<=", end_ref)   # '<=' is fine here; can use '<' if you prefer
        .select([])                 # no fields, just names
    )

    return {doc.id for doc in q.stream()}


def next_id_from_cache(base_id: str, taken: set[str]) -> str:
    """Pick base_id if free, else base_id-<n> with the smallest available n >= 1."""
    if base_id not in taken:
        taken.add(base_id)
        return base_id
    # Find the max numeric suffix already taken for this base
    pat = re.compile(rf"^{re.escape(base_id)}-(\d+)$")
    max_n = 0
    for did in taken:
        m = pat.match(did)
        if m:
            max_n = max(max_n, int(m.group(1)))
    candidate = f"{base_id}-{max_n + 1}"
    taken.add(candidate)
    return candidate

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
    #print("make_business_id", business_name, plz)
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

def bulk_get_existing_old(doc_refs: List[firestore.DocumentReference]) -> Set[str]:
    existing: Set[str] = set()
    for chunk in chunked(doc_refs, 300):
        for snap in db.get_all(chunk):
            if snap.exists:
                existing.add(snap.id)
    return existing

def bulk_get_existing(
    doc_refs: List[firestore.DocumentReference],
    chunk_size: int = 500,
    max_workers: int = 4,
) -> Set[str]:
    """
    Return the set of IDs that exist among the given doc_refs.
    Uses get_all with an empty field mask so we fetch only metadata (IDs), not fields.
    Chunked for safety; optionally parallelized for large batches.
    """
    if not doc_refs:
        return set()

    existing: Set[str] = set()

    def fetch_chunk(chunk: List[firestore.DocumentReference]) -> List[str]:
        # IMPORTANT: field_paths=[] -> request no document fields (tiny payload)
        snaps = db.get_all(chunk, field_paths=[])
        return [snap.id for snap in snaps if getattr(snap, "exists", False)]

    # Small batches: run inline
    if len(doc_refs) <= chunk_size or max_workers <= 1:
        for chunk in chunked(doc_refs, chunk_size):
            for doc_id in fetch_chunk(chunk):
                existing.add(doc_id)
        return existing

    # Larger batches: parallelize across a few workers
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(fetch_chunk, chunk) for chunk in chunked(doc_refs, chunk_size)]
        for fut in as_completed(futures):
            for doc_id in fut.result():
                existing.add(doc_id)

    return existing


def load_blacklist(owner_id: str) -> Set[str]:
    """
    Load all blacklisted business_ids for a given owner.
    Returns a set of business_id strings.
    """
    if not owner_id:
        return set()
    
    try:
        blacklist_ref = db.collection('customers').document(owner_id).collection('blacklist')
        blacklisted_ids = set()
        
        for doc in blacklist_ref.stream():
            data = doc.to_dict() or {}
            # Check business_id field
            business_id = data.get('business_id')
            if business_id:
                blacklisted_ids.add(str(business_id))
            
            # Also check the business reference if present
            business_ref = data.get('business')
            if business_ref:
                # business_ref is a DocumentReference, get its ID
                if hasattr(business_ref, 'id'):
                    blacklisted_ids.add(business_ref.id)
                elif isinstance(business_ref, str):
                    # Handle case where it's stored as a path string like "/businesses/2DC-GmbH-33602"
                    if '/businesses/' in business_ref:
                        parts = business_ref.split('/businesses/')
                        if len(parts) == 2:
                            blacklisted_ids.add(parts[1])
        
        print(f"[blacklist] Loaded {len(blacklisted_ids)} blacklisted business_ids for owner {owner_id}")
        return blacklisted_ids
    except Exception as e:
        print(f"[warn] Failed to load blacklist for owner {owner_id}: {e}")
        return set()


def normalize_campaign_code(code: Optional[str]) -> str:
    if not code:
        raise RuntimeError("campaign_code is required but missing")
    return sanitize_id(code).upper()



def get_or_create_campaign(owner_id: str,
                           campaign_id: str,
                           name: Optional[str],
                           code: Optional[str] = None) -> firestore.DocumentReference:
    if not campaign_id:
        raise RuntimeError("campaignId is required but missing")

    code_norm = sanitize_id(code).upper() if code else None

    # Check if any other campaign already has this code
    if code_norm:
        conflict_q = COL_CAMPAIGNS.where("code", "==", code_norm).limit(1).stream()
        for doc in conflict_q:
            if doc.id != campaign_id:
                raise DuplicateCampaignCodeError(
                    f"campaign_code '{code_norm}' is already in use by campaign '{doc.id}'. "
                    "Choose a different code."
                )

    ref = COL_CAMPAIGNS.document(campaign_id)
    snap = ref.get()

    if not snap.exists:
        payload = {
            "campaign_name": name or "Untitled Campaign",
            "code": code_norm,
            "owner_id": owner_id,
            "status": "draft",
            "totals": {"targets": 0, "links": 0, "hits": 0, "unique_ips": 0},
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP
        }
        ref.set(payload)
        print(f"[campaign] Created campaign with ID {campaign_id}")
    else:
        data = snap.to_dict() or {}
        existing_code = data.get("code")
        if existing_code and code_norm and existing_code != code_norm:
            raise DuplicateCampaignCodeError(
                f"Campaign '{campaign_id}' already has code '{existing_code}', not '{code_norm}'."
            )
        if not existing_code and code_norm:
            ref.set({"code": code_norm, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
        print(f"[campaign] Using existing campaign with ID {campaign_id}")

    return ref



def get_or_create_campaign_old(owner_id: str,
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

    #print("upsert_business_payload_from_row", business_name, street, house_no, plz, city, contact_name, phone, email, salutation, full_addr, coordinate)

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

def _extract_registrable_domain(email: str) -> str | None:
    try:
        host = email.split("@", 1)[1]
    except IndexError:
        return None
    ext = tldextract.extract(host)  # ext.domain is what you want
    return ext.domain or None

def _is_common_provider(email: str) -> bool:
    domain = _extract_registrable_domain(email)
    return domain in COMMON_EMAIL_PROVIDERS if domain else False

from collections import defaultdict

def assign_final_ids(precomputed: List[Dict]) -> None:
    """
    For each unique base_id in precomputed, load existing Firestore variants once
    and assign a collision-free final_id (base or base-<n>) to each item.
    Mutates items in-place: item['final_id'] = ...
    """
    # Group rows by base
    groups = defaultdict(list)
    for item in precomputed:
        if item.get("in_limit") and item.get("dest"):
            base = item.get("base_id") or ""
            groups[base].append(item)

    # For each base, query existing variants once, then allocate final IDs
    for base_id, items in groups.items():
        if not base_id:
            # still allow empty base; sanitize_id already tried to keep it readable
            # if truly empty, they’ll become "", "-1", etc — unlikely given our fallbacks
            pass
        taken = existing_variants_for_base(COL_LINKS, base_id)
        for item in items:
            item["final_id"] = next_id_from_cache(base_id, taken)




# ---------------------------
# Core flow (unchanged logic, minus argparse)
# ---------------------------
def assign_links_from_business_file(path: str, base_url: str,
                                    destination: Optional[str],
                                    campaign_code: Optional[str],
                                    campaign_code_from_business: bool,
                                    campaign_name: Optional[str],
                                    campaign_id: Optional[str],
                                    ownerId: str,
                                    limit: int,
                                    mapbox_token: Optional[str],
                                    skip_existing: bool,
                                    geocode: bool = True):
    created_links, created_targets = 0, 0
    skipped, errors = 0, 0
    blacklisted_count = 0
    blacklisted_details = []
    error_details = []
    excluded_no_destination = 0
    geocoding_successful = 0
    geocoding_failed = 0
    processing_start = datetime.now(timezone.utc)
    
    ext = os.path.splitext(path)[1].lower()
    is_excel = ext in ('.xlsx', '.xls')

    print("assign_links_from_business_file ownerId:", ownerId)
    
    # Load blacklist at the start
    blacklisted_business_ids = load_blacklist(ownerId)
    
    #print("Geocode:", geocode, "Mapbox token:", "yes" if mapbox_token else "no")

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
        #print(f"[csv] delimiter='{delimiter}' rows={len(rows)}")
        #print("2 ROWS", rows[:2])


    total_rows = len(rows)
    # How will I handle this if I use business ids from email?
    campaign_ref = get_or_create_campaign(ownerId, campaign_id, campaign_name, campaign_code)
    print("Using campaign ref id:", campaign_ref.id)

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


    print("Total rows in input file:", total_rows)
    precomputed: List[Dict] = []
    for i, row in enumerate(rows):
        in_limit = (limit <= 0) or (i < limit)
        
        # Check if business is blacklisted BEFORE other processing
        business_name = get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company')
        plz = get_ci(row, 'PLZ', 'Postleitzahl')
        biz_id = make_business_id(business_name, plz)
        
        is_blacklisted = biz_id in blacklisted_business_ids
        if is_blacklisted:
            print(f"[blacklist] Skipping blacklisted business: {biz_id} (row {i+1})")
            blacklisted_count += 1
            # Track blacklisted business details
            blacklisted_details.append({
                "business_id": biz_id,
                "business_name": business_name,
                "row_number": i + 1,
                "plz": plz,
                "city": get_ci(row, 'Ort', 'Stadt', 'City')
            })
            # Mark row to be excluded from final output
            row['_blacklisted'] = True
            precomputed.append({"in_limit": False, "row": row, "blacklisted": True})
            continue
        
        if not in_limit:
            precomputed.append({"in_limit": False, "row": row, "blacklisted": False})
            continue

        doc_id_from_row = get_ci(row, 'id', 'link_id')  # may be None
        dest = get_ci(row, 'destination', 'url') or destination
        template_key = get_ci_key(row, 'Template', 'template')
        template_raw = row.get(template_key) if template_key else None

        if campaign_code_from_business:
            email = get_ci(row, 'E-Mail-Adresse', 'Email', 'E-Mail', 'Mail')
            if not email:
                # No email provided - use clean business name from "Namenszeile"
                print("DEBUG business_name:", business_name)
                base_id = _extract_clean_business_name(business_name)
            elif _is_common_provider(email):
                base_id = doc_id_from_row or f"{(campaign_code or 'L').upper()}-{i+1}"
            else:
                # Business email - use domain
                print("DEBUG email:", email)
                base_id = _extract_registrable_domain(email)
        else:
            base_id = doc_id_from_row or business_name or f"{(campaign_code or 'L').upper()}-{i+1}"

        base_id = sanitize_id(base_id or "")

        print("DEBUG base_id:", base_id)

        precomputed.append({
            "in_limit": True,
            "row": row,
            "dest": dest,
            "base_id": base_id,          # <— store base
            "business_name": business_name,
            "template_raw": template_raw,
            "template_key": template_key,
            "blacklisted": False
        })

    assign_final_ids(precomputed)

    print("Precomputed", precomputed[:3])
    print("Len precomputed:", len(precomputed))

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
            final_id = item.get("final_id")    # <- use the ID allocated earlier
            business_name = item.get("business_name")
            template_raw = item.get("template_raw")
            template_key = item.get("template_key")

            print("DEBUG final_id:", final_id)

            try:
                coordinate = maybe_geocode(row)
                # Track geocoding stats
                if geocode:
                    if coordinate:
                        geocoding_successful += 1
                    else:
                        geocoding_failed += 1
                
                biz_id, biz_payload = upsert_business_payload_from_row(row, ownerId, coordinate)
                biz_ref = COL_BUSINESSES.document(biz_id)
                current_biz_id = biz_id  # Store for error tracking

                # upsert business
                batch.set(biz_ref, {**biz_payload, "created_at": firestore.SERVER_TIMESTAMP}, merge=True); ops += 1
                batch.set(biz_ref, {"ownerIds": ArrayUnion([ownerId])}, merge=True); ops += 1

                # target
                target_ref = campaign_ref.collection('targets').document()
                status = "validated" if dest else "excluded"
                snapshot = snapshot_mailing_from_row(row, business_name)

                # reference to link doc (by final_id)
                link_ref = COL_LINKS.document(final_id) if dest else None

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
                
                # Track excluded rows (no destination)
                if not dest:
                    excluded_no_destination += 1

                print("DEBUG target_payload:", target_payload)
                print("DEBUG dest:", dest)

                if dest:
                    # Try to create link with final_id. If a rare race hits, retry once with the next suffix.
                    try:
                        print(f"Creating link with ID: {final_id}")
                        batch.create(COL_LINKS.document(final_id), {
                            "campaign_ref": campaign_ref,
                            "business_ref": biz_ref,
                            "target_ref": target_ref,
                            "destination": dest,
                            "template_id": template_with_qr_suffix(template_raw),
                            "short_code": final_id,   # mirror the human-readable ID
                            "active": True,
                            "hit_count": 0,
                            "created_at": firestore.SERVER_TIMESTAMP,
                            "last_hit_at": None,
                            "owner_id": ownerId,
                            "snapshot_mailing": snapshot,
                            "campaign_name": campaign_name,
                        })
                        ops += 1
                        created_links += 1
                    except AlreadyExists:
                        # Recompute suffix (another worker probably grabbed our final_id)
                        print(f"[warn] Link ID collision for '{final_id}', retrying with next suffix")
                        base = item.get("base_id") or final_id
                        taken = existing_variants_for_base(COL_LINKS, base)
                        retry_id = next_id_from_cache(base, taken)

                        batch.create(COL_LINKS.document(retry_id), {
                            "campaign_ref": campaign_ref,
                            "business_ref": biz_ref,
                            "target_ref": target_ref,
                            "destination": dest,
                            "template_id": template_with_qr_suffix(template_raw),
                            "short_code": retry_id,
                            "active": True,
                            "hit_count": 0,
                            "created_at": firestore.SERVER_TIMESTAMP,
                            "last_hit_at": None,
                            "owner_id": ownerId,
                            "snapshot_mailing": snapshot,
                            "campaign_name": campaign_name,
                        })
                        ops += 1
                        created_links += 1
                        final_id = retry_id             # make sure output uses the actual created ID

                # write back tracking link + template into the row
                row['tracking_link'] = build_tracking_link(base_url, final_id) if dest else ''
                print("DEBUG tracking_link:", row['tracking_link'])
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
                # Track error details
                error_biz_id = None
                try:
                    error_biz_id = current_biz_id if 'current_biz_id' in locals() else None
                except:
                    pass
                error_details.append({
                    "row_number": i + 1,
                    "business_name": business_name or "Unknown",
                    "business_id": error_biz_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                })
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

    # Filter out blacklisted rows before writing output
    filtered_rows = [r for r in rows if not r.get('_blacklisted', False)]
    
    if is_excel:
        if pd is None:
            raise RuntimeError("Excel output requires pandas/openpyxl")
        out_df = pd.DataFrame(filtered_rows)
        out_path = write_back_excel(path, out_df)
    else:
        out_path = write_back_csv(path, filtered_rows)

    processing_end = datetime.now(timezone.utc)
    
    # Prepare statistics
    statistics = {
        "created_links": created_links,
        "created_targets": created_targets,
        "skipped": skipped,
        "errors": errors,
        "blacklisted_count": blacklisted_count,
        "blacklisted_details": blacklisted_details,
        "error_details": error_details,
        "excluded_no_destination": excluded_no_destination,
        "geocoding_stats": {
            "enabled": geocode,
            "successful": geocoding_successful,
            "failed": geocoding_failed
        },
        "total_rows": total_rows,
        "processed_rows": len(filtered_rows)
    }

    print(f"Done. created_links={created_links} created_targets={created_targets} skipped={skipped} errors={errors} blacklisted={blacklisted_count}")
    
    return {
        "output_path": out_path,
        "statistics": statistics,
        "processing_start": processing_start,
        "processing_end": processing_end
    }

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

def generate_upload_report(
    statistics: Dict,
    campaign_info: Dict,
    input_file_info: Dict,
    output_file_path: str,
    processing_start: datetime,
    processing_end: datetime,
    owner_id: str
) -> Dict:
    """
    Generate a comprehensive upload report.
    
    Returns a dictionary that can be serialized to JSON.
    """
    duration = (processing_end - processing_start).total_seconds()
    
    report = {
        "upload_id": f"{campaign_info.get('campaign_id', 'unknown')}-{int(processing_start.timestamp())}",
        "timestamp": processing_end.isoformat(),
        "campaign": {
            "campaign_id": campaign_info.get("campaign_id"),
            "campaign_name": campaign_info.get("campaign_name"),
            "campaign_code": campaign_info.get("campaign_code"),
        },
        "input_file": input_file_info,
        "processing": {
            "started_at": processing_start.isoformat(),
            "completed_at": processing_end.isoformat(),
            "duration_seconds": round(duration, 2)
        },
        "statistics": {
            "total_rows": statistics.get("total_rows", 0),
            "processed_rows": statistics.get("processed_rows", 0),
            "successful_links": statistics.get("created_links", 0),
            "targets_created": statistics.get("created_targets", 0),
            "blacklisted": {
                "count": statistics.get("blacklisted_count", 0),
                "businesses": statistics.get("blacklisted_details", [])
            },
            "skipped": {
                "count": statistics.get("skipped", 0),
                "reason": "limit_exceeded"
            },
            "errors": {
                "count": statistics.get("errors", 0),
                "details": statistics.get("error_details", [])
            },
            "excluded": {
                "count": statistics.get("excluded_no_destination", 0),
                "reason": "no_destination"
            },
            "geocoding": statistics.get("geocoding_stats", {
                "enabled": False,
                "successful": 0,
                "failed": 0
            })
        },
        "output_files": {
            "with_links": output_file_path
        },
        "status": "completed",
        "owner_id": owner_id
    }
    
    return report

def _delete_prefix(bucket: storage.Bucket, prefix: str) -> int:
    """
    Deletes all blobs under the given prefix. Returns number of deleted blobs.
    Safe to call multiple times. Ignores missing files.
    """
    print(f"[cleanup] Deleting storage prefix: {prefix}")
    deleted = 0
    # list_blobs paginates under the hood
    for blob in bucket.list_blobs(prefix=prefix):
        try:
            blob.delete()
            deleted += 1
        except Exception as e:
            # Don't block the whole function on a single delete error; just log
            print(f"[cleanup] Warn: failed to delete {blob.name}: {e}")
    print(f"[cleanup] Deleted {deleted} blobs under {prefix}")
    return deleted




def _extract_clean_business_name(business_name: Optional[str]) -> Optional[str]:
    """
    Extract a clean, short business name from a full business name.
    Removes common business suffixes, handles special characters, and keeps it concise.
    """
    if not business_name:
        return None

    name = str(business_name).strip()

    # --- 1️⃣ Convert umlauts early ---
    umlaut_map = {
        'ä': 'ae', 'ö': 'oe', 'ü': 'ue',
        'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue',
        'ß': 'ss'
    }
    for umlaut, replacement in umlaut_map.items():
        name = name.replace(umlaut, replacement)

    # --- 2️⃣ Handle "&" and "@" correctly ---
    # Join words directly so "A & B" → "AundB", "a@m" → "aatm"
    name = re.sub(r'\s*&\s*', 'und', name)
    name = re.sub(r'&', 'und', name)
    name = re.sub(r'\s*@\s*', 'at', name)
    name = name.replace('@', 'at')

    # --- 3️⃣ Remove leading special characters and normalize hyphens ---
    name = re.sub(r'^[/\s\-_]+', '', name)
    name = re.sub(r'\s*-\s*', '-', name)

    # --- 4️⃣ Remove legal suffixes anywhere (not just at the end) ---
    suffix_token = (
        r'(?:gmbh(?:\s*und\s*co\.?\s*kg)?|'  # GmbH + GmbH und Co. KG
        r'co\.?\s*kg|'
        r'kg|ag|mbh|e\.?v\.?|ug|ohg|gbr|inc\.?|ltd\.?|llc|corp\.?)'
    )
    name = re.sub(rf'(?i)(^|[\s\-_]){suffix_token}($|[\s\-_])', ' ', name)
    name = re.sub(r'(?i)gmbhundco', ' ', name)  # handle glued-together variant

    # --- 5️⃣ Normalize whitespace and separators ---
    name = re.sub(r'[_/]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    if not name:
        return None

    # --- 6️⃣ Split and remove trailing numbers (ZIP codes etc.) ---
    parts = name.split()
    while parts and re.fullmatch(r'\d+', parts[-1]):
        parts.pop()
    if not parts:
        return None

    # --- 7️⃣ Clean each part ---
    clean_parts = []
    for p in parts:
        p = p.lower()
        p = re.sub(r'[^a-z0-9\-]+', '-', p)
        p = re.sub(r'-{2,}', '-', p).strip('-')
        if p:
            clean_parts.append(p)
    if not clean_parts:
        return None

    # --- 8️⃣ Decide how many words to include ---
    first = clean_parts[0]
    rest = clean_parts[1:]
    stopwords = {'und', 'and', 'the', 'der', 'die', 'das', 'für', 'fuer', 'co', 'kg', 'mbh'}

    result_parts = [first]
    if rest:
        second = rest[0]
        first_len = len(first.replace('-', ''))
        combined_len = len(first) + 1 + len(second)
        first_is_numeric = first.isdigit()

        include_second = False
        if first_len <= 2 or first_is_numeric:
            include_second = True
        elif combined_len <= 20 and first_len <= 8 and ('-' not in first or first_len <= 4):
            include_second = True

        if include_second and second not in stopwords:
            result_parts.append(second)

    # --- 9️⃣ Final normalization ---
    result = '-'.join(result_parts)
    result = re.sub(r'-{2,}', '-', result).strip('-')

    return result if result else None



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
    # Derive the campaign root prefix: uploads/<env>/<uid>/<campaignId>/
    parts = object_name.split("/")
    # Expecting: ["uploads", env, uid, campaignId, "source", "file.ext" ...]
    campaign_root_prefix = None
    if len(parts) >= 4:
        campaign_root_prefix = "/".join(parts[:4]).rstrip("/") + "/"

    content_type = data.get("contentType", "")
    metadata = data.get("metadata", {}) or {}
    #name = (data.get("name") or "").lower()

    #logging start
    print("⚡ trigger")
    print("cloud_event", cloud_event)
    print("data", data)
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
    if not name.startswith("uploads/"):
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
        "campaign_code": manifest.get("campaign_code") or metadata.get("campaign_code"), #= trackingPrefix in frontend
        "campaign_name": manifest.get("campaign_name") or metadata.get("campaign_name"),
        "campaign_code_from_business": bool(manifest.get("campaign_code_from_business", False) or (metadata.get("campaign_code_from_business") in ("1", "true", "True"))),
        "campaign_id": manifest.get("campaignId") or metadata.get("campaign_id"),
        "limit": int(manifest.get("limit", 0) or metadata.get("limit", 0) or 0),
        "skip_existing": bool(manifest.get("skip_existing", True) or (metadata.get("skip_existing") in ("1", "true", "True"))),
        "geocode": bool(manifest.get("geocode", False) or (metadata.get("geocode") in ("1", "true", "True"))),
        "mapbox_token": manifest.get("mapbox_token") or metadata.get("mapbox_token") or DEFAULT_MAPBOX_TOKEN,
        #use business domain as tracking id 
    }

    campaign_code = params["campaign_code"]
    if not campaign_code:
        raise RuntimeError("campaign_code is required.")
    params["campaign_code"] = normalize_campaign_code(campaign_code)


    print("PARAMS", params)

    # Download uploaded file to /tmp
    local_in = os.path.join("/tmp", os.path.basename(object_name))
    _download_blob(bucket, object_name, local_in)

    # ---------------------------
    # Process (with minimal changes): on DuplicateCampaignCodeError, delete folder & log
    # ---------------------------
    processing_start = datetime.now(timezone.utc)
    try:
        # Process
        result = assign_links_from_business_file(
            path=local_in,
            base_url=base_url,
            destination=params["destination"],
            campaign_code=params["campaign_code"],
            campaign_name=params["campaign_name"],
            campaign_code_from_business=True, #params["campaign_code_from_business"]
            campaign_id=params["campaign_id"],
            ownerId=ownerId,
            limit=params["limit"],
            mapbox_token=params["mapbox_token"],
            skip_existing=params["skip_existing"],
            geocode=params["geocode"],
        )

        out_path = result["output_path"]
        statistics = result["statistics"]
        processing_end = result.get("processing_end", datetime.now(timezone.utc))

        # Upload output next to input (same folder), with suffix
        out_name = os.path.basename(out_path)
        dest_blob = f"{prefix_dir}/{out_name}" if prefix_dir else out_name
        _upload_blob(bucket, out_path, dest_blob, _content_type_for(out_path))

        print(f"[done] Wrote: gs://{bucket_name}/{dest_blob}")

        # Generate and upload report
        report = generate_upload_report(
            statistics=statistics,
            campaign_info={
                "campaign_id": params["campaign_id"],
                "campaign_name": params["campaign_name"],
                "campaign_code": params["campaign_code"]
            },
            input_file_info={
                "name": os.path.basename(object_name),
                "path": object_name,
                "total_rows": statistics.get("total_rows", 0)
            },
            output_file_path=f"gs://{bucket_name}/{dest_blob}",
            processing_start=result.get("processing_start", processing_start),
            processing_end=processing_end,
            owner_id=ownerId
        )

        # Save report to local file
        report_path = os.path.join("/tmp", "upload_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        # Upload report to GCS
        report_blob_name = f"{prefix_dir}/upload_report.json"
        _upload_blob(bucket, report_path, report_blob_name, "application/json")

        print(f"[report] Uploaded report: gs://{bucket_name}/{report_blob_name}")

        # Log summary
        print(json.dumps({
            "event": "upload_completed",
            "campaign_id": params["campaign_id"],
            "statistics": {
                "total_rows": statistics.get("total_rows", 0),
                "links_created": statistics.get("created_links", 0),
                "targets_created": statistics.get("created_targets", 0),
                "blacklisted": statistics.get("blacklisted_count", 0),
                "errors": statistics.get("errors", 0),
                "excluded_no_destination": statistics.get("excluded_no_destination", 0)
            },
            "report_path": f"gs://{bucket_name}/{report_blob_name}"
        }, ensure_ascii=False))

    except DuplicateCampaignCodeError as e:
        # Clear, structured logs about the duplicate + cleanup
        print(json.dumps({
            "event": "duplicate_campaign_code",
            "message": str(e),
            "bucket": bucket_name,
            "object": object_name,
            "owner_id": ownerId,
            "campaign_id": params.get("campaign_id"),
            "campaign_code": params.get("campaign_code"),
            "cleanup_prefix": campaign_root_prefix
        }, ensure_ascii=False))

        # Delete the entire campaign folder (manifest, CSV/XLSX, templates, etc.)
        if campaign_root_prefix:
            print(json.dumps({
                "event": "duplicate_cleanup_start",
                "bucket": bucket_name,
                "prefix": campaign_root_prefix
            }, ensure_ascii=False))

            blobs = list(bucket.list_blobs(prefix=campaign_root_prefix))
            deleted = 0
            for b in blobs:
                try:
                    b.delete()
                    deleted += 1
                except Exception as del_err:
                    print(json.dumps({
                        "event": "duplicate_cleanup_warning",
                        "prefix": campaign_root_prefix,
                        "blob": b.name,
                        "error": str(del_err)
                    }, ensure_ascii=False))

            print(json.dumps({
                "event": "duplicate_cleanup_done",
                "bucket": bucket_name,
                "prefix": campaign_root_prefix,
                "deleted": deleted,
                "listed": len(blobs)
            }, ensure_ascii=False))
        else:
            print(json.dumps({
                "event": "duplicate_cleanup_skipped_no_prefix",
                "bucket": bucket_name,
                "object": object_name
            }, ensure_ascii=False))

        # Re-raise so the invocation is marked failed and the error appears in logs
        raise
    
    except Exception as e:
        # Generate error report for other exceptions
        processing_end = datetime.now(timezone.utc)
        try:
            error_report = {
                "status": "failed",
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": processing_end.isoformat(),
                "input_file": {
                    "name": os.path.basename(object_name),
                    "path": object_name
                },
                "campaign": {
                    "campaign_id": params.get("campaign_id"),
                    "campaign_name": params.get("campaign_name"),
                    "campaign_code": params.get("campaign_code")
                },
                "processing": {
                    "started_at": processing_start.isoformat(),
                    "failed_at": processing_end.isoformat(),
                    "duration_seconds": round((processing_end - processing_start).total_seconds(), 2)
                },
                "owner_id": ownerId
            }
            
            report_path = os.path.join("/tmp", "upload_report_error.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(error_report, f, indent=2, ensure_ascii=False)
            
            report_blob_name = f"{prefix_dir}/upload_report_error.json"
            _upload_blob(bucket, report_path, report_blob_name, "application/json")
            print(f"[report] Uploaded error report: gs://{bucket_name}/{report_blob_name}")
        except Exception as report_error:
            print(f"[warn] Failed to generate error report: {report_error}")
        
        # Re-raise the original exception
        raise
