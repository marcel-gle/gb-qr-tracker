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
import urllib.parse
from typing import Optional, Tuple, List, Set, Dict, Iterable
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import firestore
from google.cloud.firestore_v1 import ArrayUnion
from google.api_core.exceptions import AlreadyExists
import functions_framework  # <- add this import
from concurrent.futures import ThreadPoolExecutor, as_completed

# #region agent log helper
def _agent_log(hypothesis_id: str, location: str, message: str, data: Dict):
    """
    Lightweight debug logger for agent-driven debugging.
    Writes NDJSON lines to .cursor/debug.log inside the workspace.
    """
    payload = {
        "sessionId": "debug-session",
        "runId": "run1",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data or {},
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    }
    try:
        log_path = "/Users/marcelgleich/Desktop/Software/gb-qr-tracker/.cursor/debug.log"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # Never let logging break the function
        pass
# #endregion agent log helper

import tldextract



# ---------------------------
# Config (env vars with sensible defaults)
# ---------------------------
COMMON_EMAIL_PROVIDERS = {
    "gmail", "gmx", "aol", "yahoo", "hotmail", "outlook",
    "icloud", "t-online", "web", "live", "msn", "mail"
}

# Link IDs that must not be used (common scanner/vulnerability probe targets)
DISALLOWED_LINK_IDS = frozenset({
    "wordpress", "wp-admin", "wp-login", "wp-content", "wp-includes", "xmlrpc",
    "stripe", "admin", "administrator", "login", "signin", "config", "env", "phpmyadmin",
    "api", "git",
})

PROJECT_ID  = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")
DEFAULT_BASE_URL = os.environ.get("BASE_URL")                   # optional fallback
DEFAULT_MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN")          # optional fallback

# Instantiate Firestore client once (outside handler)
db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
_agent_log(
    hypothesis_id="H1",
    location="upload_processor:firestore_client",
    message="Firestore client initialized",
    data={"project_id": PROJECT_ID, "database_id": DATABASE_ID},
)
COL_LINKS = db.collection('links')
COL_BUSINESSES = db.collection('businesses')
COL_CAMPAIGNS = db.collection('campaigns')

CUSTOMER_DOMAINS_COLLECTION = "customer_domains"


def resolve_tenant_id_for_upload(
    manifest_tenant: Optional[str],
    tracking_url_prefix: Optional[str],
) -> Optional[str]:
    """
    Prefer explicit manifest tenant_id; else map tracking_url_prefix hostname -> customer_domains/{host}.tenant_id.
    """
    if manifest_tenant and str(manifest_tenant).strip():
        return str(manifest_tenant).strip()
    if not tracking_url_prefix or not str(tracking_url_prefix).strip():
        return None
    raw = str(tracking_url_prefix).strip()
    if "://" not in raw:
        raw = f"https://{raw}"
    p = urllib.parse.urlparse(raw)
    host = (p.hostname or "").lower()
    if not host:
        return None
    snap = db.collection(CUSTOMER_DOMAINS_COLLECTION).document(host).get()
    if not snap.exists:
        return None
    tid = (snap.to_dict() or {}).get("tenant_id")
    if isinstance(tid, str) and tid.strip():
        return tid.strip()
    return None


def _hostname_from_tracking_url_prefix(tracking_url_prefix: Optional[str]) -> Optional[str]:
    if not tracking_url_prefix or not str(tracking_url_prefix).strip():
        return None
    raw = str(tracking_url_prefix).strip()
    if "://" not in raw:
        raw = f"https://{raw}"
    p = urllib.parse.urlparse(raw)
    h = (p.hostname or "").lower()
    return h or None


def _coerce_manifest_allowed_hosts(val) -> list[str]:
    if val is None:
        return []
    if isinstance(val, str):
        return [s.strip() for s in val.split(",") if s.strip()]
    if isinstance(val, list):
        return [str(s).strip() for s in val if s is not None and str(s).strip()]
    return []


def _normalize_allowed_host_entry(h: str) -> str:
    s = (h or "").strip()
    if not s:
        return ""
    if "://" in s:
        p = urllib.parse.urlparse(s)
        return (p.hostname or "").lower()
    return s.split(":", 1)[0].strip().lower()


def resolve_allowed_hosts_for_upload(
    manifest_allowed_hosts,
    tracking_url_prefix: Optional[str],
) -> list[str]:
    """
    Build links.allowed_hosts: manifest list + tracking_url_prefix host if that host is a shared customer_domains doc.
    """
    out: list[str] = []
    for x in _coerce_manifest_allowed_hosts(manifest_allowed_hosts):
        nh = _normalize_allowed_host_entry(x)
        if nh and nh not in out:
            out.append(nh)
    th = _hostname_from_tracking_url_prefix(tracking_url_prefix)
    if th:
        snap = db.collection(CUSTOMER_DOMAINS_COLLECTION).document(th).get()
        if snap.exists and bool((snap.to_dict() or {}).get("shared")):
            if th not in out:
                out.append(th)
    return out


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

_TENANT_POOL_RE = re.compile(r"^[a-z0-9-]{1,40}$")


def tenant_pool(tenant_id: Optional[str]) -> Optional[str]:
    """Pool name used in links/{pool}__{code}. None when the value cannot be a pool."""
    if not isinstance(tenant_id, str):
        return None
    pool = tenant_id.strip().lower()
    if not pool or not _TENANT_POOL_RE.fullmatch(pool):
        return None
    return pool


def link_document_id(tenant_id: Optional[str], short_code: str) -> str:
    """Firestore id for a short code. Unscoped codes stay links/{short_code}."""
    pool = tenant_pool(tenant_id)
    if not pool:
        return short_code
    return f"{pool}__{short_code}"


# Host printed on the letter when the upload has no tracking_url_prefix.
DEFAULT_TRACKING_HOST = "ihr-brief.de"


class TenantDomainMismatchError(RuntimeError):
    pass


def printed_host_pool(tenant_id: Optional[str], shared: bool) -> Optional[str]:
    """
    Pool the redirector opens first on this domain. Shared domains have none.
    Must match redirector tenant_utils.domain_link_pool.
    """
    if shared:
        return None
    return tenant_pool(tenant_id)


def resolve_link_pool(manifest_tenant: Optional[str], tracking_url_prefix: Optional[str]) -> Optional[str]:
    """
    Pool for this upload's link ids, taken from the domain printed on the letter.

    The Worker looks a code up under that domain's pool, so a link stored in
    any other pool would never be found. A shared or unknown domain gives None
    (bare links/{code}, as before). A manifest tenant that names a different
    tenant than a dedicated printed domain raises instead of printing letters
    that cannot resolve.
    """
    host = _hostname_from_tracking_url_prefix(tracking_url_prefix) or DEFAULT_TRACKING_HOST
    snap = db.collection(CUSTOMER_DOMAINS_COLLECTION).document(host).get()
    if not snap.exists:
        return None
    data = snap.to_dict() or {}
    pool = printed_host_pool(data.get("tenant_id"), bool(data.get("shared")))
    if pool and manifest_tenant and str(manifest_tenant).strip() and tenant_pool(str(manifest_tenant)) != pool:
        raise TenantDomainMismatchError(
            f"tenant_id {manifest_tenant!r} does not match {host!r}, which belongs to tenant {pool!r}"
        )
    return pool


def build_tracking_link(base_url: str, short_code: str, tenant_id: Optional[str] = None) -> str:
    """QR target. Adds &tenant= only when the link was stored under that pool."""
    url = f"{base_url.rstrip('/')}/?id={short_code}"
    pool = tenant_pool(tenant_id)
    if pool:
        return f"{url}&tenant={pool}"
    return url

def build_tracking_url(tracking_url_prefix: Optional[str], final_id: str) -> str:
    """
    Build the printable tracking URL shown in the export.

    - If `tracking_url_prefix` is provided (e.g. "https://example.com/track"),
      we generate "<prefix>/<final_id>".
    - If not provided, we default to "ihr-brief.de/<final_id>".
    """
    if tracking_url_prefix:
        return f"{tracking_url_prefix.rstrip('/')}/{final_id}"
    return f"{DEFAULT_TRACKING_HOST}/{final_id}"

def sanitize_id(value: str) -> str:
    if value is None:
        return ""
    v = str(value).strip()
    # Allow A-Z, a-z, 0-9, and German umlauts (ä, ö, ü, ß)
    # Replace everything else with hyphens
    v = re.sub(r"[^A-Za-z0-9äöüÄÖÜß]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    # Normalize to lowercase for consistency
    v = v.lower()
    return v

def remove_tld_suffix(domain_id: str) -> str:
    """
    Remove common TLD suffixes from the end of a domain-based ID.
    Examples: 'example-com' -> 'example', 'test-de' -> 'test'
    """
    if not domain_id:
        return domain_id
    
    # Common TLD suffixes to remove (case-insensitive)
    # Includes country codes and generic TLDs
    tld_suffixes = [
        '-de', '-com', '-net', '-org', '-io', '-co', '-uk', '-fr', '-it', '-es',
        '-nl', '-be', '-at', '-ch', '-pl', '-cz', '-dk', '-se', '-no', '-fi',
        '-au', '-ca', '-jp', '-cn', '-in', '-br', '-mx', '-ru', '-kr', '-tw',
        '-info', '-biz', '-name', '-pro', '-mobi', '-tel', '-asia', '-jobs',
        '-edu', '-gov', '-mil', '-int', '-aero', '-museum', '-travel'
    ]
    
    domain_id_lower = domain_id.lower()
    for suffix in tld_suffixes:
        if domain_id_lower.endswith(suffix):
            # Remove the suffix and any trailing dashes
            result = domain_id[:-len(suffix)].rstrip('-')
            return result if result else domain_id  # Don't return empty string
    
    return domain_id


def _normalize_domain_for_id(value: Optional[str]) -> str:
    """
    Normalize a Domain/URL-like value into a short, clean slug for use as base_id.

    Examples:
    - "https://bildungundberuf.com/kursliste?utm_source=kursnet" -> "bildungundberuf"
    - "https://www.hz-bb.de" -> "hz-bb"
    - "http://alfatraining.de/" -> "alfatraining"
    - "www.weiterbildungsinstitut.de/path" -> "weiterbildungsinstitut"

    Strategy:
    - Try to parse as URL, extract hostname, strip leading "www.".
    - Use tldextract to get the registrable domain core when possible.
    - Fall back to sanitizing the raw value.
    - In all cases, run through remove_tld_suffix to strip trailing TLD parts like "-de", "-com".
    """
    if not value:
        return ""

    raw = str(value).strip().lower()
    if not raw:
        return ""

    host_candidate = None
    text = raw

    # Heuristic: if there is no scheme, prepend http:// so urlparse can extract netloc
    if "://" not in text:
        # Handle typical "www.example.de/..." or "example.de/path"
        if text.startswith("www.") or "." in text or "/" in text:
            text = f"http://{text}"

    try:
        parsed = urllib.parse.urlparse(text)
    except Exception:
        parsed = None

    if parsed:
        netloc = parsed.netloc or ""
        path = parsed.path or ""

        # Prefer netloc; if missing (e.g. "example.de/path" without scheme), fall back to path
        if netloc:
            host_candidate = netloc
        elif path:
            # Take first path segment as host-like component
            host_candidate = path.split("/", 1)[0]

    if host_candidate:
        # Strip port if any
        host_candidate = host_candidate.split(":", 1)[0]
        # Strip leading www.
        if host_candidate.startswith("www."):
            host_candidate = host_candidate[4:]

        # Use tldextract to get the registrable domain core when possible
        ext = tldextract.extract(host_candidate)
        core = ext.domain or host_candidate
        slug = sanitize_id(core)
        return remove_tld_suffix(slug)

    # Fallback: sanitize the raw string and strip common TLD suffixes
    slug = sanitize_id(raw)
    return remove_tld_suffix(slug)


# Highest numeric prefix written for a colliding base (`100-beispiel`). The bare
# base id does not count. The next collision after that raises.
MAX_LINK_ID_NUMBER = 100


def _legacy_short_code_reserved(short_code: str, pool: str) -> bool:
    """
    True when links/{short_code} exists and is not another tenant's.

    A new links/{pool}__{short_code} would be opened first on this pool's
    domain and would hide an already printed letter with that code. Links
    without a tenant_id may have been printed anywhere, so they count as taken.
    Only a bare document with another tenant's id leaves the code free.
    """
    snap = COL_LINKS.document(short_code).get()
    if not snap.exists:
        return False
    data = snap.to_dict() or {}
    legacy_tenant = data.get("tenant_id")
    if not isinstance(legacy_tenant, str) or not legacy_tenant.strip():
        return True
    return tenant_pool(legacy_tenant) == pool


def create_unique_link(
    base_id: str,
    payload: dict,
    next_n_by_base: Dict[str, int],
    tenant_id: Optional[str] = None,
) -> str:
    """
    Create a link document and return the printed short code.

    Without a usable tenant_id this stores links/{short_code}, as before.
    With one it stores links/{tenant}__{short_code}. Collisions become
    `{n}-{base_id}` inside that same tenant. A bare links/{short_code} that
    already has this tenant_id also counts as taken.

    `create()` is the occupancy check, so two uploads cannot claim the same id.
    `next_n_by_base` remembers the next n for this upload, so later rows of the
    same base skip ids already created in this run. Raises once n would pass
    MAX_LINK_ID_NUMBER.
    """
    base_id = (base_id or "").strip() or "link"
    pool = tenant_pool(tenant_id)
    if tenant_id and not pool:
        print(f"[warn] Ignoring unusable tenant_id {tenant_id!r}; creating a global link id")
    counter_key = f"{pool}__{base_id}" if pool else base_id
    n = next_n_by_base.get(counter_key, 0)

    while n <= MAX_LINK_ID_NUMBER:
        short_code = base_id if n == 0 else f"{n}-{base_id}"
        doc_id = link_document_id(pool, short_code)
        if pool and _legacy_short_code_reserved(short_code, pool):
            print(f"[warn] Legacy link '{short_code}' is not another tenant's, trying next prefix")
            n += 1
            continue
        try:
            print(f"Creating link with ID: {doc_id}")
            body = {**payload, "short_code": short_code}
            if pool:
                body.setdefault("tenant_id", pool)
            COL_LINKS.document(doc_id).create(body)
            next_n_by_base[counter_key] = n + 1
            return short_code
        except AlreadyExists:
            print(f"[warn] Link ID collision for '{doc_id}', trying next prefix")
            n += 1

    raise RuntimeError(
        f"no free link id for base {base_id!r} after {MAX_LINK_ID_NUMBER} numbered variants"
    )


def template_with_qr_suffix(template: Optional[str]) -> Optional[str]:
    if not template:
        return None
    s = str(template).strip()
    if not s:
        return None
    if s.lower().endswith(".pdf"):
        return s
    return f"{s}.pdf"

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

def get_ci_nonempty(row: dict, *names: str) -> Optional[str]:
    """Like get_ci, but skips columns that exist with an empty value."""
    for name in names:
        val = get_ci(row, name)
        if val is not None and str(val).strip():
            return val
    return None

APOLLO_MAX_SLOTS = 3
APOLLO_EMAIL_OK_STATUSES = {"verified"}

def apollo_contact_from_row(row: dict) -> Dict[str, str]:
    """
    Personal contact of the first managing director with an Apollo phone or
    verified Apollo email. Returns only non-empty keys so a re-upload without
    Apollo data never wipes an existing overlay via merge=True.
    """
    for i in range(1, APOLLO_MAX_SLOTS + 1):
        phone = (get_ci(row, f'apollo_phone_{i}') or '').strip()
        email = (get_ci(row, f'apollo_email_{i}') or '').strip()
        email_status = (get_ci(row, f'apollo_email_status_{i}') or '').strip().lower()
        # Old final CSVs carry no status column; Apollo emails there are kept as-is.
        if email and get_ci_key(row, f'apollo_email_status_{i}') and email_status not in APOLLO_EMAIL_OK_STATUSES:
            email = ''
        if not phone and not email:
            continue
        name = " ".join(
            p.strip() for p in [get_ci(row, f'first_name_{i}'), get_ci(row, f'last_name_{i}')]
            if p and p.strip()
        )
        out = {
            "contact_phone": phone,
            "contact_phone_type": (get_ci(row, f'apollo_phone_type_{i}') or '').strip(),
            "contact_email": email,
            "contact_name": name,
            "contact_salutation": (get_ci(row, f'salutation_{i}') or '').strip(),
            "contact_title": (get_ci(row, f'apollo_title_{i}') or '').strip(),
            "contact_linkedin_url": (get_ci(row, f'apollo_linkedin_url_{i}') or '').strip(),
        }
        out = {k: v for k, v in out.items() if v}
        out["contact_source"] = "apollo"
        return out
    return {}

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


PARAGRAPH_COLUMN = "Paragraph"
# First non-empty alias wins. Output and Firestore always use Paragraph / paragraph.
PARAGRAPH_ALIASES = ("Paragraph", "paragraph", "absatz", "Absatz")
_PARAGRAPH_ALIAS_KEYS = {name.lower() for name in PARAGRAPH_ALIASES}


def paragraph_from_row(row: dict) -> str:
    """Letter body from Paragraph or absatz. Empty when the column is absent or blank."""
    return (get_ci_nonempty(row, *PARAGRAPH_ALIASES) or "").strip()


def normalize_paragraph_column(row: dict) -> Optional[str]:
    """
    Collapse Paragraph/absatz aliases onto a single Paragraph column.
    Returns the text, or None when the row has no such column.
    """
    if get_ci_key(row, *PARAGRAPH_ALIASES) is None:
        return None
    text = paragraph_from_row(row)
    for key in list(row.keys()):
        if isinstance(key, str) and key.lower() in _PARAGRAPH_ALIAS_KEYS:
            row.pop(key, None)
    row[PARAGRAPH_COLUMN] = text
    return text


def paragraph_field_for_target(row: dict) -> Optional[str]:
    """Campaign-target value. None when the column is absent or blank."""
    if PARAGRAPH_COLUMN not in row and get_ci_key(row, *PARAGRAPH_ALIASES) is None:
        return None
    text = row.get(PARAGRAPH_COLUMN) if PARAGRAPH_COLUMN in row else paragraph_from_row(row)
    text = (text or "").strip()
    return text or None


def csv_fieldnames_union(rows: List[dict]) -> List[str]:
    """
    Compute CSV headers from a list of dict rows, ensuring our tracking columns
    appear at the end in a stable order.
    """
    tracking_cols = ["tracking_link", "tracking_url", "tracking_id"]
    if not rows:
        return tracking_cols
    seen: Set[str] = set()
    ordered: List[str] = []
    for k in rows[0].keys():
        if k not in seen and k not in tracking_cols:
            ordered.append(k); seen.add(k)
    for r in rows[1:]:
        for k in r.keys():
            if k not in seen and k not in tracking_cols:
                ordered.append(k); seen.add(k)
    # Append tracking columns at the end (whether or not they appeared in rows)
    ordered.extend(tracking_cols)
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
    street = get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.', 'street', 'Street')
    house_no = get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr', 'house_number', 'House number', 'house number')
    plz = get_ci(row, 'PLZ', 'Postleitzahl', 'postcode', 'Postcode')
    city = get_ci(row, 'Ort', 'Stadt', 'City')
    country = get_ci(row, 'Country', 'Land') or "DE"
    address_lines = []
    if street or house_no:
        line1 = " ".join([p for p in [street, house_no] if p])
        if line1:
            address_lines.append(line1)
    mailing = {
        "business_name": (
            get_ci(row, 'Namenszeile')
            or get_ci(row, 'business_name', 'company', 'company_name')
            or fallback_business_name
        ),
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
    name = (
        get_ci(row, 'Namenszeile')
        or get_ci(row, 'business_name', 'company', 'company_name')
        or ''
    ).lower().strip()
    street = (get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.', 'street', 'Street') or '').lower().strip().replace('ß', 'ss')
    house = (get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr', 'house_number', 'House number', 'house number') or '').lower().strip()
    plz = (get_ci(row, 'PLZ', 'Postleitzahl', 'postcode', 'Postcode') or '').lower().strip()
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

    _agent_log(
        hypothesis_id="H2",
        location="upload_processor:get_or_create_campaign:entry",
        message="Entering get_or_create_campaign",
        data={"owner_id": owner_id, "campaign_id": campaign_id, "code": code},
    )

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
        _agent_log(
            hypothesis_id="H2",
            location="upload_processor:get_or_create_campaign",
            message="Created new campaign document",
            data={"campaign_id": campaign_id, "owner_id": owner_id, "code_norm": code_norm},
        )
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
        _agent_log(
            hypothesis_id="H2",
            location="upload_processor:get_or_create_campaign",
            message="Using existing campaign document",
            data={"campaign_id": campaign_id, "owner_id": owner_id, "existing_code": existing_code},
        )

    return ref



def get_or_create_campaign_old(owner_id: str, #DEPRECATED, can beremoved
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
                                     coordinate: Optional[Dict[str, float]]) -> Tuple[str, Dict, Dict]:
    """
    Split business data into canonical and customer-specific payloads.
    Returns: (biz_id, canonical_payload, customer_payload)
    """
    business_name = get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company', 'company_name')
    street = get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.', 'street', 'Street')
    house_no = get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr', 'house_number', 'House number', 'house number')
    plz = get_ci(row, 'PLZ', 'Postleitzahl', 'postcode', 'Postcode')
    city = get_ci(row, 'Ort', 'Stadt', 'City')
    fname = get_ci_nonempty(row, 'Entscheider 1 Vorname', 'Vorname', 'Anrede Vorname', 'first_name', 'first_name_1', 'First name', 'first name')
    lname = get_ci_nonempty(row, 'Entscheider 1 Nachname', 'Nachname', 'last_name', 'last_name_1', 'Last name', 'last name')
    prefix_tel = get_ci(row, 'Vorwahl Telefon', 'Vorwahl', 'Telefon Vorwahl')
    tel = get_ci(row, 'Telefonnummer', 'Telefon', 'Phone', 'Tel.', 'Tel')
    email = get_ci(row, 'E-Mail-Adresse', 'Email', 'E-Mail', 'Mail')
    salutation = get_ci_nonempty(row, 'Entscheider 1 Anrede', 'Salutation', 'salutation_1')

    contact_name = " ".join(p.strip() for p in [fname, lname] if p and p.strip())
    phone = " ".join(p for p in [prefix_tel, tel] if p)
    full_addr = compose_full_address(street, house_no, plz, city, "Germany")

    # Additional business attributes from new list format
    business_description = get_ci(row, 'Gegenstand', 'business_description')
    revenue_eur = get_ci(row, 'Umsatz EUR', 'revenue_eur', 'umsatz')
    business_code = get_ci(row, 'Branche (NACE)', 'business_code', 'branchencode', 'Branchencode')

    biz_id = make_business_id(business_name, plz)
    # Guard against empty business IDs which would lead to invalid Firestore paths.
    if not biz_id:
        fallback = sanitize_id(business_name or "") or sanitize_id(plz or "") or "unknown-business"
        print(f"[warn] Empty biz_id computed, using fallback: '{fallback}'")
        biz_id = fallback
    
    # Canonical payload (shared across customers)
    canonical_payload = {
        "business_name": business_name,
        "street": street,
        "house_number": house_no,
        "postcode": plz,
        "city": city,
        "address": full_addr or None,
        "business_description": business_description or None,
        "revenue_eur": revenue_eur or None,
        "business_code": business_code or None,
        "business_id": biz_id,  # Store normalized business_id in document
    }
    if coordinate:
        canonical_payload["coordinate"] = coordinate
    
    # Customer-specific payload (per-customer overlay)
    customer_payload = {
        "phone": phone or None,
        "email": email or None,
        "name": contact_name or None,
        "salutation": salutation or None,
        "hit_count": 0,
        "last_hit_at": None,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }
    customer_payload.update(apollo_contact_from_row(row))

    return biz_id, canonical_payload, customer_payload

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
    # Ensure tracking columns exist and are placed at the end
    tracking_cols = ["tracking_link", "tracking_url", "tracking_id"]
    for col in tracking_cols:
        if col not in df.columns:
            df[col] = ""
    cols = [c for c in df.columns if c not in tracking_cols] + tracking_cols
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
    Assign a provisional final_id for each item based on its base_id.

    We no longer pre-scan Firestore for existing variants here, to avoid
    long-running range queries. Instead we:
    - Use base_id (or a 'link' fallback) as the initial final_id.
    - Rely on create_unique_link() to detect collisions via AlreadyExists
      and allocate `{n}-{base_id}` (number at the front).
    This keeps the function responsive even when Firestore scans are slow.
    """
    total_items = len(precomputed)
    print(f"[DEBUG assign_final_ids] total_items={total_items}")

    for idx, item in enumerate(precomputed):
        if not (item.get("in_limit") and item.get("dest")):
            # Nothing to assign for skipped/blacklisted/out-of-limit rows
            continue

        base_id = (item.get("base_id") or "").strip()
        if not base_id:
            base_id = "link"
            print(f"[warn] Empty base_id in assign_final_ids at index={idx}, using fallback: 'link'")

        # Use the base_id directly as the initial final_id.
        # create_unique_link() replaces it when that document already exists.
        item["final_id"] = base_id

    print("[DEBUG assign_final_ids] finished assigning provisional final_ids")




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
                                    geocode: bool = True,
                                    tracking_url_prefix: Optional[str] = None,
                                    tenant_id: Optional[str] = None,
                                    allowed_hosts: Optional[List[str]] = None,
                                    link_pool: Optional[str] = None):
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
        # CSV Helpers
        def _open_text(path: str):
            # Handle BOM + normalize newlines
            return open(path, "r", encoding="utf-8-sig", newline="")

        def _detect_delimiter(sample: str) -> str:
            """
            Detect CSV delimiter by choosing the character that yields the most
            columns on the first non-empty line. This is robust for semicolon-
            separated exports where fields may contain many commas (e.g. JSON).
            """
            if not sample:
                return ","

            first_line = ""
            for line in sample.splitlines():
                if line.strip():
                    first_line = line
                    break

            if not first_line:
                return ","

            best_delimiter = ","
            best_count = 1
            for delim in (";", "\t", ",", "|"):
                count = len(first_line.split(delim))
                if count > best_count:
                    best_count = count
                    best_delimiter = delim

            return best_delimiter

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
    _agent_log(
        hypothesis_id="H3",
        location="upload_processor:assign_links_from_business_file:campaign_ref",
        message="Campaign reference resolved",
        data={"campaign_ref_id": campaign_ref.id, "owner_id": ownerId, "campaign_code": campaign_code},
    )

    geo_cache: Dict[str, Dict] = {}

    def maybe_geocode(row: dict) -> Optional[Dict]:
        if not geocode or not mapbox_token:
            return None
        street = get_ci(row, 'Straße', 'Strasse', 'Str', 'Str.', 'street', 'Street')
        house_no = get_ci(row, 'Hausnummer', 'HNr', 'Hnr', 'Nr', 'house_number', 'House number', 'house number')
        plz = get_ci(row, 'PLZ', 'Postleitzahl', 'postcode', 'Postcode')
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
        normalize_paragraph_column(row)
        in_limit = (limit <= 0) or (i < limit)
        
        # Check if business is blacklisted BEFORE other processing
        business_name = get_ci(row, 'Namenszeile') or get_ci(row, 'business_name', 'company', 'company_name')
        plz = get_ci(row, 'PLZ', 'Postleitzahl', 'postcode', 'Postcode')
        biz_id = make_business_id(business_name, plz)
        print("DEBUG biz_id:", biz_id)
        
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

        print("DEBUG campaign_code_from_business:", campaign_code_from_business)
        print("DEBUG doc_id_from_row:", doc_id_from_row)
        print("DEBUG business_name:", business_name)
        print("DEBUG campaign_code:", campaign_code)
        print("DEBUG i:", i)
        print("DEBUG destination:", destination)
        print("DEBUG template_key:", template_key)
        print("DEBUG template_raw:", template_raw)

        # Check for explicit "Domain" column first - this takes priority
        domain_from_row = get_ci(row, 'Domain', 'domain')
        print("DEBUG domain_from_row:", domain_from_row)
        
        using_domain_column = False
        if domain_from_row and domain_from_row.strip():
            # Use Domain column as base_id if present, but normalize URL/domain first
            base_id = _normalize_domain_for_id(domain_from_row)
            using_domain_column = True
            print("DEBUG using Domain column as base_id:", base_id)
        elif campaign_code_from_business:
            email = get_ci(row, 'E-Mail-Adresse', 'Email', 'E-Mail', 'Mail')
            print("DEBUG email:", email)
            if email and not _is_common_provider(email):
                # Business email - use domain
                print("DEBUG email (business):", email)
                base_id = _extract_registrable_domain(email)
                using_domain_column = True  # Also remove TLD from email-extracted domains
            else:
                # No usable business email → fallback to website/domain field if available
                website_val = get_ci(row, 'website', 'Website')
                print("DEBUG website_val:", website_val)
                if website_val and website_val.strip():
                    base_id = _normalize_domain_for_id(website_val)
                    using_domain_column = True
                    print("DEBUG using website as base_id:", base_id)
                elif not email:
                    # No email and no website/domain → clean business name
                    print("DEBUG business_name (no email/website):", business_name)
                    base_id = _extract_clean_business_name(business_name)
                else:
                    # Email is common provider and no website → fall back to sequential/campaign-based ID
                    base_id = doc_id_from_row or f"{(campaign_code or 'L').upper()}-{i+1}"
        else:
            base_id = doc_id_from_row or business_name or f"{(campaign_code or 'L').upper()}-{i+1}"

        base_id = sanitize_id(base_id or "")
        
        # Remove TLD suffixes from domain-based IDs
        if using_domain_column and base_id:
            base_id_before = base_id
            base_id = remove_tld_suffix(base_id)
            if base_id_before != base_id:
                print(f"DEBUG removed TLD suffix: '{base_id_before}' -> '{base_id}'")

        print("DEBUG base_id:", base_id)

        # Replace disallowed link IDs (scanner-favorite names) with a unique fallback
        if base_id and base_id.lower() in DISALLOWED_LINK_IDS:
            base_id = sanitize_id(f"{(campaign_code or 'L').upper()}-{i+1}")
            print(f"[disallowed] Replaced disallowed link ID for row {i+1}: {base_id}")

        if not base_id:
            print("⚠️ [DEBUG EMPTY BASE_ID] row:", i+1)
            print("    business_name:", repr(business_name))
            print("    email:", repr(email))
            print("    doc_id_from_row:", repr(doc_id_from_row))
            print("    original base:", repr(orig_base if 'orig_base' in locals() else None))
            print("    after sanitize:", repr(base_id))
            print("    Using campaign_code:", campaign_code)
            print("    FULL ROW:", row)

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

    print(f"[DEBUG assign_links_from_business_file] built precomputed list, len={len(precomputed)} - BEFORE assign_final_ids")
    assign_final_ids(precomputed)
    print("[DEBUG assign_links_from_business_file] assign_final_ids finished")

    print("Precomputed", precomputed[:3])
    print("Len precomputed:", len(precomputed))

    batch = db.batch()
    ops = 0
    def flush():
        nonlocal batch, ops
        if ops:
            try:
                batch.commit()
                print(f"[batch] Committed {ops} operations successfully")
            except Exception as e:
                print(f"[ERROR] Batch commit failed: {e}")
                print(f"[ERROR] This batch had {ops} operations")
                raise  # Re-raise to be caught by outer handler
            finally:
                batch = db.batch()
                ops = 0

    # Next numeric prefix to try per base id, for this upload only.
    next_n_by_base: Dict[str, int] = {}

    pbar = tqdm(precomputed, desc="Processing rows", unit="row")
    try:
        for item_idx, item in enumerate(pbar):
            row = item["row"]
            if not item["in_limit"]:
                row['tracking_link'] = ''
                continue

            dest = item.get("dest")
            final_id = item.get("final_id")    # <- use the ID allocated earlier
            business_name = item.get("business_name")
            template_raw = item.get("template_raw")
            template_key = item.get("template_key")

            # Validate final_id before proceeding - prevent empty final_id errors
            if dest and (not final_id or not final_id.strip()):
                print(f"[ERROR] Empty final_id detected! Row data:")
                print(f"  - dest: {dest}")
                print(f"  - final_id: '{final_id}' (type: {type(final_id)}, len: {len(final_id) if final_id else 0})")
                print(f"  - base_id: {item.get('base_id')}")
                print(f"  - business_name: {business_name}")
                print(f"  - row_index: {item_idx + 1}")
                # Set dest to None to skip link creation for this row
                dest = None
                item["dest"] = None
                errors += 1
                error_details.append({
                    "row_number": item_idx + 1,
                    "business_name": business_name or "Unknown",
                    "error": "Empty final_id detected - link creation skipped",
                    "error_type": "ValidationError"
                })

            print("DEBUG final_id:", final_id)

            try:
                coordinate = maybe_geocode(row)
                # Track geocoding stats
                if geocode:
                    if coordinate:
                        geocoding_successful += 1
                    else:
                        geocoding_failed += 1
                
                biz_id, canonical_payload, customer_payload = upsert_business_payload_from_row(row, ownerId, coordinate)
                biz_ref = COL_BUSINESSES.document(biz_id)
                current_biz_id = biz_id  # Store for error tracking

                # Upsert canonical business document (always, for address data)
                batch.set(biz_ref, {**canonical_payload, "created_at": firestore.SERVER_TIMESTAMP}, merge=True); ops += 1

                # target + link handling
                target_ref = campaign_ref.collection('targets').document()
                status = "validated" if dest else "excluded"
                snapshot = snapshot_mailing_from_row(row, business_name)

                # Link reference will be filled after successful link creation (if any)
                link_ref = None

                # Only create links, update ownerIds, and create customer overlay if destination exists.
                # This prevents creating overlays for businesses without tracking links.
                if dest and final_id and final_id.strip():
                    # Common payload shared between the base ID and any numeric prefix.
                    base_link_payload = {
                        "campaign_ref": campaign_ref,
                        "business_ref": biz_ref,
                        "target_ref": target_ref,
                        "destination": dest,
                        "template_id": template_with_qr_suffix(template_raw),
                        "active": True,
                        "hit_count": 0,
                        "created_at": firestore.SERVER_TIMESTAMP,
                        "last_hit_at": None,
                        "owner_id": ownerId,
                        "snapshot_mailing": snapshot,
                        "campaign_name": campaign_name,
                    }
                    if tenant_id:
                        base_link_payload["tenant_id"] = tenant_id
                    if allowed_hosts:
                        base_link_payload["allowed_hosts"] = allowed_hosts

                    # create() raises AlreadyExists immediately, so collisions are resolved
                    # here instead of at batch.commit(). The printed code stays
                    # `beispiel` (or `1-beispiel`). With a link pool (dedicated printed
                    # domain) the document id is `{pool}__{code}`; otherwise the bare code.
                    short_code = create_unique_link(
                        item.get("base_id") or final_id,
                        base_link_payload,
                        next_n_by_base,
                        tenant_id=link_pool,
                    )
                    final_id = short_code
                    link_ref = COL_LINKS.document(link_document_id(link_pool, short_code))
                    created_links += 1

                    # A link doc exists now, so grant the user access to the business
                    # and upsert the customer overlay.
                    batch.set(biz_ref, {"ownerIds": ArrayUnion([ownerId])}, merge=True); ops += 1

                    customer_business_ref = db.collection('customers').document(ownerId).collection('businesses').document(biz_id)
                    batch.set(customer_business_ref, {
                        "business_ref": biz_ref,
                        **customer_payload,
                        # Keep overlay campaign membership in sync on upload writes.
                        "campaign_ids": ArrayUnion([campaign_ref.id]),
                        "campaign_updated_at": firestore.SERVER_TIMESTAMP,
                    }, merge=True); ops += 1

                # Build and persist the target document, now that link_ref/final_id are settled.
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
                paragraph_value = paragraph_field_for_target(row)
                if PARAGRAPH_COLUMN in row:
                    target_payload["paragraph"] = paragraph_value
                batch.set(target_ref, target_payload); ops += 1
                created_targets += 1

                # Track excluded rows (no destination)
                if not dest:
                    excluded_no_destination += 1

                print("DEBUG target_payload:", target_payload)
                print("DEBUG dest:", dest)

                # write back tracking info + template into the row
                if dest and final_id:
                    row['tracking_link'] = build_tracking_link(base_url, final_id, link_pool)
                    row['tracking_url'] = build_tracking_url(tracking_url_prefix, final_id)
                    row['tracking_id'] = final_id
                else:
                    row['tracking_link'] = ''
                    row['tracking_url'] = ''
                    row['tracking_id'] = ''
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
                print("business_name:", business_name)
                print("business_id:", biz_id)
                print("FULL ROW:", row)
                print(f"row_index: {item_idx + 1}")
                errors += 1
                # Track error details
                error_biz_id = None
                try:
                    error_biz_id = current_biz_id if 'current_biz_id' in locals() else None
                except:
                    pass
                error_details.append({
                    "row_number": item_idx + 1,
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
    _agent_log(
        hypothesis_id="H4",
        location="upload_processor:assign_links_from_business_file:summary",
        message="Finished assign_links_from_business_file",
        data={
            "created_links": created_links,
            "created_targets": created_targets,
            "skipped": skipped,
            "errors": errors,
            "blacklisted": blacklisted_count,
            "total_rows": total_rows,
        },
    )
    
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


if __name__ == "__main__":
    # Simple local sanity checks for _normalize_domain_for_id
    samples = [
        "https://bildungundberuf.com/kursliste?utm_source=kursnet&utm_campaign=kursnet",
        "https://www.hz-bb.de",
        "http://alfatraining.de/",
        "https://www.weiterbildungsinstitut.de/path",
        "https-bildungundberuf-com-kursliste-utm-source-kursnet-utm-campaign-kursnet-utm-medium-link-kurs-utm-term-e-4244-kurs-fuer-arbeitssuchende-e-4244",
        "https-www-hz-bb",
        "http-www-weiterbildungsinstitut",
    ]
    print("Sanity check for _normalize_domain_for_id:")
    for s in samples:
        print(f"  {s!r} -> {_normalize_domain_for_id(s)!r}")



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
        # Optional prefix for printable tracking URL in exports.
        # If omitted, we fall back to "ihr-brief.de/<final_id>".
        "tracking_url_prefix": manifest.get("tracking_url_prefix") or metadata.get("tracking_url_prefix"),
        "tenant_id": manifest.get("tenant_id") or metadata.get("tenant_id"),
        "allowed_hosts": manifest.get("allowed_hosts") or metadata.get("allowed_hosts"),
        #use business domain as tracking id 
    }

    # Optional: link a search_group (and all its searches) to this campaign.
    search_group_info = manifest.get("search_group") or {}

    campaign_code = params["campaign_code"]
    if not campaign_code:
        raise RuntimeError("campaign_code is required.")
    params["campaign_code"] = normalize_campaign_code(campaign_code)


    print("PARAMS", params)

    resolved_tenant_id = resolve_tenant_id_for_upload(
        params.get("tenant_id"),
        params.get("tracking_url_prefix"),
    )
    if resolved_tenant_id:
        print(f"[tenant_id] resolved={resolved_tenant_id!r}")
    else:
        print("[tenant_id] not resolved (set manifest.tenant_id or customer_domains/* for tracking_url_prefix host)")

    resolved_allowed_hosts = resolve_allowed_hosts_for_upload(
        params.get("allowed_hosts"),
        params.get("tracking_url_prefix"),
    )
    if resolved_allowed_hosts:
        print(f"[allowed_hosts] resolved={resolved_allowed_hosts!r}")

    link_pool = resolve_link_pool(params.get("tenant_id"), params.get("tracking_url_prefix"))
    print(f"[link_pool] {link_pool!r} (None = bare links/{{code}})")

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
            tracking_url_prefix=params.get("tracking_url_prefix"),
            tenant_id=resolved_tenant_id,
            allowed_hosts=resolved_allowed_hosts or None,
            link_pool=link_pool,
        )

        out_path = result["output_path"]
        statistics = result["statistics"]
        processing_end = result.get("processing_end", datetime.now(timezone.utc))

        _agent_log(
            hypothesis_id="H5",
            location="upload_processor:process_business_upload:after_assign",
            message="assign_links_from_business_file completed",
            data={
                "campaign_id": params.get("campaign_id"),
                "ownerId": ownerId,
                "stats": {
                    "created_links": statistics.get("created_links"),
                    "created_targets": statistics.get("created_targets"),
                    "errors": statistics.get("errors"),
                },
                "out_path": out_path,
            },
        )

        # If a search_group was provided in the manifest, link it (and all of its
        # searches) to this campaign by setting campaign_id / campaign_ref once.
        sg_id = search_group_info.get("id")
        if sg_id and ownerId and params.get("campaign_id"):
            try:
                campaign_ref = COL_CAMPAIGNS.document(params["campaign_id"])
                customer_ref = db.collection("customers").document(ownerId)
                sg_ref = customer_ref.collection("search_groups").document(sg_id)

                # Link the search_group document itself
                sg_ref.set(
                    {
                        "campaign_id": params["campaign_id"],
                        "campaign_ref": campaign_ref,
                    },
                    merge=True,
                )

                # Link all searches under this group that are not yet linked
                searches_col = sg_ref.collection("searches")
                for search_doc in searches_col.stream():
                    data_doc = search_doc.to_dict() or {}
                    if not data_doc.get("campaign_id") and not data_doc.get("campaign_ref"):
                        search_doc.reference.set(
                            {
                                "campaign_id": params["campaign_id"],
                                "campaign_ref": campaign_ref,
                            },
                            merge=True,
                        )
                print(f"[search_group] Linked search_group '{sg_id}' and its searches to campaign {params['campaign_id']}")
            except Exception as link_err:
                # Don't fail the whole upload if linking the search_group fails.
                print(f"[search_group] Failed to link search_group '{sg_id}' for owner '{ownerId}': {link_err}")

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
