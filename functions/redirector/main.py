# main.py
# Google Cloud Functions (Gen2) - Redirector with analytics:
# - Aggregated counts (links), optional business aggregates (businesses)
# - Campaign & template on links
# - Per-hit logs (hits): timestamp, user-agent parsing, referer
# - NEW: Optional IP geolocation (country/region/city/lat/lon) + optional salted IP hash
#
# Configure via env vars:
#   HIT_TTL_DAYS=30               # optional TTL for hits (adds expires_at)
#   GEOIP_DB_PATH=/workspace/GeoLite2-City.mmdb   # optional local MaxMind db path
#   GEOIP_API_URL=https://ipapi.co/{ip}/json/     # optional external API template
#   STORE_IP_HASH=1               # if set to "1", store SHA256(salt+ip) in ip_hash
#   IP_HASH_SALT=some-random-salt # salt used for hashing, required if STORE_IP_HASH=1
#   LOG_HIT_ERRORS=1              # log exceptions for per-hit writes (helpful for debugging)
#
# Note: Do not store raw IPs. This code derives geo only and (optionally) stores a salted hash.
#test

import os
import re
import hmac
import hashlib
import time
from dataclasses import dataclass
from ipaddress import ip_address, ip_network
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from cachetools import TTLCache
from google.api_core.exceptions import AlreadyExists

from tenant_utils import normalize_original_host
from flask import Request, redirect
import requests
from google.cloud import firestore
from user_agents import parse as parse_ua


try:
    import geoip2.database  # type: ignore
    print("geoip2 available, will use for geolocation")
except Exception:
    print("geoip2 not available, skipping geolocation features")
    geoip2 = None  # geoip2 not installed or unusable

_db = firestore.Client()

ID_PATTERN = re.compile(r'^[A-Za-z0-9_äöüÄÖÜß-]{1,64}$')
ALLOWED_SCHEMES = {'http', 'https'}

#HIT_TTL_DAYS = int(os.getenv('HIT_TTL_DAYS', '0'))
GEOIP_DB_PATH = os.getenv('GEOIP_DB_PATH') or None
GEOIP_API_URL = os.getenv('GEOIP_API_URL') or None
STORE_IP_HASH = os.getenv('STORE_IP_HASH') == '1'
IP_HASH_SALT = os.getenv('IP_HASH_SALT', '')
LOG_HIT_ERRORS = os.getenv('LOG_HIT_ERRORS') == '1'
HITS_BOTS_COLLECTION = 'hits_bots'

Increment = firestore.Increment
SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP

HMAC_SECRET = os.environ.get("WORKER_HMAC_SECRET", "")

# off | log_only | enforce — only applies to Cloudflare Worker traffic (valid HMAC)
REDIRECTOR_DOMAIN_TENANT_CHECK = "enforce"

# Firestore: customer_domains/{hostname} -> { tenant_id?, shared? }
CUSTOMER_DOMAINS_COLLECTION = "customer_domains"

@dataclass(frozen=True)
class _CachedCustomerDomain:
    """Snapshot from customer_domains/{host}; exists=False if document missing."""

    exists: bool
    tenant_id: str | None
    shared: bool


# customer_domains/{hostname} -> _CachedCustomerDomain
_DOMAIN_INFO_CACHE: TTLCache = TTLCache(maxsize=256, ttl=600)


_geo_reader = None
if GEOIP_DB_PATH and geoip2:
    try:
        _geo_reader = geoip2.database.Reader(GEOIP_DB_PATH)
    except Exception:
        _geo_reader = None

# Private networks to ignore for geolocation (local/dev)
_PRIVATE_NETS = [
    ip_network('10.0.0.0/8'),
    ip_network('172.16.0.0/12'),
    ip_network('192.168.0.0/16'),
    ip_network('127.0.0.0/8'),
    ip_network('::1/128'),
    ip_network('fc00::/7'),
]

def _is_private_ip(ip: str) -> bool:
    try:
        ip_obj = ip_address(ip)
        return any(ip_obj in net for net in _PRIVATE_NETS)
    except Exception:
        return True

def _first_ip_from_xff(xff: str) -> str | None:
    if not xff:
        return None
    parts = [p.strip() for p in xff.split(',') if p.strip()]
    return parts[0] if parts else None

def _is_safe_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme.lower() in ALLOWED_SCHEMES and bool(p.netloc)
    except Exception:
        return False


def _request_has_forbidden_explicit_port(request: Request) -> bool:
    """True if the request URL includes :port (canonical URLs omit the port)."""
    try:
        p = urlparse(request.url)
        if p.port is None:
            return False
        host = (p.hostname or '').lower()
        if host in ('localhost', '127.0.0.1', '::1') or host.endswith('.localhost'):
            return False
        return True
    except Exception:
        return False

def _device_type(ua) -> str:
    try:
        if ua.is_bot:
            return 'bot'
        if ua.is_mobile:
            return 'mobile'
        if ua.is_tablet:
            return 'tablet'
        if ua.is_pc:
            return 'desktop'
        return 'other'
    except Exception:
        return 'other'

# UA substrings that indicate bot/scanner (case-insensitive)
_BOT_UA_SUBSTRINGS = (
    'bot', 'crawler', 'spider', 'scanner', 'curl', 'python-requests', 'httpie',
    'wget', 'go-http-client', 'java/', 'okhttp',
)

def _is_bot_request(request: Request) -> bool:
    """Return True if the request appears to be from a bot (UA + headers)."""
    ua_str = (request.headers.get('User-Agent') or '').strip()
    try:
        ua = parse_ua(ua_str)
        if getattr(ua, 'is_bot', False):
            return True
    except Exception:
        pass
    ua_lower = ua_str.lower()
    if any(s in ua_lower for s in _BOT_UA_SUBSTRINGS):
        return True
    accept = (request.headers.get('Accept') or '').strip()
    if not accept or accept == '*/*':
        if not (request.headers.get('Accept-Language') or '').strip():
            return True
    return False

def _hash_ip(ip: str) -> str | None:
    if not STORE_IP_HASH or not IP_HASH_SALT or not ip:
        return None
    h = hashlib.sha256()
    h.update((IP_HASH_SALT + ip).encode('utf-8'))
    return h.hexdigest()

def _geo_from_maxmind(ip: str) -> dict | None:
    if not _geo_reader or not ip or _is_private_ip(ip):
        return None
    try:
        print("Looking up geo for IP:", ip)
        r = _geo_reader.city(ip)
        print("Geo lookup result:", r)
        return {
            'geo_country': (r.country.iso_code or '')[:2] if r.country else None,
            'geo_region':  (r.subdivisions[0].iso_code if r.subdivisions and len(r.subdivisions) else None),
            'geo_city':    (r.city.name if r.city else None),
            'geo_lat':     (r.location.latitude if r.location else None),
            'geo_lon':     (r.location.longitude if r.location else None),
            'geo_source':  'maxmind',
        }
    except Exception:
        return None

def _geo_from_api(ip: str) -> dict | None:
    if not GEOIP_API_URL or not ip or _is_private_ip(ip):
        return None
    try:
        url = GEOIP_API_URL.format(ip=ip)
        r = requests.get(url, timeout=1.5)
        if r.status_code != 200:
            return None
        data = r.json()
        # Map common fields from ipapi/ipinfo-style responses
        country = data.get('country') or data.get('country_code')
        region = data.get('region') or data.get('region_code') or data.get('state')
        city = data.get('city')
        lat = data.get('latitude') or data.get('lat')
        lon = data.get('longitude') or data.get('lon')
        return {
            'geo_country': (str(country)[:2] if country else None),
            'geo_region': region,
            'geo_city': city,
            'geo_lat': float(lat) if lat is not None else None,
            'geo_lon': float(lon) if lon is not None else None,
            'geo_source': 'api',
        }
    except Exception:
        return None

def _lookup_geo(ip: str) -> dict | None:
    # Prefer local DB, fall back to API
    geo = _geo_from_maxmind(ip)
    if geo:
        return geo
    geo = _geo_from_api(ip)
    if geo:
        return geo
    return None


def _extract_link_id(request):
    """
    Resolves the link id from either:
      - query param:  ?id=TRACKING-ID
      - path:         /TRACKING-ID   or   /r/TRACKING-ID   or   /go/TRACKING-ID

    Path must match exactly: no extra segments (e.g. /id/extra/file → invalid).
    """
    q = (request.args.get("id") or "").strip()
    print("Extracted link ID from query param:", q)
    if q:
        return q

    path = (request.path or "/").strip("/")
    if not path:
        return ""

    parts = path.split("/")

    if len(parts) == 1:
        link_id = parts[0].strip()
        print("Extracted link ID from path:", link_id)
        return link_id

    if len(parts) == 2 and parts[0] in {"r", "go", "t"}:
        link_id = parts[1].strip()
        print("Extracted link ID from path (prefixed):", link_id)
        return link_id

    return ""


def _get_customer_domain_info(hostname: str) -> _CachedCustomerDomain | None:
    """Load customer_domains/{hostname} with TTL cache. None if hostname empty."""
    if not hostname or not str(hostname).strip():
        return None
    key = str(hostname).strip().lower()
    if not key:
        return None
    if key in _DOMAIN_INFO_CACHE:
        return _DOMAIN_INFO_CACHE[key]
    snap = _db.collection(CUSTOMER_DOMAINS_COLLECTION).document(key).get()
    if not snap.exists:
        info = _CachedCustomerDomain(exists=False, tenant_id=None, shared=False)
    else:
        d = snap.to_dict() or {}
        raw_tid = d.get("tenant_id")
        tid: str | None = None
        if isinstance(raw_tid, str) and raw_tid.strip():
            tid = raw_tid.strip()
        shared = bool(d.get("shared"))
        info = _CachedCustomerDomain(exists=True, tenant_id=tid, shared=shared)
    _DOMAIN_INFO_CACHE[key] = info
    return info


def _normalized_link_allowed_hosts(raw) -> list[str]:
    """Normalize allowed_hosts from link document (list of hostnames, lowercase, no port)."""
    if raw is None:
        return []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for x in raw:
        if not isinstance(x, str) or not x.strip():
            continue
        h = normalize_original_host(x.strip())
        if h:
            out.append(h)
    return out


def _apply_worker_domain_tenant_enforcement(
    request: Request,
    source: str,
    link_id: str,
    link_tenant_id: str | None,
    link_allowed_hosts: list[str] | None,
) -> tuple | None:
    """
    For Worker traffic only:
    - Unknown host / missing customer_domains doc -> reject.
    - Shared domain (customer_domains.shared true): link must list host in allowed_hosts.
    - Dedicated domain: link tenant_id must match domain tenant_id.
    Returns Flask tuple to short-circuit, or None to continue.
    """
    mode = REDIRECTOR_DOMAIN_TENANT_CHECK
    if mode not in ("log_only", "enforce") or source != "cloudflare_worker":
        return None

    host = normalize_original_host(request.headers.get("X-Original-Host") or "")
    info = _get_customer_domain_info(host) if host else None

    ltid = (link_tenant_id or "").strip() if isinstance(link_tenant_id, str) else ""
    ltid = ltid or None
    allowed = _normalized_link_allowed_hosts(link_allowed_hosts)

    unknown_host = not host or info is None or not info.exists
    shared_host_not_listed = False
    missing_link_tenant = False
    missing_domain_tenant = False
    mismatch = False

    if not unknown_host and info.shared:
        shared_host_not_listed = host not in allowed
    elif not unknown_host and not info.shared:
        missing_link_tenant = ltid is None
        missing_domain_tenant = info.tenant_id is None
        mismatch = not missing_link_tenant and not missing_domain_tenant and ltid != info.tenant_id

    bad = (
        unknown_host
        or shared_host_not_listed
        or missing_link_tenant
        or missing_domain_tenant
        or mismatch
    )

    if not bad:
        return None

    print(
        "[REDIRECTOR_DOMAIN_TENANT] "
        f"link_id={link_id!r} host={host!r} domain_shared={getattr(info, 'shared', None)} "
        f"domain_tenant={getattr(info, 'tenant_id', None)!r} link_tenant={ltid!r} "
        f"allowed_hosts={allowed!r} unknown_host={unknown_host} shared_host_not_listed={shared_host_not_listed} "
        f"missing_link_tenant={missing_link_tenant} missing_domain_tenant={missing_domain_tenant} mismatch={mismatch}"
    )

    if mode == "log_only":
        return None

    return (
        "Link not found.",
        404,
        {"Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store"},
    )


def _is_from_worker(request: Request, link_id: str) -> bool:
    """Return True if signature is valid for this request (ts:id)."""
    try:
        ts = request.headers.get("x-ts")
        sig = request.headers.get("x-sig")
        if not (HMAC_SECRET and ts and sig and link_id):
            return False

        # Basic replay protection: 5-minute window
        now = int(time.time())
        if abs(now - int(ts)) > 300:
            return False

        msg = f"{ts}:{link_id}"
        secret = os.environ.get("WORKER_HMAC_SECRET", "") or HMAC_SECRET

        #print("DEBUG ts=", repr(ts))
        #print("DEBUG id=", repr(link_id))
        #print("DEBUG msg=", repr(msg))
        #print("DEBUG secret_len=", len(secret))
        #print("DEBUG secret_head_tail=", repr(secret[:2]), repr(secret[-2:]))  # look for quotes/newlines

        # If you previously set the env var as ...WORKER_HMAC_SECRET='value' it may contain the quotes!
        if secret and ((secret.startswith("'") and secret.endswith("'")) or (secret.startswith('"') and secret.endswith('"'))):
            print("DEBUG WARNING: secret appears quoted; stripping quotes for now")
            secret = secret[1:-1]

        expected = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, (sig or "").lower())
    except Exception:
        return False


def redirector(request: Request):
    if _request_has_forbidden_explicit_port(request):
        return (
            'Invalid URL: do not include a port in the address.',
            400,
            {'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store'},
        )

    # Health
    #test deploy2
    if request.path.strip('/') == 'health':
        return ('ok', 200, {'Content-Type': 'text/plain', 'Cache-Control': 'no-store'})

    link_id = _extract_link_id(request)
    source = "cloudflare_worker" if _is_from_worker(request, link_id) else "direct"
    print("DEGUB Source:", source)

    if not link_id or not ID_PATTERN.match(link_id):
        return ('Missing or invalid "id" query parameter.', 400)

    link_ref = _db.collection('links').document(link_id)
    print("Fetching LINK:", link_id)
    print("Link ref path:", link_ref.path)
    snap = link_ref.get()
    if not snap.exists:
        return ('Link not found.', 404)

    data = snap.to_dict() or {}
    if not data.get('active', True):
        return ('Link is inactive.', 410)

    destination = data.get('destination')
    if not destination or not _is_safe_url(destination):
        return ('Destination is invalid or missing.', 500)

    blocked = _apply_worker_domain_tenant_enforcement(
        request,
        source,
        link_id,
        data.get("tenant_id"),
        data.get("allowed_hosts"),
    )
    if blocked is not None:
        return blocked

    # --- Pull refs from link (new schema) ---
    campaign_ref = data.get('campaign_ref')     # DocumentReference or None
    business_ref = data.get('business_ref')     # DocumentReference or None
    target_ref   = data.get('target_ref')       # DocumentReference or None
    template_id  = data.get('template_id')      # string or None
    owner_id     = data.get('owner_id')
    campaign_name = data.get("campaign_name")

    # Detect if this is a test request (health monitor or manual test)
    # Primary check: link_id pattern (most reliable - test links use "monitor-test-*" pattern)
    # Secondary check: User-Agent header (backup, may not always be preserved through proxies)
    # Tertiary check: utm_test query parameter (for manual browser-based testing)
    is_test_data = (
        link_id.startswith('monitor-test') or  # Primary: test link ID pattern
        request.headers.get('User-Agent', '').startswith('HealthMonitor/') or  # Secondary: health monitor user agent
        request.args.get('utm_test') == 'true'  # Tertiary: manual test via UTM parameter
    )
    
    # Log test requests for debugging
    if is_test_data:
        print(f"[TEST REQUEST] link_id={link_id}, utm_test={request.args.get('utm_test')}, user_agent={request.headers.get('User-Agent', '')[:50]}")

    is_bot = _is_bot_request(request)

    # --- Batch: update link (+ business, + campaign totals.hits) ---
    # Skip counter updates for test requests and bot requests
    if not is_test_data and not is_bot:
        try:
            batch = _db.batch()

            # link aggregates
            print("Updating link hit count:", link_ref)
            print("Link REF server timestamp:", SERVER_TIMESTAMP)
            batch.update(link_ref, {
                'hit_count': Increment(1),
                'last_hit_at': SERVER_TIMESTAMP,
            })

            # business aggregates (per-customer overlay)
            if isinstance(business_ref, firestore.DocumentReference) and owner_id:
                # Update customer-specific business overlay instead of canonical business
                business_id = business_ref.id
                customer_business_ref = _db.collection('customers').document(owner_id).collection('businesses').document(business_id)
                batch.set(customer_business_ref, {
                    'hit_count': Increment(1),
                    'last_hit_at': SERVER_TIMESTAMP,
                    'updated_at': SERVER_TIMESTAMP,
                }, merge=True)

            # campaign aggregates (totals.hits)
            if isinstance(campaign_ref, firestore.DocumentReference):
                batch.set(campaign_ref, {
                    'totals.hits': Increment(1),
                    'updated_at': SERVER_TIMESTAMP,
                    'last_hit_at': SERVER_TIMESTAMP,
                }, merge=True)

            batch.commit()
        except Exception as e:
            print(f"[ERROR] Exception during aggregate update: {e}")
            # Never block redirect on aggregates
            pass
            # Never block redirect on aggregates
            pass

    # --- Build hit doc (skip for test requests) ---
    # Test requests redirect but don't create hit documents
    if not is_test_data:
        ua_str = request.headers.get('User-Agent', '') or ''
        ua = parse_ua(ua_str)
        dev = _device_type(ua)
        browser = f"{ua.browser.family} {ua.browser.version_string}".strip()
        os_str = f"{ua.os.family} {ua.os.version_string}".strip()
        referer = request.headers.get('Referer')

        xff = request.headers.get('X-Forwarded-For', '')
        client_ip = _first_ip_from_xff(xff)

        hit = {
            'link_id': link_id,
            'campaign_ref': campaign_ref,
            'business_ref': business_ref,
            'target_ref': target_ref,
            'owner_id': owner_id,
            'template_id': template_id,
            'ts': SERVER_TIMESTAMP,
            'user_agent': ua_str[:1024],
            'device_type': dev,
            'ua_browser': browser[:128],
            'ua_os': os_str[:128],
            "campaign_name": campaign_name,
            "hit_origin": source, #shows if it is from link or qr code
        }
        if is_bot:
            hit['suspected_bot'] = True
        hit['ip-address'] = client_ip

        if referer:
            hit['referer'] = referer[:512]

        # Optional geo + ip hash (no raw IP stored)
        try:
            if client_ip and not _is_private_ip(client_ip):
                geo = _lookup_geo(client_ip)
                if geo:
                    hit.update({k: v for k, v in geo.items() if v is not None})
                ip_hash = _hash_ip(client_ip)
                if ip_hash:
                    hit['ip_hash'] = ip_hash
        except Exception:
            ip_hash = None  # ensure defined if used later
        
        # Write hit (never block): bots -> hits_bots, others -> hits
        try:
            if is_bot:
                _db.collection(HITS_BOTS_COLLECTION).add(hit)
            else:
                _db.collection('hits').add(hit)
        except Exception:
            if LOG_HIT_ERRORS:
                import logging; logging.exception("Hit write failed")

        # Optional: first-seen unique IP per campaign (write-time aggregation); skip for bots
        try:
            if not is_bot and ip_hash and isinstance(campaign_ref, firestore.DocumentReference):
                uniq_ref = campaign_ref.collection('unique_ips').document(ip_hash)
                # create if not exists; increment totals.unique_ips only on first seen
                unique_ip_data = {'first_seen': SERVER_TIMESTAMP}
                uniq_ref.create(unique_ip_data)
                campaign_ref.set({'totals.unique_ips': Increment(1)}, merge=True)
        except AlreadyExists:
            pass  # already counted
        except Exception:
            # do not block redirect
            pass

    # Redirect
    resp = redirect(destination, code=302)
    resp.headers['Cache-Control'] = 'no-store'
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['Referrer-Policy'] = 'no-referrer'
    return resp