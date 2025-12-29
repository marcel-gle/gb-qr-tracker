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
from ipaddress import ip_address, ip_network
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from google.api_core.exceptions import AlreadyExists
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


Increment = firestore.Increment
SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP

HMAC_SECRET = os.environ.get("WORKER_HMAC_SECRET", "")


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
    """
    # 1) Prefer query param (backwards compatible)
    q = (request.args.get("id") or "").strip()
    print("Extracted link ID from query param:", q)
    if q:
        return q

    # 2) Fallback to path
    path = (request.path or "/").strip("/")  # e.g., "TRACKING-ID" or "r/TRACKING-ID"
    if not path:
        return ""

    parts = path.split("/")
    # Support optional short prefixes to avoid route collisions
    #if parts[0] in {"r", "go", "t"} and len(parts) >= 2:
    #    return parts[1].strip()

    # Otherwise treat the first segment as the id

    link_id = parts[0].strip()
    print("Extracted link ID from path:", link_id)
    return link_id

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
        expected = hmac.new(
            HMAC_SECRET.encode("utf-8"),
            msg.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        # timing-safe compare

        compared = hmac.compare_digest(expected, sig)
        #print("Expected HMAC:", expected)
        #print("Provided HMAC:", sig)
        #print("HMAC comparison result:", compared)
        msg = f"{ts}:{link_id}"
        secret = os.environ.get("WORKER_HMAC_SECRET", "")

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
        #print("Expected HMAC:", expected)
        #print("Provided HMAC:", sig)
        #print("HMAC comparison result:", hmac.compare_digest(expected, (sig or "").lower()))
        return compared
    except Exception:
        return False


def redirector(request: Request):
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

    # --- Pull refs from link (new schema) ---
    campaign_ref = data.get('campaign_ref')     # DocumentReference or None
    business_ref = data.get('business_ref')     # DocumentReference or None
    target_ref   = data.get('target_ref')       # DocumentReference or None
    template_id  = data.get('template_id')      # string or None
    owner_id     = data.get('owner_id')
    campaign_name = data.get("campaign_name")

    # Detect if this is a health monitor test request
    # Primary check: link_id pattern (most reliable - test links use "monitor-test-*" pattern)
    # Secondary check: User-Agent header (backup, may not always be preserved through proxies)
    is_test_data = (
        link_id.startswith('monitor-test') or  # Primary: test link ID pattern
        request.headers.get('User-Agent', '').startswith('HealthMonitor/')  # Secondary: health monitor user agent
    )

    # --- Batch: update link (+ business, + campaign totals.hits) ---
    # Skip counter updates for test requests to prevent polluting production metrics
    if not is_test_data:
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
        except Exception:
            # Never block redirect on aggregates
            pass

    # --- Build hit doc ---
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
    
    # Mark test data from health monitor
    if is_test_data:
        hit['is_test_data'] = True
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
    # write hit (never block)
    # For test data, write to separate test_hits collection to avoid polluting production data
    try:
        if is_test_data:
            _db.collection('test_hits').add(hit)
        else:
            _db.collection('hits').add(hit)
    except Exception:
        if LOG_HIT_ERRORS:
            import logging; logging.exception("Hit write failed")

    # Optional: first-seen unique IP per campaign (write-time aggregation)
    # Skip for test data to prevent polluting production metrics
    if not is_test_data:
        try:
            if ip_hash and isinstance(campaign_ref, firestore.DocumentReference):
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