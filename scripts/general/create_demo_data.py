#!/usr/bin/env python3
"""
Generate coherent demo data for all Firestore collections.

This script creates realistic, interconnected demo data that maintains
referential integrity across all collections (campaigns, businesses, targets,
links, hits, calls). All demo data is marked with is_demo: True for easy identification
and cleanup.

Note: The customer/user must already exist in both Firestore customers collection
and Firebase Auth. This script does not create or delete customers.

Usage:
    python create_demo_data.py [--owner-id OWNER_ID] [--campaigns N] [--businesses N] [--links-per-campaign N] [--hits-per-link N] [--cleanup] [--dry-run]

Setup:
    1. Ensure Firebase Admin SDK credentials are available
    2. Default credentials path points to dev environment
    3. Ensure the demo customer exists in Firestore and Firebase Auth
    4. Run with --dry-run first to preview what will be created

Cleanup:
    All demo data is marked with is_demo: True flag, making it easy to identify
    and delete. The cleanup function queries for is_demo == True across all
    collections (campaigns, businesses, targets, links, hits, calls).
"""

import argparse
import hashlib
import random
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import ArrayUnion, SERVER_TIMESTAMP

# --- 🔧 CONFIGURATION ---
DEFAULT_CREDENTIALS_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
DEFAULT_OWNER_ID = "Panugay5HYQ6WzyiBvUB5E3FSRB3"
# Demo links use this tenant_id; add customer_domains/{your-demo-go-host} with the same tenant_id for Worker tests.
DEMO_TENANT_ID = "demo"
CALLS_COLLECTION = "calls"

# German cities with coordinates (city, region, lat, lon, sample_postcode)
GERMAN_CITIES = [
    ("Munich", "Bavaria", 48.1372, 11.5756, "80331"),
    ("Berlin", "Berlin", 52.5200, 13.4050, "10115"),
    ("Hamburg", "Hamburg", 53.5511, 9.9937, "20095"),
    ("Cologne", "North Rhine-Westphalia", 50.9375, 6.9603, "50667"),
    ("Frankfurt", "Hesse", 50.1109, 8.6821, "60311"),
    ("Stuttgart", "Baden-Württemberg", 48.7758, 9.1829, "70173"),
    ("Düsseldorf", "North Rhine-Westphalia", 51.2277, 6.7735, "40213"),
    ("Dortmund", "North Rhine-Westphalia", 51.5136, 7.4653, "44135"),
    ("Essen", "North Rhine-Westphalia", 51.4556, 7.0116, "45127"),
    ("Leipzig", "Saxony", 51.3397, 12.3731, "04103"),
    ("Bremen", "Bremen", 53.0793, 8.8017, "28195"),
    ("Dresden", "Saxony", 51.0504, 13.7373, "01067"),
    ("Hannover", "Lower Saxony", 52.3759, 9.7320, "30159"),
    ("Nuremberg", "Bavaria", 49.4521, 11.0767, "90402"),
    ("Duisburg", "North Rhine-Westphalia", 51.4344, 6.7623, "47051"),
]

# German business name patterns
BUSINESS_NAME_PATTERNS = [
    "{name} GmbH",
    "{name} UG",
    "{name} & Co. KG",
    "{name} e.K.",
    "{name} AG",
    "{name} OHG",
    "{name} mbH",
]

# Business name bases (German company names)
BUSINESS_NAME_BASES = [
    "TechLösungen", "DigitalDienstleistungen", "Innovationszentrum", "Unternehmensberatung",
    "MarketingProfi", "Datenanalyse", "CloudServices", "Softwareentwicklung",
    "WebDesign", "ECommerce", "Logistik", "Transport", "Produktion",
    "Einzelhandel", "Großhandel", "Bauwesen", "Ingenieurwesen", "Architektur",
    "Rechtsberatung", "Buchhaltung", "Finanzen", "Versicherung", "Immobilien",
    "Gesundheitswesen", "Pharmazie", "Bildung", "Schulung", "Beratung",
    "Medien", "Werbung", "Öffentlichkeitsarbeit", "Eventmanagement", "Catering",
    "Gastronomie", "Tourismus", "Automobil", "Elektronik", "Telekommunikation",
    "Handwerk", "Metallbau", "Elektrotechnik", "Sanitär", "Heizung",
    "Dachdecker", "Maler", "Tischler", "Gärtnerei", "Bäckerei",
]

# German street names
STREET_NAMES = [
    "Hauptstraße", "Bahnhofstraße", "Kirchstraße", "Dorfstraße", "Gartenstraße",
    "Schulstraße", "Bergstraße", "Waldstraße", "Mühlenstraße", "Lindenstraße",
    "Parkstraße", "Friedhofstraße", "Neue Straße", "Alte Straße", "Ringstraße",
    "Poststraße", "Marktstraße", "Rathausstraße", "Kirchplatz", "Am Markt",
]

# Campaign name templates (German) - uses business name base
CAMPAIGN_NAME_TEMPLATES = [
    "Kampagne {name} 1",
    "Kampagne {name} 2",
    "Kampagne {name} 3",
    "Kampagne {name} Q1",
    "Kampagne {name} Q2",
    "Kampagne {name} Q3",
    "Kampagne {name} Q4",
    "Kampagne {name} Winter",
    "Kampagne {name} Sommer",
    "Kampagne {name} Herbst",
    "Kampagne {name} Frühling",
    "Kampagne {name} Sonder",
]

# Template IDs
TEMPLATE_IDS = [
    "template_standart_qr_track.pdf",
    "template_4791.pdf",
    "template_6622.pdf",
    "template_781.pdf",
    "template_731.pdf",
    "template_813.pdf",
    "template_9313.pdf",
    "template_862.pdf",
    "template_855.pdf",
    "template_4511.pdf",
    "template_561.pdf",
    "template_551.pdf",
    "template_6831.pdf",
]

# Device types
DEVICE_TYPES = ["desktop", "mobile", "tablet"]

# Hit origins
HIT_ORIGINS = ["direct", "cloudflare_worker"]

# Browsers
BROWSERS = [
    "Chrome 139.0.0", "Chrome 138.0.0", "Chrome 137.0.0",
    "Firefox 130.0", "Firefox 129.0",
    "Safari 17.5", "Safari 17.4",
    "Edge 128.0", "Edge 127.0",
]

# Operating systems
OSES = [
    "Mac OS X 10.15.7", "Windows 11", "Windows 10",
    "Ubuntu 22.04", "Ubuntu 20.04",
    "iOS 17.6", "iOS 17.5",
    "Android 14", "Android 13",
]

# User agent templates
USER_AGENT_TEMPLATES = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser} Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser} Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser} Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
]

# Salutations
SALUTATIONS = ["Herr", "Frau", "Herr Dr.", "Frau Dr.", "Herr Prof.", "Frau Prof."]

# Campaign statuses (weighted)
CAMPAIGN_STATUSES = ["draft", "active", "active", "active", "archived"]  # More active than draft/archived

# Target statuses
TARGET_STATUSES = ["linked", "validated", "excluded"]

# call_process presets (weight, process dict) — aligned with frontend outcomeToCallProcess
CALL_PROCESS_PRESETS = [
    (35, {"reach": "not_reached", "gatekeeper": None, "interest": None, "appointment": None}),
    (15, {"reach": "reached", "gatekeeper": "not_overcome", "interest": None, "appointment": None}),
    (20, {"reach": "reached", "gatekeeper": "none", "interest": "interested", "appointment": "no"}),
    (15, {"reach": "reached", "gatekeeper": "overcome", "interest": "interested", "appointment": "no"}),
    (10, {"reach": "reached", "gatekeeper": "overcome", "interest": "interested", "appointment": "yes"}),
    (5, {"reach": "reached", "gatekeeper": "none", "interest": "not_interested", "appointment": "no"}),
]

DEMO_CALL_NOTES = [
    "Erneut versucht, GF nicht verfügbar.",
    "Kurzes Gespräch, Infomaterial per E-Mail zugesagt.",
    "Termin für nächste Woche vereinbart.",
    "Kein Interesse am Moment, Follow-up in 4 Wochen.",
    "Gatekeeper legt auf, Rückruf versprochen.",
    "Positives Feedback zum QR-Briefing.",
    "Falsche Durchwahl, korrekte Nummer notiert.",
    None,
]


# --- HELPERS ---
def sanitize_id(value: str) -> str:
    """Normalize ID to lowercase, matching the upload_processor logic."""
    if value is None:
        return ""
    v = str(value).strip()
    # Allow A-Z, a-z, 0-9, and German umlauts (ä, ö, ü, ß)
    v = re.sub(r"[^A-Za-z0-9äöüÄÖÜß]+", "-", v)
    v = re.sub(r"-{2,}", "-", v).strip("-")
    v = v.lower()
    return v


def make_business_id(business_name: Optional[str], postcode: Optional[str]) -> str:
    """Create a stable business document ID from business name and postcode."""
    base = sanitize_id(business_name or "")
    if postcode:
        base = f"{base}-{sanitize_id(postcode)}" if base else sanitize_id(postcode)
    return base or "biz"


def normalize_postcode(value: Optional[str]) -> str:
    """Normalize to a 5-digit German PLZ string (keeps leading zeros)."""
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return "00000"
    return digits.zfill(5)[-5:]


def jitter_postcode(value: Optional[str], suffix_delta: int = 15) -> str:
    """
    Return a nearby valid 5-digit PLZ without stripping leading zeros.

    Jitters only the last two digits so the regional prefix (first 3) stays stable.
    """
    plz = normalize_postcode(value)
    prefix, suffix = plz[:3], int(plz[3:])
    suffix = max(0, min(99, suffix + random.randint(-suffix_delta, suffix_delta)))
    return f"{prefix}{suffix:02d}"


def allocate_demo_business_id(
    db: firestore.Client,
    base_business_id: str,
    max_attempts: int = 100,
) -> str:
    """
    Pick a businesses/{id} that is free or already marked is_demo.

    Never returns the ID of a non-demo document (avoids overwriting production data).
    """
    candidate = base_business_id or "biz"
    for n in range(max_attempts):
        snap = db.collection("businesses").document(candidate).get()
        if not snap.exists:
            return candidate
        if (snap.to_dict() or {}).get("is_demo"):
            return candidate
        candidate = f"{base_business_id}-demo-{n + 1}"
    raise RuntimeError(
        f"Could not allocate demo business id for {base_business_id!r} "
        f"after {max_attempts} attempts"
    )


def business_data_from_snapshot(
    snap: firestore.DocumentSnapshot,
) -> Dict:
    """Build target/link payload fields from an existing business document."""
    data = snap.to_dict() or {}
    business_name = data.get("business_name") or data.get("name") or ""
    return {
        "business_id": snap.id,
        "business_name": business_name,
        "name": data.get("name") or business_name,
        "street": data.get("street"),
        "house_number": data.get("house_number"),
        "postcode": data.get("postcode"),
        "city": data.get("city"),
        "address": data.get("address"),
        "email": data.get("email"),
        "phone": data.get("phone"),
        "salutation": data.get("salutation"),
    }


def random_timestamp_within_days(days: int) -> datetime:
    """Generate a random timestamp within the last N days."""
    now = datetime.now(timezone.utc)
    days_back = random.randint(0, days)
    seconds_in_day = random.randint(0, 24 * 3600 - 1)
    ts = (now - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
    ts += timedelta(seconds=seconds_in_day)
    return min(ts, now)


def random_timestamp_within_weeks(weeks: int) -> datetime:
    """Generate a random timestamp within the last N weeks, weighted toward recent dates."""
    now = datetime.now(timezone.utc)
    days = weeks * 7
    
    # Weight toward more recent dates (exponential decay)
    # This makes recent hits more common than old ones
    days_back = int(random.expovariate(1.0 / (days / 2))) % days
    
    # Random time within the day
    seconds_in_day = random.randint(0, 24 * 3600 - 1)
    ts = (now - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
    ts += timedelta(seconds=seconds_in_day)
    return min(ts, now)


def random_call_time_within_weeks(weeks: int = 4) -> datetime:
    """Random call_time in the last N weeks, weekday business hours 8–18 UTC."""
    ts = random_timestamp_within_weeks(weeks)
    return ts.replace(
        hour=random.randint(8, 18),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=0,
    )


def random_call_process() -> Dict:
    """Sample a valid call_process map (canonical V2 shape)."""
    processes = [p for _, p in CALL_PROCESS_PRESETS]
    weights = [w for w, _ in CALL_PROCESS_PRESETS]
    chosen = random.choices(processes, weights=weights, k=1)[0]
    return dict(chosen)


def sample_demo_call_note() -> Optional[str]:
    return random.choice(DEMO_CALL_NOTES)


def demo_ip_hash(seed: str) -> str:
    """Deterministic SHA-256 hex digest (same shape as redirector ip_hash)."""
    return hashlib.sha256(f"demo-ip:{seed}".encode("utf-8")).hexdigest()


def pick_demo_ip_hash(
    used_hashes: set,
    reuse_pool: Optional[List[str]] = None,
    reuse_probability: float = 0.35,
) -> str:
    """
    Return an ip_hash for a demo hit.

    Prefer globally unique hashes; when reuse_pool is set (same link), sometimes
    reuse an earlier hash for that link (~35% by default).
    """
    if reuse_pool and random.random() < reuse_probability:
        return random.choice(reuse_pool)

    for _ in range(50):
        candidate = demo_ip_hash(uuid.uuid4().hex)
        if candidate not in used_hashes:
            used_hashes.add(candidate)
            return candidate

    # Extremely unlikely fallback: allow collision rather than fail the run.
    candidate = demo_ip_hash(f"overflow-{len(used_hashes)}-{random.random()}")
    used_hashes.add(candidate)
    return candidate


def firestore_add_ref(collection, data: dict) -> firestore.DocumentReference:
    """
    Add a document and return its reference.

    google-cloud-firestore may return (write_time, DocumentReference) or only
    DocumentReference depending on version; firebase_admin wraps the client.
    """
    result = collection.add(data)
    if isinstance(result, tuple):
        return result[1]
    return result


def small_jitter(v: float, max_abs_delta: float = 0.01) -> float:
    """Add small random jitter to a coordinate value."""
    return v + random.uniform(-max_abs_delta, max_abs_delta)


def generate_business_name() -> str:
    """Generate a realistic German business name."""
    base = random.choice(BUSINESS_NAME_BASES)
    pattern = random.choice(BUSINESS_NAME_PATTERNS)
    return pattern.format(name=base)


def generate_street_address() -> Tuple[str, str]:
    """Generate a realistic German street address."""
    street = random.choice(STREET_NAMES)
    house_number = str(random.randint(1, 200))
    # Sometimes add a letter suffix
    if random.random() < 0.2:
        house_number += random.choice(["a", "b", "c"])
    return street, house_number


def generate_phone() -> str:
    """Generate a realistic German phone number."""
    area_code = random.choice(["030", "040", "089", "0211", "0221", "0711", "069"])
    number = "".join([str(random.randint(0, 9)) for _ in range(7)])
    return f"{area_code} {number}"


def generate_email(business_name: str) -> str:
    """Generate a realistic business email."""
    domain_base = sanitize_id(business_name.split()[0].lower())
    # Remove common suffixes
    domain_base = re.sub(r"-(gmbh|ug|ag|kg|ek|mbh|ohg)$", "", domain_base)
    domain = f"{domain_base}.de"
    return f"info@{domain}"


def generate_user_agent(browser: str) -> str:
    """Generate a realistic user agent string."""
    template = random.choice(USER_AGENT_TEMPLATES)
    return template.format(browser=browser)


def compose_full_address(street: str, house_number: str, postcode: str, city: str, country: str = "Germany") -> str:
    """Compose a full address string."""
    parts = [f"{street} {house_number}", f"{postcode} {city}", country]
    return ", ".join(filter(None, parts))


# --- MAIN GENERATION FUNCTIONS ---
def generate_campaigns(
    db: firestore.Client,
    owner_id: str,
    num_campaigns: int,
    dry_run: bool = False
) -> List[firestore.DocumentReference]:
    """Generate campaign documents."""
    campaigns = []
    
    for i in range(num_campaigns):
        campaign_id = str(uuid.uuid4())
        campaign_name_template = random.choice(CAMPAIGN_NAME_TEMPLATES)
        # Use a random business name base for the campaign name
        business_name_base = random.choice(BUSINESS_NAME_BASES)
        campaign_name = campaign_name_template.format(name=business_name_base)
        code = sanitize_id(campaign_name).upper()
        status = random.choice(CAMPAIGN_STATUSES)
        created_at = random_timestamp_within_days(180)  # Within last 6 months
        
        campaign_ref = db.collection("campaigns").document(campaign_id)
        
        if not dry_run:
            campaign_ref.set({
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "code": code,
                "owner_id": owner_id,
                "status": status,
                "totals.targets": 0,
                "totals.links": 0,
                "totals.hits": 0,
                "totals.unique_ips": 0,
                "created_at": created_at,
                "updated_at": created_at,
                "last_hit_at": None,
                "is_demo": True,  # Mark as demo data for easy cleanup
            })
            print(f"✓ Created campaign: {campaign_name} ({campaign_id})")
        else:
            print(f"[DRY RUN] Would create campaign: {campaign_name} ({campaign_id})")
        
        campaigns.append(campaign_ref)
    
    return campaigns


def generate_businesses(
    db: firestore.Client,
    owner_id: str,
    num_businesses: int,
    dry_run: bool = False
) -> List[Tuple[firestore.DocumentReference, Dict]]:
    """Generate business documents. Returns list of (ref, data) tuples."""
    businesses = []
    
    for i in range(num_businesses):
        city, region, lat, lon, postcode = random.choice(GERMAN_CITIES)
        business_name = generate_business_name()
        street, house_number = generate_street_address()
        
        postcode = jitter_postcode(postcode)
        
        base_business_id = make_business_id(business_name, postcode)
        address = compose_full_address(street, house_number, postcode, city)

        generated_data = {
            "business_id": base_business_id,
            "business_name": business_name,
            "name": business_name,
            "street": street,
            "house_number": house_number,
            "postcode": postcode,
            "city": city,
            "address": address,
            "coordinate": {
                "lat": round(small_jitter(lat), 6),
                "lon": round(small_jitter(lon), 6),
                "source": "demo",
            },
            "email": generate_email(business_name) if random.random() < 0.8 else None,
            "phone": generate_phone() if random.random() < 0.7 else None,
            "salutation": random.choice(SALUTATIONS) if random.random() < 0.6 else None,
            "hit_count": 0,
            "last_hit_at": None,
            "created_at": random_timestamp_within_days(365),
            "updated_at": random_timestamp_within_days(365),
            "is_demo": True,
        }

        if dry_run:
            business_id = allocate_demo_business_id(db, base_business_id)
            business_ref = db.collection("businesses").document(business_id)
            snap = business_ref.get()
            if snap.exists and (snap.to_dict() or {}).get("is_demo"):
                business_data = business_data_from_snapshot(snap)
                print(
                    f"[DRY RUN] Would reuse demo business: "
                    f"{business_data['business_name']} ({business_id})"
                )
            else:
                business_data = {**generated_data, "business_id": business_id}
                print(
                    f"[DRY RUN] Would create business: {business_name} ({business_id})"
                )
        else:
            business_id = allocate_demo_business_id(db, base_business_id)
            business_ref = db.collection("businesses").document(business_id)
            snap = business_ref.get()
            if snap.exists:
                # Existing demo doc only — never merge generated fields onto production.
                business_ref.set({"ownerIds": ArrayUnion([owner_id])}, merge=True)
                business_data = business_data_from_snapshot(snap)
                print(
                    f"✓ Reused demo business: "
                    f"{business_data['business_name']} ({business_id})"
                )
            else:
                create_payload = {**generated_data, "business_id": business_id}
                create_payload["ownerIds"] = [owner_id]
                business_ref.create(create_payload)
                business_data = create_payload
                print(f"✓ Created business: {business_name} ({business_id})")

        businesses.append((business_ref, business_data))
    
    return businesses


def generate_targets(
    db: firestore.Client,
    campaign_ref: firestore.DocumentReference,
    businesses: List[Tuple[firestore.DocumentReference, Dict]],
    links_per_campaign: int,
    dry_run: bool = False
) -> List[Tuple[firestore.DocumentReference, firestore.DocumentReference, Dict]]:
    """Generate target documents for a campaign. Returns list of (target_ref, business_ref, business_data) tuples."""
    targets = []

    # Every business in the pool is a mailing target (capped only if pool exceeds links_per_campaign).
    cap = min(len(businesses), links_per_campaign) if links_per_campaign > 0 else len(businesses)
    selected_businesses = businesses[:cap]
    
    for business_ref, business_data in selected_businesses:
        target_id = str(uuid.uuid4())
        target_ref = campaign_ref.collection("targets").document(target_id)
        
        # linked / validated only so each business can receive a tracking link
        status_weights = [0.85, 0.15, 0.0]  # linked, validated, excluded
        status = random.choices(TARGET_STATUSES, weights=status_weights)[0]
        
        import_row = {
            "Namenszeile": business_data["business_name"],
            "Straße": business_data["street"],
            "Hausnummer": business_data["house_number"],
            "PLZ": business_data["postcode"],
            "Ort": business_data["city"],
        }
        
        dedupe_key = sanitize_id(f"{business_data['business_name']}-{business_data['postcode']}")
        
        if not dry_run:
            target_ref.set({
                "business_ref": business_ref,
                "link_ref": None,  # Will be set when link is created
                "status": status,
                "reason_excluded": "No destination" if status == "excluded" else None,
                "import_row": import_row,
                "dedupe_key": dedupe_key,
                "created_at": SERVER_TIMESTAMP,
                "updated_at": SERVER_TIMESTAMP,
                "is_demo": True,  # Mark as demo data for easy cleanup
            })
            print(f"  ✓ Created target: {target_id} (status: {status})")
        else:
            print(f"  [DRY RUN] Would create target: {target_id} (status: {status})")
        
        targets.append((target_ref, business_ref, business_data))
    
    return targets


def generate_links(
    db: firestore.Client,
    campaign_ref: firestore.DocumentReference,
    campaign_name: str,
    targets: List[Tuple[firestore.DocumentReference, firestore.DocumentReference, Dict]],
    owner_id: str,
    business_link_cache: Dict[str, firestore.DocumentReference],
    dry_run: bool = False
) -> List[Tuple[firestore.DocumentReference, firestore.DocumentReference, firestore.DocumentReference, firestore.DocumentReference]]:
    """Generate link documents. Returns list of (link_ref, business_ref, target_ref, campaign_ref) tuples."""
    links = []

    for target_ref, business_ref, business_data in targets:
        if not dry_run:
            target_data = target_ref.get().to_dict()
            if target_data and target_data.get("status") == "excluded":
                continue

        business_name = business_data.get("business_name", "business") if business_data else "business"
        postcode = business_data.get("postcode", "00000") if business_data else "00000"
        business_id = business_ref.id

        base_id = sanitize_id(business_name)
        link_id = f"{base_id}-{postcode}" if base_id else f"link-{postcode}"

        if business_id in business_link_cache:
            link_ref = business_link_cache[business_id]
            if not dry_run:
                target_ref.update({"link_ref": link_ref, "status": "linked"})
            links.append((link_ref, business_ref, target_ref, campaign_ref))
            continue

        if not dry_run:
            existing = db.collection("links").document(link_id).get()
            if existing.exists:
                counter = 1
                while True:
                    candidate = f"{link_id}-{counter}"
                    if not db.collection("links").document(candidate).get().exists:
                        link_id = candidate
                        break
                    counter += 1

        link_ref = db.collection("links").document(link_id)

        domain_base = sanitize_id(business_name.split()[0].lower())
        domain_base = re.sub(r"-(gmbh|ug|ag|kg|ek|mbh|ohg)$", "", domain_base)
        destination = f"https://{domain_base}.de/angebot" if domain_base else "https://example.com/offer"

        template_id = random.choice(TEMPLATE_IDS)

        snapshot_mailing = {
            "business_name": business_name,
            "address_lines": [f"{business_data.get('street', '')} {business_data.get('house_number', '')}"],
            "postcode": postcode,
            "city": business_data.get("city", "") if business_data else "",
            "country": "Germany",
            "recipient_name": None,
        }

        active = random.random() < 0.9  # 90% active

        if not dry_run:
            link_ref.set({
                "short_code": link_id,
                "destination": destination,
                "campaign_ref": campaign_ref,
                "business_ref": business_ref,
                "target_ref": target_ref,
                "owner_id": owner_id,
                "tenant_id": DEMO_TENANT_ID,
                "template_id": template_id,
                "campaign_name": campaign_name,
                "snapshot_mailing": snapshot_mailing,
                "active": active,
                "hit_count": 0,
                "last_hit_at": None,
                "created_at": SERVER_TIMESTAMP,
                "is_demo": True,
            })

            target_ref.update({"link_ref": link_ref, "status": "linked"})

            customer_business_ref = (
                db.collection("customers")
                .document(owner_id)
                .collection("businesses")
                .document(business_id)
            )
            customer_business_ref.set(
                {
                    "business_id": business_id,
                    "business_ref": business_ref,
                    "salutation": business_data.get("salutation"),
                    "name": business_data.get("name"),
                    "email": business_data.get("email"),
                    "phone": business_data.get("phone"),
                    "hit_count": 0,
                    "last_hit_at": None,
                    "updated_at": SERVER_TIMESTAMP,
                },
                merge=True,
            )

            business_link_cache[business_id] = link_ref
            print(f"  ✓ Created link: {link_id}")
        else:
            business_link_cache[business_id] = link_ref
            print(f"  [DRY RUN] Would create link: {link_id}")

        links.append((link_ref, business_ref, target_ref, campaign_ref))

    return links


def generate_hits(
    db: firestore.Client,
    links: List[Tuple[firestore.DocumentReference, firestore.DocumentReference, firestore.DocumentReference, firestore.DocumentReference]],
    campaign_name: str,
    owner_id: str,
    avg_hits_per_link: int,
    dry_run: bool = False
) -> int:
    """Generate hit documents. Returns total number of hits created."""
    total_hits = 0
    used_ip_hashes: set = set()

    for link_ref, business_ref, target_ref, campaign_ref in links:
        # Realistic low conversion distribution: only ~2% of links receive any hits.
        # Approximate distribution (per link):
        # - 98.0% chance of 0 hits
        # - 1.5% chance of 1 hit
        # - 0.3% chance of 2 hits
        # - 0.15% chance of 3 hits
        # - 0.05% chance of 4 hits (absolute maximum, extremely rare)
        # This yields ~2% of links with ≥1 hit and an average of ~0.02–0.03 hits/link,
        # i.e. a 1.6–2.4% conversion range when interpreted as hits per mailed link.
        rand = random.random()
        if rand < 0.98:
            num_hits = 0
        elif rand < 0.995:
            num_hits = 1
        elif rand < 0.998:
            num_hits = 2
        elif rand < 0.9995:
            num_hits = 3
        else:
            num_hits = 4  # Absolute maximum, extremely rare
        
        # Explicitly cap at 5 to prevent any edge cases or bugs
        num_hits = min(num_hits, 5)
        
        if num_hits == 0:
            continue
        
        if not dry_run:
            link_data = link_ref.get().to_dict()
            template_id = link_data.get("template_id") if link_data else random.choice(TEMPLATE_IDS)
        else:
            template_id = random.choice(TEMPLATE_IDS)
        
        # Generate timestamps spread across the last 4 weeks
        # Create a list of timestamps that are well-distributed
        hit_timestamps = []
        for i in range(num_hits):
            # Distribute hits across 4 weeks (28 days), weighted toward recent dates
            ts = random_timestamp_within_weeks(4)
            hit_timestamps.append(ts)
        
        # Sort timestamps to ensure chronological order (optional, but more realistic)
        hit_timestamps.sort()

        # IPs for this link: first hit is always new; later hits may reuse one of these.
        link_ip_hashes: List[str] = []

        for ts in hit_timestamps:
            ip_hash = pick_demo_ip_hash(
                used_ip_hashes,
                reuse_pool=link_ip_hashes if link_ip_hashes else None,
                reuse_probability=0.35,
            )
            if ip_hash not in link_ip_hashes:
                link_ip_hashes.append(ip_hash)

            city, region, lat, lon, _ = random.choice(GERMAN_CITIES)
            device = random.choice(DEVICE_TYPES)
            browser = random.choice(BROWSERS)
            os_ = random.choice(OSES)
            user_agent = generate_user_agent(browser)
            hit_origin = random.choice(HIT_ORIGINS)

            hit_data = {
                "link_id": link_ref.id,
                "campaign_ref": campaign_ref,
                "business_ref": business_ref,
                "target_ref": target_ref,
                "owner_id": owner_id,
                "template_id": template_id,
                "campaign_name": campaign_name,
                "ts": ts,
                "user_agent": user_agent,
                "device_type": device,
                "ua_browser": browser,
                "ua_os": os_,
                "hit_origin": hit_origin,
                "geo_city": city,
                "geo_region": region,
                "geo_country": "DE",
                "geo_lat": round(small_jitter(lat), 6),
                "geo_lon": round(small_jitter(lon), 6),
                "geo_source": "demo",
                "ip_hash": ip_hash,
                "is_demo": True,
            }
            
            if not dry_run:
                firestore_add_ref(db.collection("hits"), hit_data)
            total_hits += 1
        
        if not dry_run and num_hits > 0:
            print(f"    ✓ Created {num_hits} hits for link: {link_ref.id}")
        elif dry_run and num_hits > 0:
            print(f"    [DRY RUN] Would create {num_hits} hits for link: {link_ref.id}")
    
    return total_hits


def update_aggregates(
    db: firestore.Client,
    campaigns: List[firestore.DocumentReference],
    links: List[Tuple[firestore.DocumentReference, firestore.DocumentReference, firestore.DocumentReference, firestore.DocumentReference]],
    businesses: List[Tuple[firestore.DocumentReference, Dict]],
    owner_id: str,
    dry_run: bool = False
) -> None:
    """Update aggregate counts and timestamps after creating hits."""
    if dry_run:
        print("[DRY RUN] Would update aggregates")
        return
    
    # Group links by campaign and business
    campaign_links = defaultdict(list)
    business_hits = defaultdict(int)
    business_last_hit = {}
    link_hits = defaultdict(int)
    link_last_hit = {}
    campaign_hits = defaultdict(int)
    campaign_last_hit = {}
    campaign_unique_ips = defaultdict(set)  # Track unique IPs per campaign
    
    # Collect all links for campaigns
    for link_ref, business_ref, target_ref, campaign_ref in links:
        campaign_id = campaign_ref.id
        campaign_links[campaign_id].append(link_ref)
    
    # Query hits and aggregate
    hits_query = db.collection("hits").where("is_demo", "==", True).stream()
    for hit in hits_query:
        hit_data = hit.to_dict()
        link_id = hit_data.get("link_id")
        campaign_ref = hit_data.get("campaign_ref")
        business_ref = hit_data.get("business_ref")
        ts = hit_data.get("ts")
        ip_hash = hit_data.get("ip_hash")
        
        if link_id:
            link_hits[link_id] += 1
            if ts:
                if link_id not in link_last_hit:
                    link_last_hit[link_id] = ts
                else:
                    # Compare timestamps - Firestore Timestamp objects are comparable
                    try:
                        if ts > link_last_hit[link_id]:
                            link_last_hit[link_id] = ts
                    except (TypeError, AttributeError):
                        # Fallback: convert to comparable format
                        if isinstance(ts, datetime):
                            existing = link_last_hit[link_id]
                            if isinstance(existing, datetime) and ts > existing:
                                link_last_hit[link_id] = ts
        
        if campaign_ref:
            campaign_id = campaign_ref.id
            campaign_hits[campaign_id] += 1
            # Track unique IPs for this campaign
            if ip_hash:
                campaign_unique_ips[campaign_id].add(ip_hash)
            if ts:
                if campaign_id not in campaign_last_hit:
                    campaign_last_hit[campaign_id] = ts
                else:
                    try:
                        if ts > campaign_last_hit[campaign_id]:
                            campaign_last_hit[campaign_id] = ts
                    except (TypeError, AttributeError):
                        if isinstance(ts, datetime):
                            existing = campaign_last_hit[campaign_id]
                            if isinstance(existing, datetime) and ts > existing:
                                campaign_last_hit[campaign_id] = ts
        
        if business_ref:
            business_id = business_ref.id
            business_hits[business_id] += 1
            if ts:
                if business_id not in business_last_hit:
                    business_last_hit[business_id] = ts
                else:
                    try:
                        if ts > business_last_hit[business_id]:
                            business_last_hit[business_id] = ts
                    except (TypeError, AttributeError):
                        if isinstance(ts, datetime):
                            existing = business_last_hit[business_id]
                            if isinstance(existing, datetime) and ts > existing:
                                business_last_hit[business_id] = ts
    
    # Update links
    for link_id, hit_count in link_hits.items():
        link_ref = db.collection("links").document(link_id)
        update_data = {"hit_count": hit_count}
        if link_id in link_last_hit:
            update_data["last_hit_at"] = link_last_hit[link_id]
        link_ref.update(update_data)
    
    # Update campaigns
    for campaign_ref in campaigns:
        campaign_id = campaign_ref.id
        num_hits = campaign_hits.get(campaign_id, 0)
        
        # Count targets for this campaign
        targets_query = campaign_ref.collection("targets").stream()
        num_targets = sum(1 for _ in targets_query)
        
        # Count unique IPs for this campaign
        num_unique_ips = len(campaign_unique_ips.get(campaign_id, set()))
        
        # Use in-memory links grouped by campaign to count links
        num_links = len(campaign_links.get(campaign_id, []))
        print(f"Number of links for campaign {campaign_id}: {num_links}")
        print(f"Number of hits for campaign {campaign_id}: {num_hits}")
        print(f"Number of targets for campaign {campaign_id}: {num_targets}")
        print(f"Number of unique IPs for campaign {campaign_id}: {num_unique_ips}")
        
        update_data = {
            "totals.targets": num_targets,
            "totals.links": num_links,
            "totals.hits": num_hits,
            "totals.unique_ips": num_unique_ips,
            "updated_at": SERVER_TIMESTAMP,
        }
        if campaign_id in campaign_last_hit:
            update_data["last_hit_at"] = campaign_last_hit[campaign_id]
        
        # Use set() with merge=True for dot notation fields (update() doesn't work well with dot notation)
        campaign_ref.set(update_data, merge=True)
    
    # Update businesses
    for business_ref, _ in businesses:
        business_id = business_ref.id
        hit_count = business_hits.get(business_id, 0)
        update_data = {"hit_count": hit_count}
        if business_id in business_last_hit:
            update_data["last_hit_at"] = business_last_hit[business_id]
        business_ref.update(update_data)
    
    # Update customer business overlays
    for business_ref, _ in businesses:
        business_id = business_ref.id
        hit_count = business_hits.get(business_id, 0)
        if hit_count > 0 or business_id in business_last_hit:
            customer_business_ref = db.collection('customers').document(owner_id).collection('businesses').document(business_id)
            update_data = {"hit_count": hit_count, "updated_at": SERVER_TIMESTAMP}
            if business_id in business_last_hit:
                update_data["last_hit_at"] = business_last_hit[business_id]
            customer_business_ref.set(update_data, merge=True)
    
    print("✓ Updated aggregates")


def generate_calls(
    db: firestore.Client,
    owner_id: str,
    businesses: List[Tuple[firestore.DocumentReference, Dict]],
    business_pct: float = 0.20,
    dry_run: bool = False,
) -> Tuple[int, List[str]]:
    """
    Create demo call logs for a random subset of businesses.

    Returns (total_calls_created, business_ids that received calls).
    """
    if not businesses:
        return 0, []

    k = max(1, round(len(businesses) * business_pct))
    k = min(k, len(businesses))
    selected = random.sample(businesses, k)

    total_calls = 0
    businesses_with_calls: List[str] = []

    for business_ref, _ in selected:
        business_id = business_ref.id
        num_calls = random.choices([1, 2, 3], weights=[70, 25, 5], k=1)[0]
        use_callback = num_calls >= 2 and random.random() < 0.05
        first_call_id: Optional[str] = None

        for call_idx in range(num_calls):
            call_process = random_call_process()
            call_time = random_call_time_within_weeks(4)
            call_kind = "outbound_attempt"
            callback_of_call_id = None
            callback_origin = None

            if use_callback and call_idx == 1 and first_call_id:
                call_kind = "owner_callback"
                callback_of_call_id = first_call_id
                callback_origin = "owner"

            payload = {
                "customer_id": owner_id,
                "business_id": business_id,
                "business_ref": business_ref,
                "owner_id": owner_id,
                "created_by_uid": owner_id,
                "status": "successful",
                "call_process": call_process,
                "call_note": sample_demo_call_note(),
                "created_at": call_time,
                "updated_at": call_time,
                "call_time": call_time,
                "call_kind": call_kind,
                "callback_of_call_id": callback_of_call_id,
                "callback_origin": callback_origin,
                "is_demo": True,
            }

            if not dry_run:
                doc_ref = firestore_add_ref(db.collection(CALLS_COLLECTION), payload)
                call_id = doc_ref.id
            else:
                call_id = f"dry-run-{uuid.uuid4().hex[:12]}"

            if call_idx == 0:
                first_call_id = call_id
            total_calls += 1

        businesses_with_calls.append(business_id)
        action = "Would create" if dry_run else "Created"
        print(f"  ✓ {action} {num_calls} call(s) for business: {business_id}")

    return total_calls, businesses_with_calls


def backfill_call_overlays(
    db: firestore.Client,
    owner_id: str,
    business_ids: List[str],
) -> None:
    """Set latest_call_process on customer business overlays from newest demo call."""
    if not business_ids:
        return

    business_id_set = set(business_ids)
    latest_by_business: Dict[str, Tuple[object, Dict]] = {}

    calls_query = (
        db.collection(CALLS_COLLECTION)
        .where("customer_id", "==", owner_id)
        .where("is_demo", "==", True)
    )
    for doc in calls_query.stream():
        data = doc.to_dict() or {}
        bid = data.get("business_id")
        if not bid or bid not in business_id_set:
            continue
        call_time = data.get("call_time")
        call_process = data.get("call_process")
        if not call_process:
            continue

        prev = latest_by_business.get(bid)
        if prev is None:
            latest_by_business[bid] = (call_time, call_process)
            continue
        prev_time = prev[0]
        try:
            if call_time and (prev_time is None or call_time > prev_time):
                latest_by_business[bid] = (call_time, call_process)
        except (TypeError, AttributeError):
            if call_time is not None:
                latest_by_business[bid] = (call_time, call_process)

    batch = db.batch()
    batch_count = 0
    for business_id, (_, call_process) in latest_by_business.items():
        overlay_ref = (
            db.collection("customers")
            .document(owner_id)
            .collection("businesses")
            .document(business_id)
        )
        batch.set(
            overlay_ref,
            {
                "latest_call_process": call_process,
                "updated_at": SERVER_TIMESTAMP,
            },
            merge=True,
        )
        batch_count += 1
        if batch_count >= 500:
            batch.commit()
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    print(f"✓ Backfilled latest_call_process on {len(latest_by_business)} overlays")


def cleanup_demo_data(
    db: firestore.Client,
    owner_id: str,
    dry_run: bool = False
) -> None:
    """Delete all demo data marked with is_demo flag."""
    print("🧹 Cleaning up existing demo data...")
    
    deleted_counts = {
        "hits": 0,
        "calls": 0,
        "links": 0,
        "targets": 0,
        "campaigns": 0,
        "businesses": 0,
        "customer_businesses": 0,
    }
    
    # Delete hits with is_demo flag
    if not dry_run:
        hits_query = db.collection("hits").where("is_demo", "==", True).stream()
        batch = db.batch()
        batch_count = 0
        for hit in hits_query:
            batch.delete(hit.reference)
            batch_count += 1
            deleted_counts["hits"] += 1
            if batch_count >= 500:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        if batch_count > 0:
            batch.commit()
    else:
        # Count in dry run
        hits_query = db.collection("hits").where("is_demo", "==", True).stream()
        deleted_counts["hits"] = sum(1 for _ in hits_query)

    # Delete demo calls for this customer
    if not dry_run:
        calls_query = (
            db.collection(CALLS_COLLECTION)
            .where("is_demo", "==", True)
            .where("customer_id", "==", owner_id)
        )
        batch = db.batch()
        batch_count = 0
        for call in calls_query.stream():
            batch.delete(call.reference)
            batch_count += 1
            deleted_counts["calls"] += 1
            if batch_count >= 500:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        if batch_count > 0:
            batch.commit()
    else:
        calls_query = (
            db.collection(CALLS_COLLECTION)
            .where("is_demo", "==", True)
            .where("customer_id", "==", owner_id)
        )
        deleted_counts["calls"] = sum(1 for _ in calls_query.stream())
    
    # Delete links with is_demo flag
    if not dry_run:
        links_query = db.collection("links").where("is_demo", "==", True).stream()
        batch = db.batch()
        batch_count = 0
        for link in links_query:
            batch.delete(link.reference)
            batch_count += 1
            deleted_counts["links"] += 1
            if batch_count >= 500:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        if batch_count > 0:
            batch.commit()
    else:
        links_query = db.collection("links").where("is_demo", "==", True).stream()
        deleted_counts["links"] = sum(1 for _ in links_query)
    
    # Delete campaigns with is_demo flag and their targets
    if not dry_run:
        campaigns_query = db.collection("campaigns").where("is_demo", "==", True).stream()
        batch = db.batch()
        batch_count = 0
        for campaign in campaigns_query:
            # Delete targets for this campaign (only demo targets)
            targets_query = campaign.reference.collection("targets").where("is_demo", "==", True).stream()
            for target in targets_query:
                batch.delete(target.reference)
                deleted_counts["targets"] += 1
                batch_count += 1
                if batch_count >= 500:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
            
            # Delete campaign
            batch.delete(campaign.reference)
            batch_count += 1
            deleted_counts["campaigns"] += 1
            if batch_count >= 500:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        if batch_count > 0:
            batch.commit()
    else:
        campaigns_query = db.collection("campaigns").where("is_demo", "==", True).stream()
        for campaign in campaigns_query:
            deleted_counts["campaigns"] += 1
            targets_query = campaign.reference.collection("targets").where("is_demo", "==", True).stream()
            deleted_counts["targets"] += sum(1 for _ in targets_query)
    
    # Resolve demo business IDs before deleting canonical businesses (overlays need the ref/doc).
    demo_business_ids = {
        doc.id
        for doc in db.collection("businesses").where("is_demo", "==", True).stream()
    }

    def _overlay_is_demo(overlay) -> bool:
        if overlay.id in demo_business_ids:
            return True
        overlay_data = overlay.to_dict() or {}
        business_id = overlay_data.get("business_id")
        if isinstance(business_id, str) and business_id in demo_business_ids:
            return True
        business_ref = overlay_data.get("business_ref")
        if business_ref:
            business_doc = business_ref.get()
            if business_doc.exists and (business_doc.to_dict() or {}).get("is_demo"):
                return True
        return False

    # Delete customer business overlays while demo businesses still exist.
    customer_businesses_ref = (
        db.collection("customers").document(owner_id).collection("businesses")
    )
    if not dry_run:
        batch = db.batch()
        batch_count = 0
        for overlay in customer_businesses_ref.stream():
            if not _overlay_is_demo(overlay):
                continue
            batch.delete(overlay.reference)
            batch_count += 1
            deleted_counts["customer_businesses"] += 1
            if batch_count >= 500:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        if batch_count > 0:
            batch.commit()
    else:
        for overlay in customer_businesses_ref.stream():
            if _overlay_is_demo(overlay):
                deleted_counts["customer_businesses"] += 1

    # Delete businesses with is_demo flag (after overlays).
    if not dry_run:
        batch = db.batch()
        batch_count = 0
        for business_id in demo_business_ids:
            batch.delete(db.collection("businesses").document(business_id))
            batch_count += 1
            deleted_counts["businesses"] += 1
            if batch_count >= 500:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        if batch_count > 0:
            batch.commit()
    else:
        deleted_counts["businesses"] = len(demo_business_ids)
    
    print(f"✓ Cleanup complete:")
    for collection, count in deleted_counts.items():
        if count > 0:
            action = "Would delete" if dry_run else "Deleted"
            print(f"  {action} {count} {collection}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate coherent demo data for all Firestore collections"
    )
    parser.add_argument(
        "--owner-id",
        default=DEFAULT_OWNER_ID,
        help=f"Demo user ID (default: {DEFAULT_OWNER_ID})"
    )
    parser.add_argument(
        "--campaigns",
        type=int,
        default=2,
        help="Number of campaigns to create (default: 2)"
    )
    parser.add_argument(
        "--businesses",
        type=int,
        default=500,
        help="Number of businesses to create (default: 500)"
    )
    parser.add_argument(
        "--links-per-campaign",
        type=int,
        default=500,
        help="Max targets per campaign; all businesses get a link, up to this cap (default: 500)"
    )
    parser.add_argument(
        "--hits-per-link",
        type=int,
        default=10,
        help="Average number of hits per link (default: 10)"
    )
    parser.add_argument(
        "--calls-business-pct",
        type=float,
        default=0.20,
        help="Fraction of demo businesses that receive call logs (default: 0.20)"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        default=True,
        help="Delete existing demo data before creating new data (default: True)"
    )
    parser.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help="Don't delete existing demo data"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without writing to Firestore"
    )
    parser.add_argument(
        "--credentials",
        default=DEFAULT_CREDENTIALS_PATH,
        help=f"Path to Firebase credentials JSON (default: {DEFAULT_CREDENTIALS_PATH})"
    )
    
    args = parser.parse_args()
    
    # Initialize Firebase
    if not firebase_admin._apps:
        cred = credentials.Certificate(args.credentials)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    
    print("=" * 60)
    print("DEMO DATA GENERATOR")
    print("=" * 60)
    print(f"Owner ID: {args.owner_id}")
    print(f"Campaigns: {args.campaigns}")
    print(f"Businesses: {args.businesses}")
    print(f"Links per campaign: {args.links_per_campaign}")
    print(f"Hits per link (avg): {args.hits_per_link}")
    print(f"Calls business %%: {args.calls_business_pct * 100:.0f}")
    print(f"Cleanup: {args.cleanup}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 60)
    print()
    
    # Cleanup existing demo data
    if args.cleanup:
        cleanup_demo_data(db, args.owner_id, args.dry_run)
        print()
    
    # Generate data in correct order
    print("📦 Generating demo data...")
    print()
    
    # 1. Generate campaigns
    print(f"1. Generating {args.campaigns} campaigns...")
    campaigns = generate_campaigns(db, args.owner_id, args.campaigns, args.dry_run)
    print()
    
    # 2. Generate businesses
    print(f"2. Generating {args.businesses} businesses...")
    businesses = generate_businesses(db, args.owner_id, args.businesses, args.dry_run)
    print()
    
    # 3. Generate targets and links for each campaign (one link doc per business, reused across campaigns)
    all_links = []
    business_link_cache: Dict[str, firestore.DocumentReference] = {}
    for i, campaign_ref in enumerate(campaigns):
        if not args.dry_run:
            campaign_data = campaign_ref.get().to_dict()
            campaign_name = campaign_data.get("campaign_name", f"Campaign {i+1}") if campaign_data else f"Campaign {i+1}"
        else:
            campaign_name = f"Campaign {i+1}"
        
        print(f"3.{i+1}. Generating targets and links for campaign {i+1}...")
        targets = generate_targets(db, campaign_ref, businesses, args.links_per_campaign, args.dry_run)
        links = generate_links(
            db,
            campaign_ref,
            campaign_name,
            targets,
            args.owner_id,
            business_link_cache,
            args.dry_run,
        )
        all_links.extend(links)
        print()
    
    # 4. Generate hits
    print("4. Generating hits...")
    total_hits = 0
    for i, campaign_ref in enumerate(campaigns):
        if not args.dry_run:
            campaign_data = campaign_ref.get().to_dict()
            campaign_name = campaign_data.get("campaign_name", f"Campaign {i+1}") if campaign_data else f"Campaign {i+1}"
        else:
            campaign_name = f"Campaign {i+1}"
        
        # Get links for this campaign (links are tuples: (link_ref, business_ref, target_ref, campaign_ref))
        campaign_links = [link for link in all_links if link[3].id == campaign_ref.id]
        
        hits = generate_hits(db, campaign_links, campaign_name, args.owner_id, args.hits_per_link, args.dry_run)
        total_hits += hits
    print()
    
    # 5. Update aggregates
    if not args.dry_run:
        print("5. Updating aggregates...")
        update_aggregates(db, campaigns, all_links, businesses, args.owner_id, args.dry_run)
        print()

    # 6. Generate calls
    print("6. Generating calls...")
    total_calls, businesses_with_calls = generate_calls(
        db,
        args.owner_id,
        businesses,
        args.calls_business_pct,
        args.dry_run,
    )
    if not args.dry_run and businesses_with_calls:
        print("   Backfilling latest_call_process on overlays...")
        backfill_call_overlays(db, args.owner_id, businesses_with_calls)
    print()
    
    print("=" * 60)
    print("✅ DONE")
    print("=" * 60)
    print(f"Created:")
    print(f"  - {len(campaigns)} campaigns")
    print(f"  - {len(businesses)} businesses")
    print(f"  - {len(business_link_cache)} links ({len(all_links)} campaign targets with links)")
    print(f"  - {total_hits} hits")
    print(f"  - {total_calls} calls ({len(businesses_with_calls)} businesses)")
    print("=" * 60)


if __name__ == "__main__":
    random.seed()  # Seed for reproducibility (can be made configurable)
    main()

