"""
Seed Firestore 'hits' collection with demo data (authenticated via JSON credentials).

Setup:
  1. Download your Firebase service account JSON from:
     Firebase Console → Project Settings → Service Accounts → Generate new private key
  2. Set FIREBASE_CREDENTIALS_PATH below to that JSON file's absolute path.
  3. Run:  python seed_hits_demo.py
"""

import random
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials, firestore

# --- 🔧 CONFIGURATION ---
FIREBASE_CREDENTIALS_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json" #should always be dev
NUM_DOCS = 26  # number of demo docs to create
OWNER_ID = "4qXpgdIK0vTg7wThQ89OHWMUlf13"

BUSINESS_REF_PATH = "businesses/4-advice-GmbH-53177"
CAMPAIGN_REF_PATH = "campaigns/eebb74b8-6c5b-44b9-96ab-f4b6e3505206"
TARGET_REF_PATH   = "campaigns/eebb74b8-6c5b-44b9-96ab-f4b6e3505206/targets/1Ooy6wos1S7UzloMPP5K"

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
CITIES = [
    ("Munich", "Bavaria", 48.1372, 11.5756),
    ("Berlin", "Berlin", 52.5200, 13.4050),
    ("Hamburg", "Hamburg", 53.5511, 9.9937),
    ("Cologne", "North Rhine-Westphalia", 50.9375, 6.9603),
    ("Frankfurt", "Hesse", 50.1109, 8.6821),
]
BROWSERS = ["Chrome 139.0.0", "Firefox 130.0", "Safari 17.5", "Edge 128.0"]
OSES = ["Mac OS X 10.15.7", "Windows 11", "Ubuntu 22.04", "iOS 17.6", "Android 14"]
USER_AGENT_TEMPLATES = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser} Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv/130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser} Mobile Safari/537.36",
]


# --- 🔥 FIREBASE INIT ---
cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()


# --- HELPERS ---
def random_timestamp_within_last_14_days() -> datetime:
    now = datetime.now(timezone.utc)
    days_back = random.randint(0, 14)
    seconds_in_day = random.randint(0, 24 * 3600 - 1)
    ts = (now - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
    ts += timedelta(seconds=seconds_in_day)
    return min(ts, now)


def small_jitter(v: float, max_abs_delta: float = 0.01) -> float:
    return v + random.uniform(-max_abs_delta, max_abs_delta)


# --- MAIN SEEDING ---
def seed_demo_data():
    business_ref = db.document(BUSINESS_REF_PATH)
    campaign_ref = db.document(CAMPAIGN_REF_PATH)
    target_ref = db.document(TARGET_REF_PATH)

    for i in range(NUM_DOCS):
        city, region, lat, lon = random.choice(CITIES)
        device = random.choice(DEVICE_TYPES)
        browser = random.choice(BROWSERS)
        os_ = random.choice(OSES)
        ua_template = random.choice(USER_AGENT_TEMPLATES)
        user_agent = ua_template.format(browser=browser)

        doc = {
            "business_ref": business_ref,
            "campaign_ref": campaign_ref,
            "target_ref": target_ref,
            "geo_city": city,
            "geo_region": region,
            "geo_country": "DE",
            "geo_lat": round(small_jitter(lat), 6),
            "geo_lon": round(small_jitter(lon), 6),
            "geo_source": "api",
            "device_type": device,
            "ua_browser": browser,
            "ua_os": os_,
            "user_agent": user_agent,
            "ip_hash": "6d1c7ed813d50e8349259aea620e9d8a8c58a373145e2e261a7aee6d13d4a7b7",
            "link_id": f"CSV5-PRE-{i+1}",
            "owner_id": OWNER_ID,
            "template_id": "template_standart_qr_track.pdf",
            "ts": random_timestamp_within_last_14_days(),
            "is_demo": True,  # flag for easy cleanup
        }

        doc_ref = db.collection("hits").document()
        doc_ref.set(doc)
        print(f"Created demo hit: {doc_ref.id} ({city}, {device})")

    print(f"\n✅ Done. Inserted {NUM_DOCS} demo docs into 'hits'.")


# --- 🧹 OPTIONAL CLEANUP ---
def delete_demo_data():
    hits_ref = db.collection("hits")
    docs = hits_ref.where("is_demo", "==", True).stream()
    count = 0
    for doc in docs:
        doc.reference.delete()
        count += 1
    print(f"🧹 Deleted {count} demo docs from 'hits'.")


# --- RUN ---
if __name__ == "__main__":
    random.seed()
    seed_demo_data()
    #delete_demo_data()  # uncomment if you want to clean up later
