from firebase_admin import credentials, firestore, initialize_app
import csv

# --- Firestore init (prod) ---
cred = credentials.Certificate(
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"#Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"
)
initialize_app(cred)
db = firestore.client()

campaign_id = "fc7b4541-0eef-4bd8-958e-52bf4c122da7"
campaign_ref = db.collection("campaigns").document(campaign_id)

# 1) IDs currently in Firestore
existing_ids = {doc.id for doc in db.collection("links")
                              .where("campaign_ref", "==", campaign_ref)
                              .stream()}

print("links in Firestore:", len(existing_ids))

# 2) IDs expected from CSV
expected_ids = set()
with open("/Users/marcelgleich/Downloads/target_list_with_links_groe.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        tid = (row.get("tracking_id") or "").strip()
        if tid:
            expected_ids.add(tid)

print("links in CSV:", len(expected_ids))

missing_ids = expected_ids - existing_ids
print("missing link docs:", len(missing_ids))

# Optional: dump a small sample for manual inspection
for tid in list(sorted(missing_ids))[:50]:
    print("MISSING:", tid)