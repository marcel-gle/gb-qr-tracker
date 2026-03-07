#!/usr/bin/env python3
"""Quick diagnostic to check a specific business and its overlay."""

from google.cloud import firestore

db = firestore.Client(project="gb-qr-tracker-dev", database="test")

BUSINESS_ID = "secure-it-gmbh-92706"
OWNER_ID = "xLRk37rnV7T4CbOXzW5N3saxVfy1"

# 1. Check the canonical business document
print("=== Canonical business ===")
biz_ref = db.collection("businesses").document(BUSINESS_ID)
biz_snap = biz_ref.get()
if biz_snap.exists:
    data = biz_snap.to_dict()
    print(f"  Document ID:  {biz_snap.id}")
    print(f"  business_id:  {data.get('business_id')}")
    print(f"  ownerIds:     {data.get('ownerIds')}")
    print(f"  owner_id:     {data.get('owner_id')}")
    print(f"  business_name:{data.get('business_name')}")
    print(f"  All keys:     {sorted(data.keys())}")
else:
    print(f"  Document businesses/{BUSINESS_ID} does NOT exist!")
    # Try to find by business_id field
    print("  Searching by business_id field...")
    results = list(
        db.collection("businesses")
        .where("business_id", "==", BUSINESS_ID)
        .limit(5)
        .stream()
    )
    for r in results:
        d = r.to_dict()
        print(f"  Found: doc_id={r.id}, ownerIds={d.get('ownerIds')}, owner_id={d.get('owner_id')}")

# 2. Check the overlay document
print(f"\n=== Overlay: customers/{OWNER_ID}/businesses/{BUSINESS_ID} ===")
overlay_ref = (
    db.collection("customers")
    .document(OWNER_ID)
    .collection("businesses")
    .document(BUSINESS_ID)
)
overlay_snap = overlay_ref.get()
if overlay_snap.exists:
    data = overlay_snap.to_dict()
    print(f"  EXISTS - keys: {sorted(data.keys())}")
else:
    print(f"  DOES NOT EXIST")

# 3. Check what overlays DO exist for this owner
print(f"\n=== All overlays for owner {OWNER_ID} ===")
overlays = list(
    db.collection("customers")
    .document(OWNER_ID)
    .collection("businesses")
    .limit(20)
    .stream()
)
print(f"  Found {len(overlays)} overlays (showing up to 20)")
for o in overlays[:20]:
    d = o.to_dict()
    print(f"  - {o.id}  (business_ref={d.get('business_ref')}, name={d.get('name')})")

# 4. Check with get_all (same method the migration uses)
print(f"\n=== get_all() check ===")
snaps = list(db.get_all([overlay_ref]))
for snap in snaps:
    print(f"  path={snap.reference.path}, exists={snap.exists}")
