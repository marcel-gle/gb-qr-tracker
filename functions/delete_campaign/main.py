import os
import json
from typing import Dict, List, Set
from google.cloud import firestore, storage
from google.api_core.exceptions import NotFound
from flask import Request
import functions_framework

# Optional: verify Firebase ID token
try:
    import firebase_admin
    from firebase_admin import auth as fb_auth
    firebase_admin.initialize_app()  # uses default credentials
except Exception:
    fb_auth = None  # if you prefer IAM-only auth, handle below

PROJECT_ID  = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT") or "gb-qr-tracker-dev"
DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")

db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
storage_client = storage.Client(project=PROJECT_ID)

COL_LINKS = db.collection("links")
COL_HITS = db.collection("hits")
COL_CAMPAIGNS = db.collection("campaigns")
COL_BUSINESSES = db.collection("businesses")

BATCH_SIZE = 400

def _json(req: Request) -> Dict:
    try:
        return req.get_json(silent=True) or {}
    except Exception:
        return {}

def _verify_firebase_token(req: Request) -> str:
    """Returns caller uid or raises."""
    if fb_auth is None:
        raise PermissionError("Auth not configured on server (firebase_admin missing).")
    authz = req.headers.get("Authorization", "")
    if not authz.startswith("Bearer "):
        raise PermissionError("Missing bearer token.")
    id_token = authz.split(" ", 1)[1].strip()
    decoded = fb_auth.verify_id_token(id_token, check_revoked=True)
    return decoded["uid"]

def _delete_in_batches(doc_refs: List[firestore.DocumentReference]):
    batch = db.batch()
    ops = 0
    for ref in doc_refs:
        batch.delete(ref); ops += 1
        if ops >= BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            ops = 0
    if ops:
        batch.commit()

def _list_targets(campaign_ref) -> List[firestore.DocumentReference]:
    return [d.reference for d in campaign_ref.collection("targets").stream()]

def _list_unique_ips(campaign_ref) -> List[firestore.DocumentReference]:
    return [d.reference for d in campaign_ref.collection("unique_ips").stream()]

def _list_links(campaign_ref) -> List[firestore.DocumentReference]:
    # links store a reference field "campaign_ref"
    return [d.reference for d in COL_LINKS.where("campaign_ref", "==", campaign_ref).stream()]

def _list_hits(campaign_ref) -> List[firestore.DocumentReference]:
    # links store a reference field "campaign_ref"
    return [d.reference for d in COL_HITS.where("campaign_ref", "==", campaign_ref).stream()]

def _list_hits_for_links(link_refs: List[firestore.DocumentReference]) -> List[firestore.DocumentReference]:
    """Currently unsued, might delete later"""
    # hits have a reference field "link_ref"
    out: List[firestore.DocumentReference] = []
    # Chunk 'IN' queries to <= 30 refs per call (Firestore limit)
    CHUNK = 30
    for i in range(0, len(link_refs), CHUNK):
        refs_chunk = link_refs[i:i+CHUNK]
        q = COL_HITS.where("link_ref", "in", refs_chunk)
        out.extend([d.reference for d in q.stream()])
    return out


def _list_businesses_from_links(link_refs: List[firestore.DocumentReference]) -> Set[firestore.DocumentReference]:
    biz: Set[firestore.DocumentReference] = set()
    CHUNK = 30
    for i in range(0, len(link_refs), CHUNK):
        refs_chunk = link_refs[i:i+CHUNK]
        for snap in COL_LINKS.where("__name__", "in", refs_chunk).stream():
            ref = snap.get("business_ref")
            if ref:
                biz.add(ref)
    return biz

def _is_business_unused(biz_ref: firestore.DocumentReference) -> bool:
    # If no other link references this business, it's safe to delete
    q = COL_LINKS.where("business_ref", "==", biz_ref).limit(1).stream()
    return next(q, None) is None

def _delete_storage_prefix(bucket_name: str, prefix: str) -> int:
    try:
        bucket = storage_client.bucket(bucket_name)
    except NotFound:
        return 0
    n = 0
    for blob in bucket.list_blobs(prefix=prefix):
        blob.delete()
        n += 1
    return n

@functions_framework.http
def delete_campaign(request: Request):
    """
    HTTP (Gen 2) admin endpoint to delete a full campaign and related data.

    Body JSON:
      {
        "campaignId": "abc123",            # required
        "storage": {                       # optional, for Storage cleanup
          "bucket": "gb-qr-tracker.firebasestorage.app",
          "prefix": "uploads/prod/<ownerId>/<campaignId>/"
        },
        "deleteBusinesses": false,         # optional (default false): delete businesses if unused elsewhere
        "dryRun": true,                    # optional (default false): only return counts
        "confirm": true                    # required for destructive run
      }

    Auth:
      - Expects Firebase ID token in Authorization: Bearer <token>
      - Verifies caller owns the campaign (campaign.owner_id == uid) unless you allow admins
    """
    data = _json(request)

    # 1) Auth
    try:
        uid = _verify_firebase_token(request)
    except Exception as e:
        return (f"Unauthorized: {e}", 401)

    campaign_id = data.get("campaignId")
    if not campaign_id:
        return ("campaignId required", 400)

    delete_businesses = bool(data.get("deleteBusinesses", False))
    dry_run = bool(data.get("dryRun", False))
    confirm = bool(data.get("confirm", False))

    storage_cfg = data.get("storage") or {}
    bucket_name = storage_cfg.get("bucket")
    storage_prefix = storage_cfg.get("prefix")  # like uploads/prod/<owner>/<campaignId>/

    # 2) Ownership check
    campaign_ref = COL_CAMPAIGNS.document(campaign_id)
    camp_snap = campaign_ref.get()
    if not camp_snap.exists:
        return (json.dumps({"ok": True, "message": "Campaign not found (already deleted?)"}), 200)
    owner_id = camp_snap.get("owner_id")
    if owner_id and owner_id != uid:
        return ("Forbidden: not your campaign", 403)

    # 3) Plan
    target_refs = _list_targets(campaign_ref)
    unique_ip_refs = _list_unique_ips(campaign_ref)
    link_refs = _list_links(campaign_ref)
    hit_refs = _list_hits(campaign_ref)
    biz_refs = _list_businesses_from_links(link_refs) if delete_businesses else set() #will be null, this will not work with the current schema

    # Filter businesses to only those unused elsewhere
    prunable_biz_refs: List[firestore.DocumentReference] = []
    if delete_businesses and biz_refs:
        for b in biz_refs:
            if _is_business_unused(b):
                prunable_biz_refs.append(b)

    plan = {
        "counts": {
            "targets": len(target_refs),
            "uniqueIps": len(unique_ip_refs),
            "links": len(link_refs),
            "hits": len(hit_refs),
            "businessesToMaybeDelete": len(biz_refs),
            "businessesPrunable": len(prunable_biz_refs),
            "campaignDoc": 1,
            "storage": 0  # computed if we have a prefix and not a dryRun
        },
        "storage": {
            "bucket": bucket_name,
            "prefix": storage_prefix
        }
    }

    if dry_run or not confirm:
        # Optionally preview how many storage blobs exist
        if bucket_name and storage_prefix:
            try:
                bucket = storage_client.bucket(bucket_name)
                plan["counts"]["storage"] = sum(1 for _ in bucket.list_blobs(prefix=storage_prefix))
            except Exception:
                pass
        return (json.dumps({"ok": True, "dryRun": dry_run, "plan": plan}), 200)

    # 4) Execute (order matters)
    # hits → targets → links → (businesses optional) → campaign → storage
    _delete_in_batches(hit_refs)
    _delete_in_batches(target_refs)
    _delete_in_batches(unique_ip_refs)
    _delete_in_batches(link_refs)
    if prunable_biz_refs:
        _delete_in_batches(prunable_biz_refs)
    campaign_ref.delete()

    deleted_blobs = 0
    print("Deleting storage...", bucket_name, storage_prefix)
    if bucket_name and storage_prefix:
        deleted_blobs = _delete_storage_prefix(bucket_name, storage_prefix)

    return (json.dumps({
        "ok": True,
        "deleted": {
            "hits": len(hit_refs),
            "targets": len(target_refs),
            "unique_ips": len(unique_ip_refs),
            "links": len(link_refs),
            "businesses": len(prunable_biz_refs),
            "campaignDoc": 1,
            "bucket_name": bucket_name,
            "storage_prefix": storage_prefix,
            "storageBlobs": deleted_blobs
        }
    }), 200)
