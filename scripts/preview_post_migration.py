#!/usr/bin/env python3
"""
preview_post_migration.py

Read-only preview of how Firestore documents WOULD look after the ownership migration.
Writes a small JSON report so you can verify correctness before committing any changes.

Usage examples:
  python scripts/preview_post_migration.py \
    --project gb-qr-tracker \
    --map data/migration_customers.json \
    --fallback-uid 9xVVm8dhRhNmInut2bYcUfyQeTV2 \
    --limit 8 \
    --out preview_report.json \
    --delete-legacy

If you have a single customer in prod, you can skip --map and just pass --fallback-uid.
"""

import argparse
import json
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import firestore, auth


def init_admin(project: str):
    if not firebase_admin._apps:
        firebase_admin.initialize_app(options={"projectId": project})
    return firestore.client()


def load_mapping(path: str | None) -> dict[str, str]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("--map must be a JSON object {label: uid_or_email}")
    return {str(k): str(v) for k, v in data.items()}


def resolve_uid(target: str) -> str | None:
    # Treat string as uid first, then fallback to email.
    try:
        return auth.get_user(target).uid
    except Exception:
        pass
    try:
        return auth.get_user_by_email(target).uid
    except Exception:
        return None


def make_label_to_uid(map_json: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for label, tgt in map_json.items():
        uid = resolve_uid(tgt)
        if uid:
            out[label] = uid
    return out


def to_str(v):
    # Pretty-print a few common Firestore types
    try:
        # Firestore Timestamp has .isoformat() via datetime
        if hasattr(v, "isoformat"):
            return v.isoformat()
    except Exception:
        pass
    return v


def collect_link_owner_map(db) -> dict[str, str]:
    """
    Build {link_id -> owner_id} for all links that already have owner_id.
    Used to derive hits.owner_id and businesses.ownerIds in preview.
    """
    m: dict[str, str] = {}
    for d in db.collection("links").stream():
        data = d.to_dict() or {}
        oid = data.get("owner_id")
        if oid:
            m[d.id] = oid
    return m


def collect_business_owners_from_links(db) -> dict[str, set[str]]:
    """
    Build {business_id -> set(owner_ids)} from links that have both business_id and owner_id.
    """
    m: dict[str, set[str]] = {}
    for d in db.collection("links").stream():
        data = d.to_dict() or {}
        biz = data.get("business_id")
        oid = data.get("owner_id")
        if biz and oid:
            m.setdefault(biz, set()).add(oid)
    return m


def main():
    ap = argparse.ArgumentParser(description="Read-only JSON preview of post-migration state")
    ap.add_argument("--project", required=True)
    ap.add_argument("--map", help="JSON mapping {customer_label: uid_or_email}")
    ap.add_argument("--fallback-uid", help="UID to use if mapping missing")
    ap.add_argument("--limit", type=int, default=5, help="Max samples per collection")
    ap.add_argument("--out", default="preview_report.json", help="Output JSON file")
    ap.add_argument("--delete-legacy", action="store_true",
                    help="Preview as if legacy 'customer' field would be removed")
    args = ap.parse_args()

    db = init_admin(args.project)

    mapping_raw = load_mapping(args.map)
    label_to_uid = make_label_to_uid(mapping_raw) if mapping_raw else {}

    # Build helpers once
    link_owner = collect_link_owner_map(db)  # link_id -> owner_id (existing)
    biz_owners_from_links = collect_business_owners_from_links(db)  # business_id -> owners (from links)

    # --- counts (current) ---
    counts = {
        "links_missing_owner_id": 0,
        "hits_missing_owner_id": 0,
        "businesses_without_ownerIds_array": 0
    }

    # --- preview links ---
    links_samples = []
    for d in db.collection("links").stream():
        data = d.to_dict() or {}
        if not data.get("owner_id"):
            counts["links_missing_owner_id"] += 1

        if len(links_samples) >= args.limit:
            continue

        needs = not data.get("owner_id")
        if not needs:
            # Only sample missing ones to keep report small
            continue

        label = data.get("customer")
        uid = None
        if label:
            uid = label_to_uid.get(label)
        if not uid and args.fallback_uid:
            uid = args.fallback_uid

        after = dict(data)
        if uid:
            after["owner_id"] = uid
        if args.delete_legacy and "customer" in after:
            del after["customer"]

        links_samples.append({
            "id": d.id,
            "before": {
                "owner_id": data.get("owner_id"),
                "customer": data.get("customer"),
                "campaign": data.get("campaign"),
                "last_hit_at": to_str(data.get("last_hit_at")),
            },
            "after": {
                "owner_id": after.get("owner_id"),
                "customer": after.get("customer", None),
                "campaign": after.get("campaign"),
                "last_hit_at": to_str(after.get("last_hit_at")),
            },
            "would_update": bool(uid)
        })

    # --- preview hits ---
    hits_samples = []
    for d in db.collection("hits").stream():
        data = d.to_dict() or {}
        if not data.get("owner_id"):
            counts["hits_missing_owner_id"] += 1

        if len(hits_samples) >= args.limit:
            continue

        needs = not data.get("owner_id")
        if not needs:
            continue

        label = data.get("customer")
        uid = None
        if label:
            uid = label_to_uid.get(label)
        if not uid:
            lid = data.get("link_id")
            if lid and link_owner.get(lid):
                uid = link_owner[lid]
        if not uid and args.fallback_uid:
            uid = args.fallback_uid

        after = dict(data)
        if uid:
            after["owner_id"] = uid
        if args.delete_legacy and "customer" in after:
            del after["customer"]

        hits_samples.append({
            "id": d.id,
            "before": {
                "owner_id": data.get("owner_id"),
                "customer": data.get("customer"),
                "link_id": data.get("link_id"),
                "ts": to_str(data.get("ts")),
            },
            "after": {
                "owner_id": after.get("owner_id"),
                "customer": after.get("customer", None),
                "link_id": after.get("link_id"),
                "ts": to_str(after.get("ts")),
            },
            "would_update": bool(uid)
        })

    # --- preview businesses ---
    # Sample first N businesses; derive new ownerIds as union(current, owners_from_links)
    businesses_samples = []
    sampled_biz_ids = []
    for d in db.collection("businesses").stream():
        if len(businesses_samples) >= args.limit:
            break
        sampled_biz_ids.append(d.id)
        data = d.to_dict() or {}
        cur = data.get("ownerIds")
        if not isinstance(cur, list):
            counts["businesses_without_ownerIds_array"] += 1
        derived = sorted(list(biz_owners_from_links.get(d.id, set())))
        union = sorted(list(set(cur or []) | set(derived)))
        businesses_samples.append({
            "id": d.id,
            "before": {
                "ownerIds": cur,
                "business_name": data.get("business_name") or data.get("name")
            },
            "derived_from_links": derived,
            "after": {
                "ownerIds": union,
                "business_name": data.get("business_name") or data.get("name")
            },
            "would_update": (cur != union)
        })

    report = {
        "project": args.project,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "assumptions": {
            "delete_legacy_customer_field": args.delete_legacy,
            "fallback_uid": args.fallback_uid,
            "mapping_labels_loaded": sorted(list((mapping_raw := load_mapping(args.map)).keys())) if args.map else [],
        },
        "counts_now": counts,
        "samples": {
            "links": links_samples,
            "hits": hits_samples,
            "businesses": businesses_samples
        }
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"[ok] wrote preview to {args.out}")
    print("(no writes were made to Firestore)")


if __name__ == "__main__":
    main()
