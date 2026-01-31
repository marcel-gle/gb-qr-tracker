#!/usr/bin/env python3
"""
Run the delete_campaign workflow locally (same behavior as the HTTP Cloud Function).

This script imports and reuses the logic from functions/delete_campaign/main.py
without modifying it. It sets project and credentials for the chosen env, then
calls the same count/delete helpers. See API.md "Delete Campaign API" for the
HTTP contract this mirrors.

Usage:
    python scripts/local_delete_campaign.py --env dev --campaign-id <id> --dry-run
    python scripts/local_delete_campaign.py --env dev --campaign-id <id> --confirm

Storage cleanup is always part of the deletion: bucket and prefix are derived from
--env and the campaign document (owner_id). Ownership is not enforced for local runs.
"""

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _get_env_config(env: str) -> tuple[str, str, str]:
    """Return (project_id, default_credentials_path, storage_bucket) for dev or prod."""
    if env == "prod":
        project_id = "gb-qr-tracker"
        default_creds = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"
        bucket = "gb-qr-tracker.firebasestorage.app"
    else:
        project_id = "gb-qr-tracker-dev"
        default_creds = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
        bucket = "gb-qr-tracker-dev.firebasestorage.app"
    return project_id, default_creds, bucket


def _load_delete_campaign_module(repo_root: Path):
    """Load functions/delete_campaign/main.py as a module (after env is set)."""
    main_path = repo_root / "functions" / "delete_campaign" / "main.py"
    if not main_path.is_file():
        raise FileNotFoundError(f"Cloud function main not found: {main_path}")
    spec = importlib.util.spec_from_file_location("delete_campaign_main", main_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load spec for {main_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run delete_campaign logic locally (mirrors Cloud Function).",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Target environment (project and credentials). Default: dev",
    )
    parser.add_argument(
        "--campaign-id",
        required=True,
        help="Campaign document ID to delete.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only compute and print counts; do not delete.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required for actual deletion (safety).",
    )
    args = parser.parse_args()

    repo_root = _repo_root()
    project_id, default_creds, default_bucket = _get_env_config(args.env)

    # Set env before importing the function module so Firestore/Storage use correct project and credentials
    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
    os.environ["GCP_PROJECT"] = project_id
    os.environ["DATABASE_ID"] = "(default)"
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = default_creds

    print("Local delete_campaign: ownership check skipped (service account).")
    print(f"PROJECT_ID={project_id} DATABASE_ID=(default)")

    cf_main = _load_delete_campaign_module(repo_root)
    campaign_ref = cf_main.COL_CAMPAIGNS.document(args.campaign_id)
    camp_snap = campaign_ref.get()

    if not camp_snap.exists:
        print(json.dumps({"ok": True, "message": "Campaign not found (already deleted?)"}))
        return 0

    # Storage cleanup is always part of the process: derive bucket and prefix from env + campaign
    owner_id = camp_snap.get("owner_id") or ""
    bucket_name = default_bucket
    storage_prefix = f"uploads/{args.env}/{owner_id}/{args.campaign_id}/" if owner_id else None
    if not owner_id:
        print("Warning: campaign has no owner_id; storage cleanup will be skipped.", file=sys.stderr)

    # Build plan (same counts as the function)
    targets_count = cf_main._count_targets_for_campaign(campaign_ref)
    unique_ips_count = cf_main._count_unique_ips_for_campaign(campaign_ref)
    links_count = cf_main._count_links_for_campaign(campaign_ref)
    hits_count = cf_main._count_hits_for_campaign(campaign_ref)

    plan = {
        "counts": {
            "targets": targets_count,
            "uniqueIps": unique_ips_count,
            "links": links_count,
            "hits": hits_count,
            "businessesToMaybeDelete": 0,
            "businessesPrunable": 0,
            "campaignDoc": 1,
            "storage": 0,
        },
        "storage": {
            "bucket": bucket_name,
            "prefix": storage_prefix,
        },
    }

    if bucket_name and storage_prefix:
        try:
            bucket = cf_main.storage_client.bucket(bucket_name)
            plan["counts"]["storage"] = sum(1 for _ in bucket.list_blobs(prefix=storage_prefix))
        except Exception:
            pass

    print("Delete plan:", json.dumps(plan, indent=2))

    if args.dry_run or not args.confirm:
        print(json.dumps({"ok": True, "dryRun": args.dry_run, "plan": plan}, indent=2))
        return 0

    # Execute (same order as the function)
    cf_main._delete_hits_for_campaign(campaign_ref)
    cf_main._delete_targets_for_campaign(campaign_ref)
    cf_main._delete_unique_ips_for_campaign(campaign_ref)
    cf_main._delete_links_for_campaign(campaign_ref)
    campaign_ref.delete()

    deleted_blobs = 0
    if bucket_name and storage_prefix:
        print("Deleting storage...", bucket_name, storage_prefix)
        deleted_blobs = cf_main._delete_storage_prefix(bucket_name, storage_prefix)

    result = {
        "ok": True,
        "deleted": {
            "hits": hits_count,
            "targets": targets_count,
            "unique_ips": unique_ips_count,
            "links": links_count,
            "businesses": 0,
            "campaignDoc": 1,
            "bucket_name": bucket_name,
            "storage_prefix": storage_prefix,
            "storageBlobs": deleted_blobs,
        },
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
