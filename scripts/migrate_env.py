#!/usr/bin/env python3
"""
migrate_env.py

Clone selected parts of Firebase/GCP from PROD to DEV:

- Firestore: uses `gcloud firestore export/import`
- Storage: copies all objects between buckets (with uploads/prod → uploads/dev rewrite)
- Auth users: recreates users in DEV with same uid/email/custom claims

Usage examples:

    # Dry run (no changes)
    python migrate_env.py clone-firestore --dry-run
    python migrate_env.py clone-storage --dry-run
    python migrate_env.py clone-auth --dry-run

    # Real migration
    python migrate_env.py clone-firestore
    python migrate_env.py clone-storage
    python migrate_env.py clone-auth

Adjust CONFIG below to your environment.
"""

import argparse
import subprocess
import sys
from datetime import datetime, timezone

from google.cloud import storage
import firebase_admin
from firebase_admin import credentials, auth
from google.oauth2 import service_account



# ------------- CONFIG -------------

# Projects
PROD_PROJECT_ID = "gb-qr-tracker"
DEV_PROJECT_ID = "gb-qr-tracker-dev"

# Firestore database IDs
# Default DB: "(default)"
# Example non-default: "test-2"
PROD_DATABASE_ID = "(default)"
DEV_DATABASE_ID = "test"  # change to e.g. "test" if needed

# Firestore export bucket (must exist, usually in the prod project)
# Example: a dedicated backup bucket in gb-qr-tracker
FIRESTORE_EXPORT_BUCKET = "gs://gb-qr-tracker-prod-firestore-backup"

# Storage buckets (Firebase default buckets)
PROD_STORAGE_BUCKET = "gb-qr-tracker.firebasestorage.app"
DEV_STORAGE_BUCKET = "gb-qr-tracker-dev.firebasestorage.app"

# Service account JSON paths
PROD_SA_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-1b9e04b746.json"
DEV_SA_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"

# Default password for migrated users (users will need to change this on first login)
DEFAULT_PASSWORD = "ChangeMe123!"  # Change this to your desired default password

# ----------------------------------


def run_cmd(cmd: list[str]) -> None:
    """Run a shell command and exit on failure."""
    print(f"\n[run] {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"Command failed with code {result.returncode}", file=sys.stderr)
        sys.exit(result.returncode)


# ========== FIRESTORE CLONE (via gcloud export/import) ==========

def clone_firestore(dry_run: bool = False):
    """
    1. Export from PROD to a GCS bucket.
    2. Import into DEV from that export.

    NOTE:
    - Uses gcloud CLI.
    - DEV project's Firestore service account must have read access
      to the export bucket in PROD.
    - DEV data for imported collections may be overwritten.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    export_path = f"{FIRESTORE_EXPORT_BUCKET}/firestore-prod-export-{timestamp}"

    print("=== Firestore clone configuration ===")
    print(f"PROD project: {PROD_PROJECT_ID}, database: {PROD_DATABASE_ID}")
    print(f"DEV project:  {DEV_PROJECT_ID}, database: {DEV_DATABASE_ID}")
    print(f"Export path:  {export_path}")
    print(f"Dry run:      {dry_run}")
    print()

    export_cmd = [
        "gcloud",
        "firestore",
        "export",
        export_path,
        f"--database={PROD_DATABASE_ID}",
    ]
    import_cmd = [
        "gcloud",
        "firestore",
        "import",
        export_path,
        f"--database={DEV_DATABASE_ID}",
    ]

    if dry_run:
        print("=== DRY RUN: would execute ===")
        print(f"gcloud config set project {PROD_PROJECT_ID}")
        print(" ".join(export_cmd))
        print()
        print(f"gcloud config set project {DEV_PROJECT_ID}")
        print(" ".join(import_cmd))
        print("\n[DRY RUN] No Firestore export/import executed.")
        return

    print("=== Step 1: Export Firestore from PROD ===")
    run_cmd(["gcloud", "config", "set", "project", PROD_PROJECT_ID])
    run_cmd(export_cmd)

    print("\n=== Step 2: Import Firestore into DEV ===")
    run_cmd(["gcloud", "config", "set", "project", DEV_PROJECT_ID])
    run_cmd(import_cmd)

    print("\n[OK] Firestore clone finished.")
    print("Note: existing data in DEV may have been overwritten for imported collections.")


# ========== STORAGE CLONE (via google-cloud-storage) ==========

def init_storage_client(sa_path: str, project_id: str) -> storage.Client:
    """
    Create a google-cloud-storage client using a service account JSON key.
    """
    cred = service_account.Credentials.from_service_account_file(sa_path)
    return storage.Client(project=project_id, credentials=cred)



def clone_storage(dry_run: bool = False):
    """
    Copy all objects from PROD_STORAGE_BUCKET to DEV_STORAGE_BUCKET.

    Adjust folder prefix:
        uploads/prod/...  -->  uploads/dev/...

    Existing objects with same name in DEV will be overwritten.
    """
    print("=== Storage clone configuration ===")
    print(f"PROD bucket: {PROD_STORAGE_BUCKET}")
    print(f"DEV bucket:  {DEV_STORAGE_BUCKET}")
    print(f"Dry run:     {dry_run}")
    print()

    print("=== Initialize Storage clients ===")
    prod_client = init_storage_client(PROD_SA_PATH, PROD_PROJECT_ID)
    dev_client = init_storage_client(DEV_SA_PATH, DEV_PROJECT_ID)

    prod_bucket = prod_client.bucket(PROD_STORAGE_BUCKET)
    dev_bucket = dev_client.bucket(DEV_STORAGE_BUCKET)

    print(f"Listing objects in {PROD_STORAGE_BUCKET} ...")
    blobs = list(prod_bucket.list_blobs())
    total = len(blobs)
    print(f"Found {total} objects to copy\n")

    for i, blob in enumerate(blobs, start=1):
        src_name = blob.name

        # ---------- PATH REWRITE LOGIC ----------
        # prod: uploads/prod/{uid}/{campaignId}/...
        # dev:  uploads/dev/{uid}/{campaignId}/...
        if src_name.startswith("uploads/prod/"):
            dest_name = src_name.replace("uploads/prod/", "uploads/dev/", 1)
        else:
            # If the blob is outside this folder, keep path unchanged
            dest_name = src_name
        # ----------------------------------------

        print(f"[{i}/{total}] {src_name}  -->  {dest_name}")

        if dry_run:
            continue  # don't download/upload in dry run

        # Download & upload
        data = blob.download_as_bytes()
        new_blob = dev_bucket.blob(dest_name)
        new_blob.upload_from_string(data, content_type=blob.content_type)

        # Optional: copy simple metadata
        new_blob.cache_control = blob.cache_control
        new_blob.content_encoding = blob.content_encoding
        new_blob.content_language = blob.content_language
        new_blob.content_disposition = blob.content_disposition
        new_blob.patch()

    if dry_run:
        print("\n[DRY RUN] No objects were copied to DEV.")
    else:
        print("\n[OK] Storage clone finished with path rewrite applied.")


# ========== AUTH USERS CLONE (via firebase_admin) ==========

def init_firebase_app(sa_path: str, project_id: str, app_name: str):
    cred = credentials.Certificate(sa_path)
    try:
        return firebase_admin.get_app(app_name)
    except ValueError:
        return firebase_admin.initialize_app(
            cred,
            {"projectId": project_id},
            name=app_name,
        )


def clone_auth_users(dry_run: bool = False):
    """
    Copy users from PROD to DEV:

    - Keeps uid, email, email_verified, display_name, phone_number, disabled
    - Keeps custom_claims
    - Sets a default password for all users with email (see DEFAULT_PASSWORD config)
    - Handles email conflicts:
        * If UID exists in DEV -> update that user
        * Else if email exists with different UID in DEV -> delete that user, then create with PROD UID
        * Else -> create new user with PROD UID
    - Note: Password hashes cannot be copied from PROD (not accessible via Admin SDK).
      All users with email will be set to the default password configured in DEFAULT_PASSWORD.
    """
    print("=== Auth clone configuration ===")
    print(f"PROD project: {PROD_PROJECT_ID}")
    print(f"DEV project:  {DEV_PROJECT_ID}")
    print(f"Dry run:      {dry_run}")
    print()

    print("=== Initialize Firebase Admin apps ===")
    prod_app = init_firebase_app(PROD_SA_PATH, PROD_PROJECT_ID, "prod")
    dev_app = init_firebase_app(DEV_SA_PATH, DEV_PROJECT_ID, "dev")

    print("=== Fetch users from PROD ===")
    users = []
    page = auth.list_users(app=prod_app)
    while page:
        for u in page.users:
            users.append(u)
        page = page.get_next_page()

    print(f"Found {len(users)} users in PROD.\n")

    for i, u in enumerate(users, start=1):
        print(f"[{i}/{len(users)}] uid={u.uid} email={u.email}")

        params = {
            "uid": u.uid,
            "email": u.email,
            "email_verified": u.email_verified,
            "display_name": u.display_name,
            "phone_number": u.phone_number,
            "disabled": u.disabled,
        }
        # Set default password only for users with email (password auth requires email)
        if u.email:
            params["password"] = DEFAULT_PASSWORD
        params = {k: v for k, v in params.items() if v is not None}

        dev_user_by_uid = None
        dev_user_by_email = None

        # Check if user with this UID exists in dev
        try:
            dev_user_by_uid = auth.get_user(u.uid, app=dev_app)
        except auth.UserNotFoundError:
            dev_user_by_uid = None

        # Check if a user with this email exists in dev (only if email present)
        if u.email:
            try:
                dev_user_by_email = auth.get_user_by_email(u.email, app=dev_app)
            except auth.UserNotFoundError:
                dev_user_by_email = None

        action = None
        extra = ""

        if dev_user_by_uid:
            # UID already exists in DEV → just update to match PROD
            action = "update_uid"
        elif dev_user_by_email and dev_user_by_email.uid != u.uid:
            # Email exists in DEV but with a different UID → delete that user, then create with PROD UID
            action = "delete_conflicting_email_and_create"
            extra = f"(conflicting dev UID {dev_user_by_email.uid} for email {u.email})"
        else:
            # Neither uid nor email exists in dev → create new user with PROD UID
            action = "create_new"

        print(f"  -> planned action: {action} {extra} with params={params}")

        if dry_run:
            continue

        # Execute chosen action
        if action == "update_uid":
            # update_user() takes uid as positional arg, not in **params
            update_params = {k: v for k, v in params.items() if k != "uid"}
            auth.update_user(u.uid, app=dev_app, **update_params)

        elif action == "delete_conflicting_email_and_create":
            auth.delete_user(dev_user_by_email.uid, app=dev_app)
            auth.create_user(app=dev_app, **params)

        elif action == "create_new":
            auth.create_user(app=dev_app, **params)

        # Copy custom claims if present
        if u.custom_claims:
            auth.set_custom_user_claims(u.uid, u.custom_claims, app=dev_app)

    if dry_run:
        print("\n[DRY RUN] No users were created/updated/deleted in DEV.")
    else:
        print("\n[OK] Auth users clone finished.")
        print(f"Note: All users with email have been set to default password: {DEFAULT_PASSWORD}")
        print("      Users should change their password on first login.")



# ========== CLI ENTRYPOINT ==========

def main():
    parser = argparse.ArgumentParser(description="Clone Firebase env from PROD to DEV")
    sub = parser.add_subparsers(dest="command", required=True)

    p_fs = sub.add_parser("clone-firestore", help="Export Firestore from PROD and import into DEV")
    p_fs.add_argument("--dry-run", action="store_true", help="Print actions but do not execute")

    p_st = sub.add_parser("clone-storage", help="Copy all objects from PROD storage bucket to DEV")
    p_st.add_argument("--dry-run", action="store_true", help="Print actions but do not execute")

    p_auth = sub.add_parser("clone-auth", help="Copy Firebase Auth users from PROD to DEV")
    p_auth.add_argument("--dry-run", action="store_true", help="Print actions but do not execute")

    args = parser.parse_args()

    if args.command == "clone-firestore":
        clone_firestore(dry_run=args.dry_run)
    elif args.command == "clone-storage":
        clone_storage(dry_run=args.dry_run)
    elif args.command == "clone-auth":
        clone_auth_users(dry_run=args.dry_run)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
