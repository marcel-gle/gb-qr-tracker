#!/usr/bin/env python3
"""
Create or update a customer user (Auth + Firestore) in dev/prod.

This script does, in one go:
- create or load the Firebase Auth user by email
- set / update the password
- set custom claims (isAdmin, userId)
- create / update the Firestore customer document at customers/{uid}

Usage examples:

  # Dev user, admin, random generated password
  python setup_user.py --env dev --email owner@example.com --name "Owner GmbH" --admin

  # Prod user, explicit password, non-admin
  python setup_user.py --env prod --email user@example.com --password "StrongPass123"

You need the Firebase Admin SDK installed:
  pip install firebase-admin
"""

import argparse
import json
import secrets
import string
from typing import Optional, Tuple

import firebase_admin
from firebase_admin import auth, credentials, firestore


# --- Credentials configuration (dev / prod) ---
# Keep these in sync with other admin scripts.
DEV_CREDENTIALS_PATH = (
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/"
    "gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
)
PROD_CREDENTIALS_PATH = (
    "/Users/marcelgleich/Desktop/Software/Firebase_Service/"
    "gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"
)


def _init_admin(credential_path: str) -> firestore.Client:
    """
    Initialize Firebase Admin (Auth + Firestore) once and return a Firestore client.
    """
    if not firebase_admin._apps:
        cred = credentials.Certificate(credential_path)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def _generate_password(length: int = 20) -> str:
    """
    Generate a reasonably strong random password (letters + digits).
    """
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def ensure_auth_user(
    email: str,
    password: str,
    display_name: Optional[str],
    inactive: bool,
) -> Tuple[auth.UserRecord, bool]:
    """
    Get or create a Firebase Auth user by email and ensure the password is set.

    Returns (user, created?).
    """
    try:
        user = auth.get_user_by_email(email)
        # Update password (and optionally display name / disabled flag) on existing user.
        user = auth.update_user(
            user.uid,
            password=password,
            display_name=display_name or user.display_name,
            disabled=inactive,
        )
        return user, False
    except auth.UserNotFoundError:
        user = auth.create_user(
            email=email,
            password=password,
            display_name=display_name or None,
            disabled=inactive,
        )
        return user, True


def ensure_claims(uid: str, is_admin: Optional[bool]) -> dict:
    """
    Merge/update custom claims for a user.

    - Always ensures userId = uid.
    - If is_admin is not None, sets isAdmin to that value.
      If is_admin is None, leaves isAdmin unchanged.

    Returns the new claims dict.
    """
    u = auth.get_user(uid)
    claims = dict(u.custom_claims or {})

    # Always keep a stable userId claim.
    claims["userId"] = uid

    # Only touch isAdmin if explicitly requested.
    if is_admin is not None:
        claims["isAdmin"] = bool(is_admin)

    auth.set_custom_user_claims(uid, claims)
    return claims


def ensure_customer_doc(
    db: firestore.Client,
    uid: str,
    email: Optional[str],
    display_name: Optional[str],
    plan: str = "free",
    is_active: bool = True,
    timezone: str = "Europe/Berlin",
    locale: str = "de-DE",
) -> bool:
    """
    Ensure Firestore doc at customers/{uid} exists.

    Returns True if created, False if updated/no-op.
    """
    ref = db.collection("customers").document(uid)
    snap = ref.get()
    if not snap.exists:
        ref.set(
            {
                "owner_id": uid,
                "email": email or None,
                "display_name": display_name
                or (email.split("@")[0] if email else None),
                "plan": plan,
                "is_active": is_active,
                "settings": {"timezone": timezone, "locale": locale},
                "created_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        return True
    else:
        ref.set(
            {
                "email": email or None,
                "display_name": display_name
                or (email.split("@")[0] if email else None),
                "plan": plan,
                "is_active": is_active,
            },
            merge=True,
        )
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create or update a customer user (Auth + Firestore) in dev/prod."
    )
    parser.add_argument("--email", required=True, help="User login email")
    parser.add_argument("--password", help="Password to set (if omitted, a random one is generated)")
    parser.add_argument("--name", help="Display name (defaults to email prefix)")
    parser.add_argument(
        "--plan",
        default="free",
        choices=["free", "pro", "enterprise"],
        help="Customer plan for Firestore customers/{uid} doc",
    )
    parser.add_argument(
        "--admin",
        action="store_true",
        help="Set isAdmin=true in custom claims",
    )
    parser.add_argument(
        "--no-admin",
        action="store_true",
        help="Set isAdmin=false in custom claims",
    )
    parser.add_argument(
        "--inactive",
        action="store_true",
        help="Create/update the user as disabled (Auth.disabled=True, is_active=False in Firestore)",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Which environment's service account to use (default: dev)",
    )
    parser.add_argument(
        "--credentials",
        help="Override path to service-account JSON (otherwise derived from --env)",
    )
    parser.add_argument(
        "--timezone",
        default="Europe/Berlin",
        help="Timezone stored under customers/{uid}.settings.timezone",
    )
    parser.add_argument(
        "--locale",
        default="de-DE",
        help="Locale stored under customers/{uid}.settings.locale",
    )
    args = parser.parse_args()

    if args.admin and args.no_admin:
        parser.error("--admin and --no-admin are mutually exclusive")

    # Determine which credentials to use.
    if args.credentials:
        credential_path = args.credentials
    else:
        credential_path = (
            DEV_CREDENTIALS_PATH if args.env == "dev" else PROD_CREDENTIALS_PATH
        )

    is_active = not args.inactive

    # Determine desired isAdmin value:
    # - True if --admin
    # - False if --no-admin
    # - None => leave as-is
    if args.admin:
        is_admin: Optional[bool] = True
    elif args.no_admin:
        is_admin = False
    else:
        is_admin = None

    # Generate password if not provided.
    password = args.password or _generate_password()

    print("=" * 60)
    print("SETUP USER")
    print("=" * 60)
    print(f"Environment: {args.env}")
    print(f"Email:       {args.email}")
    print(f"Admin flag:  {is_admin if is_admin is not None else '(unchanged)'}")
    print(f"Active:      {is_active}")
    print(f"Plan:        {args.plan}")
    print(f"Credentials: {credential_path}")
    print("=" * 60)
    print()

    # Initialize Admin SDK + Firestore.
    db = _init_admin(credential_path)

    # Create or update Auth user.
    user, created = ensure_auth_user(
        email=args.email,
        password=password,
        display_name=args.name,
        inactive=not is_active,
    )

    # Set claims (userId always, isAdmin optionally).
    claims = ensure_claims(user.uid, is_admin=is_admin)

    # Ensure Firestore customer document.
    created_customer = ensure_customer_doc(
        db=db,
        uid=user.uid,
        email=user.email,
        display_name=user.display_name,
        plan=args.plan,
        is_active=is_active,
        timezone=args.timezone,
        locale=args.locale,
    )

    # Final summary (also JSON for easy copy/paste into notes).
    result = {
        "uid": user.uid,
        "email": user.email,
        "display_name": user.display_name,
        "env": args.env,
        "user_created": created,
        "customer_created": created_customer,
        "is_admin": claims.get("isAdmin"),
        "is_active": is_active,
        "plan": args.plan,
        "claims": claims,
        "password": password,
    }

    print()
    print("=" * 60)
    print("RESULT")
    print("=" * 60)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print("=" * 60)


if __name__ == "__main__":
    main()

