# main.py
# Run as CLI:
#   GOOGLE_APPLICATION_CREDENTIALS=./service-account.json \
#   python main.py --email owner@acme.com --name "Acme GmbH" --plan pro --active 1 --admin 0
#
# Deploy as Cloud Function (HTTP):
#   gcloud functions deploy create_customer_http \
#     --runtime python311 --region europe-west3 \
#     --project YOUR_PROJECT_ID \
#     --entry-point create_customer_http \
#     --trigger-http --allow-unauthenticated
#
# Then call with:
#   curl -H "Authorization: Bearer <FIREBASE_ID_TOKEN>" \
#        -H "Content-Type: application/json" \
#        -d '{"email":"owner@acme.com","display_name":"Acme GmbH","plan":"pro","is_active":true,"set_admin":false}' \
#        https://<cloud-function-url>

from __future__ import annotations
import argparse
import json
import os
import sys
from typing import Optional, Tuple

import firebase_admin
from firebase_admin import credentials, auth, firestore
from google.api_core.exceptions import NotFound
from flask import Request, jsonify  # used only for the HTTP function

# ---------- Environment variables ----------
PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT")
DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")

# ---------- Admin SDK init ----------
def _init_admin(project_id: Optional[str] = None, database_id: Optional[str] = None):
    """
    Initialize Firebase Admin once and return a Firestore client
    bound to the given project & database.

    - project_id: your GCP project (e.g. "acme-prod")
    - database_id: Firestore database id (use "(default)" for the default)
    """
    # Pick sensible defaults
    print("project_id function", project_id)
    print("database_id", database_id)

    if not firebase_admin._apps:
        # Locally: use service account if provided; otherwise ADC.
        cred = (
            credentials.Certificate(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
            if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            else credentials.ApplicationDefault()
        )
        # Pass projectId here so Auth & other services also bind to the right project.
        firebase_admin.initialize_app(cred, {"projectId": project_id} if project_id else None)

    # Bind Firestore explicitly to project + database (multi-DB aware).
    return firestore.Client(project=project_id, database=database_id)



# ---------- Core ops (idempotent) ----------
def ensure_user(email: str, display_name: Optional[str] = None) -> Tuple[auth.UserRecord, bool]:
    """Get or create a Firebase Auth user by email. Returns (user, created?)."""
    try:
        user = auth.get_user_by_email(email)
        return user, False
    except auth.UserNotFoundError:
        user = auth.create_user(email=email, display_name=display_name or None, disabled=False)
        return user, True


def ensure_claims(uid: str, set_admin: Optional[bool] = False, extra: Optional[dict] = None) -> dict:
    """
    Merge/Update custom claims. Only changes provided keys.
    Returns the new claims dict.
    """
    u = auth.get_user(uid)
    claims = dict(u.custom_claims or {})
    if set_admin is not None:
        claims["isAdmin"] = bool(set_admin)
    if extra:
        claims.update(extra)
    claims.update({"isAdmin": bool(set_admin), "userId": uid})
    print("Setting claims for", uid, claims)
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
    Ensure Firestore doc at customers/{uid} exists. Returns True if created, False if updated/no-op.
    """
    ref = db.collection("customers").document(uid)
    snap = ref.get()
    if not snap.exists:
        ref.set(
            {
                "owner_id": uid,                       # echo for rules/queries
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
        # Minimal safe update. Keep it idempotent.
        ref.set(
            {
                "email": email or None,
                "display_name": display_name
                or (email.split("@")[0] if email else None),
            },
            merge=True,
        )
        return False

# ---------- HTTP Cloud Function ----------
def _require_admin_from_bearer(request: Request) -> dict:
    """
    Verify Firebase ID token from Authorization: Bearer <token>.
    Require isAdmin == true in custom claims.
    Returns decoded token.
    """
    authz = request.headers.get("Authorization", "")
    if not authz.startswith("Bearer "):
        return None
    id_token = authz.split(" ", 1)[1].strip()
    try:
        decoded = auth.verify_id_token(id_token)
    except Exception:
        return None
    if not decoded.get("isAdmin", False):
        return None
    return decoded


def create_customer_http(request: Request):
    """
    HTTP endpoint (admin-only) to create/ensure a customer.
    Body JSON: { email, display_name?, plan? = "free", is_active? = true, set_admin? = false }
    """
    db = _init_admin(project_id=PROJECT_ID, database_id=DATABASE_ID)

    decoded = _require_admin_from_bearer(request)
    if not decoded:
        return jsonify({"error": "Unauthorized (admin token required)"}), 401

    try:
        body = request.get_json(silent=True) or {}
        email = body.get("email")
        display_name = body.get("display_name")
        plan = body.get("plan", "free")
        is_active = bool(body.get("is_active", True))
        set_admin = body.get("set_admin", None)
        if email is None:
            return jsonify({"error": "email is required"}), 400

        user, created_user = ensure_user(email, display_name)
        # Only set isAdmin if provided; otherwise leave claims as-is
        claims = ensure_claims(user.uid, bool(set_admin) if set_admin is not None else None)

        created_customer = ensure_customer_doc(
            db,
            uid=user.uid,
            email=user.email,
            display_name=user.display_name,
            plan=plan,
            is_active=is_active,
        )

        return jsonify(
            {
                "ok": True,
                "uid": user.uid,
                "user_created": created_user,
                "customer_created": created_customer,
                "claims": claims,
            }
        ), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

# ---------- CLI ----------
def main_cli():
    p = argparse.ArgumentParser(description="Create/ensure a customer by email")
    p.add_argument("--email", required=True, help="Customer's login email")
    p.add_argument("--name", help="Display name (defaults to email prefix)")
    p.add_argument("--plan", default="free", choices=["free", "pro", "enterprise"])
    p.add_argument("--active", type=int, default=1, help="1 or 0 (default 1)")
    p.add_argument("--admin", type=int, default=None, help="set isAdmin claim (1/0); omit to leave unchanged") #Buggy
    p.add_argument("--locale", default="de-DE")
    p.add_argument("--tz", default="Europe/Berlin")
    p.add_argument("--project", help="GCP project ID (overrides env)")
    p.add_argument("--database", default="(default)", help="Firestore database ID")
    args = p.parse_args()

    db = _init_admin(project_id=PROJECT_ID, database_id=DATABASE_ID)

    user, created_user = ensure_user(args.email, args.name)
    claims = ensure_claims(user.uid, (args.admin == 1) if args.admin is not None else None)
    created_customer = ensure_customer_doc(
        db,
        uid=user.uid,
        email=user.email,
        display_name=user.display_name,
        plan=args.plan,
        is_active=bool(args.active),
        timezone=args.tz,
        locale=args.locale,
    )

    print(
        json.dumps(
            {
                "uid": user.uid,
                "user_created": created_user,
                "customer_created": created_customer,
                "claims": claims,
            },
            indent=2,
            ensure_ascii=False,
        )
    )


# ---------- Entrypoint ----------
if __name__ == "__main__":
    # CLI mode
    # Override with defaults if not set via environment
    if not PROJECT_ID:
        PROJECT_ID = "gb-qr-tracker"
    if not DATABASE_ID:
        DATABASE_ID = "(default)"
    _init_admin(project_id=PROJECT_ID, database_id=DATABASE_ID)
    main_cli()
