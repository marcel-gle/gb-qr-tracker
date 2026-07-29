"""
Mint Typesense scoped search keys for the authenticated Firebase user.

POST /api/search-key (via Hosting rewrite) or direct Cloud Function URL.
Requires Authorization: Bearer <Firebase ID token>.

Env (server-side only):
  FIREBASE_PROJECT_ID / PROJECT_ID  — Firebase project for token audience
  TYPESENSE_HOST                    — cluster host (no https://, no port)
  TYPESENSE_SEARCH_ONLY_KEY         — documents:search key (Secret Manager)
"""

from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

import firebase_admin
import functions_framework
import typesense
from firebase_admin import auth as fb_auth
from flask import Request, Response, jsonify

logger = logging.getLogger("search_key")

_ts_client: Optional[typesense.Client] = None


def _ensure_firebase_app() -> None:
    if not firebase_admin._apps:
        firebase_admin.initialize_app()


def _cors_headers(request: Request) -> dict:
    origin = request.headers.get("Origin") or "*"
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
    }


def _json(body: dict, status: int, request: Request):
    resp = jsonify(body)
    resp.status_code = status
    for key, value in _cors_headers(request).items():
        resp.headers[key] = value
    return resp


def _bearer_token(request: Request) -> Optional[str]:
    authz = request.headers.get("Authorization") or ""
    if not authz.lower().startswith("bearer "):
        return None
    token = authz.split(" ", 1)[1].strip()
    return token or None


def _project_id() -> str:
    return (
        os.environ.get("FIREBASE_PROJECT_ID")
        or os.environ.get("PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCP_PROJECT")
        or ""
    )


def _typesense_config() -> Tuple[str, str]:
    host = (os.environ.get("TYPESENSE_HOST") or "").strip()
    key = (os.environ.get("TYPESENSE_SEARCH_ONLY_KEY") or "").strip()
    return host, key


def _get_typesense_client(host: str, api_key: str) -> typesense.Client:
    global _ts_client
    if _ts_client is None:
        _ts_client = typesense.Client(
            {
                "nodes": [
                    {
                        "host": host,
                        "port": "443",
                        "protocol": "https",
                    }
                ],
                "api_key": api_key,
                "connection_timeout_seconds": 5,
            }
        )
    return _ts_client


def _as_str(value) -> str:
    """Python Typesense returns scoped keys as bytes; Flask needs str for JSON."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _verify_uid(id_token: str, project_id: str) -> str:
    """Verify Firebase ID token and return uid (sub)."""
    _ensure_firebase_app()
    decoded = fb_auth.verify_id_token(id_token, check_revoked=True)
    uid = decoded.get("uid") or decoded.get("sub")
    if not uid:
        raise ValueError("Token missing uid/sub claim")
    # Ensure token is for the expected project when we know it
    token_aud = decoded.get("aud")
    if project_id and token_aud and token_aud != project_id:
        raise fb_auth.InvalidIdTokenError(
            f"Token audience {token_aud!r} does not match project {project_id!r}"
        )
    return uid


@functions_framework.http
def search_key(request: Request):
    if request.method == "OPTIONS":
        resp = Response(status=204)
        for key, value in _cors_headers(request).items():
            resp.headers[key] = value
        return resp

    if request.method != "POST":
        return _json({"error": "Method not allowed"}, 405, request)

    token = _bearer_token(request)
    if not token:
        return _json({"error": "Missing bearer token"}, 401, request)

    project_id = _project_id()
    if not project_id:
        return _json({"error": "FIREBASE_PROJECT_ID is not configured"}, 500, request)

    host, search_only_key = _typesense_config()
    if not host or not search_only_key:
        return _json({"error": "Typesense env vars are not configured"}, 500, request)

    try:
        uid = _verify_uid(token, project_id)
        client = _get_typesense_client(host, search_only_key)
        businesses_key = _as_str(
            client.keys.generate_scoped_search_key(
                search_only_key, {"filter_by": f"ownerIds:={uid}"}
            )
        )
        customer_businesses_key = _as_str(
            client.keys.generate_scoped_search_key(
                search_only_key, {"filter_by": f"owner_id:={uid}"}
            )
        )
        return _json(
            {
                "host": host,
                "businessesKey": businesses_key,
                "customerBusinessesKey": customer_businesses_key,
            },
            200,
            request,
        )
    except (
        fb_auth.InvalidIdTokenError,
        fb_auth.ExpiredIdTokenError,
        fb_auth.RevokedIdTokenError,
        fb_auth.CertificateFetchError,
        ValueError,
    ) as err:
        logger.warning("[search-key] auth failed: %s", err)
        return _json({"error": "Invalid or expired Firebase ID token"}, 401, request)
    except Exception as err:
        logger.exception("[search-key] error: %s", err)
        return _json({"error": "Failed to generate search keys"}, 500, request)
