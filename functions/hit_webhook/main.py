"""
Hit webhook integration (Gen 2 Cloud Functions).

Two entry points share this module:

  deliver_hit     <- Firestore hits/{document} created  (hit_webhook_delivery)
  manage_config   <- HTTP CRUD / test / rotate          (hit_webhook_config)

Deploy: ./deploy.sh <dev|prod> hit_webhook delivery|config
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

import firebase_admin
import functions_framework
import requests
from cloudevents.http import CloudEvent
from firebase_admin import auth as fb_auth
from flask import Request, Response, jsonify
from google.cloud import firestore
from google.events.cloud import firestore as firestoredata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hit_webhook")

PROJECT_ID = (
    os.environ.get("PROJECT_ID")
    or os.environ.get("FIREBASE_PROJECT_ID")
    or os.environ.get("GCP_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or ""
)
DATABASE_ID = os.environ.get("DATABASE_ID", "(default)")
WEBHOOK_TIMEOUT_S = float(os.environ.get("WEBHOOK_TIMEOUT_S", "8"))
USER_AGENT = "GB-QR-Tracker-Webhook/1.0"
SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP

_db: Optional[firestore.Client] = None


def _ensure_firebase_app() -> None:
    if not firebase_admin._apps:
        firebase_admin.initialize_app()


def _db_client() -> firestore.Client:
    global _db
    if _db is None:
        _ensure_firebase_app()
        _db = firestore.Client(project=PROJECT_ID or None, database=DATABASE_ID)
    return _db


# ---------------------------------------------------------------------------
# Shared: URL / secret / signing / payload / delivery
# ---------------------------------------------------------------------------

def is_https_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return parsed.scheme == "https" and bool(parsed.netloc)


def generate_secret() -> str:
    return secrets.token_hex(32)


def mask_secret(secret: Optional[str]) -> Optional[str]:
    if not secret:
        return None
    if len(secret) <= 8:
        return "••••"
    return f"••••{secret[-4:]}"


def sign_body(secret: str, body: bytes, ts: int) -> str:
    """HMAC-SHA256 over '{ts}.{body}' so timestamp is bound to the signature."""
    message = f"{ts}.".encode("utf-8") + body
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _ref_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, firestore.DocumentReference):
        return value.id
    if hasattr(value, "id") and not isinstance(value, dict):
        try:
            return value.id
        except Exception:
            pass
    if isinstance(value, str):
        parts = value.strip("/").split("/")
        return parts[-1] if parts else None
    return None


def _ts_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (int, float)):
        # protobuf helpers may emit epoch ms
        seconds = value / 1000.0 if value > 1e12 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    if hasattr(value, "timestamp") and callable(value.timestamp):
        try:
            return datetime.fromtimestamp(value.timestamp(), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return None
    return None


def lookup_business_name(owner_id: str, business_id: Optional[str], business_ref_path: Any = None) -> Optional[str]:
    if not owner_id or not business_id:
        return None
    try:
        # Prefer customer overlay contact/name, then canonical business_name
        overlay = (
            _db_client()
            .collection("customers")
            .document(owner_id)
            .collection("businesses")
            .document(business_id)
            .get()
        )
        if overlay.exists:
            data = overlay.to_dict() or {}
            for key in ("business_name", "name", "company_name", "firma"):
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    # Overlay "name" is often contact person; prefer business_name keys first
                    if key != "name":
                        return val.strip()
            contact_name = data.get("name")
            # Resolve via overlay.business_ref or hit business_ref / businesses/{id}
            ref = data.get("business_ref")
            if isinstance(ref, firestore.DocumentReference):
                snap = ref.get()
                if snap.exists:
                    bn = (snap.to_dict() or {}).get("business_name")
                    if isinstance(bn, str) and bn.strip():
                        return bn.strip()
            if isinstance(contact_name, str) and contact_name.strip():
                return contact_name.strip()

        # Canonical businesses/{id}
        canon = _db_client().collection("businesses").document(business_id).get()
        if canon.exists:
            bn = (canon.to_dict() or {}).get("business_name")
            if isinstance(bn, str) and bn.strip():
                return bn.strip()

        # Explicit reference path from hit
        if isinstance(business_ref_path, str) and business_ref_path:
            parts = business_ref_path.strip("/").split("/")
            if "documents" in parts:
                parts = parts[parts.index("documents") + 1 :]
            if len(parts) >= 2:
                snap = _db_client().document("/".join(parts)).get()
                if snap.exists:
                    bn = (snap.to_dict() or {}).get("business_name")
                    if isinstance(bn, str) and bn.strip():
                        return bn.strip()
    except Exception as exc:
        logger.warning("business name lookup failed: %s", exc)
    return None


def build_hit_payload(hit_id: str, hit: dict, business_name: Optional[str] = None) -> dict:
    owner_id = hit.get("owner_id") or ""
    business_id = _ref_id(hit.get("business_ref"))
    campaign_id = _ref_id(hit.get("campaign_ref"))
    if business_name is None:
        business_name = lookup_business_name(
            str(owner_id), business_id, hit.get("business_ref")
        )

    payload = {
        "event": "hit.created",
        "hit_id": hit_id,
        "ts": _ts_iso(hit.get("ts")) or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "owner_id": owner_id,
        "link_id": hit.get("link_id"),
        "campaign_id": campaign_id,
        "campaign_name": hit.get("campaign_name"),
        "business_id": business_id,
        "business_name": business_name,
        "device_type": hit.get("device_type"),
        "ua_browser": hit.get("ua_browser"),
        "ua_os": hit.get("ua_os"),
        "geo_city": hit.get("geo_city"),
        "geo_country": hit.get("geo_country"),
        "hit_origin": hit.get("hit_origin"),
    }
    return {k: v for k, v in payload.items() if v is not None}


def build_test_payload(owner_id: str) -> dict:
    return {
        "event": "hit.created",
        "hit_id": "test-hit",
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "owner_id": owner_id,
        "link_id": "test-link",
        "campaign_id": "test-campaign",
        "campaign_name": "Webhook Test",
        "business_id": "test-business",
        "business_name": "Webhook Test Business",
        "device_type": "mobile",
        "ua_browser": "Test",
        "ua_os": "Test",
        "geo_city": "Berlin",
        "geo_country": "DE",
        "hit_origin": "webhook_test",
    }


def get_webhook_config(customer_data: dict) -> dict:
    integrations = customer_data.get("integrations") or {}
    cfg = integrations.get("hits_webhook") or {}
    return cfg if isinstance(cfg, dict) else {}


def deliver_payload(url: str, secret: str, payload: dict) -> tuple[bool, str, bool]:
    """Return (ok, detail, retryable). Permanent client errors are not retryable."""
    if not is_https_url(url):
        return False, "Webhook URL must be https", False
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ts = int(time.time())
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "X-GB-Signature": sign_body(secret, body, ts),
        "X-GB-Timestamp": str(ts),
    }
    try:
        resp = requests.post(url, data=body, headers=headers, timeout=WEBHOOK_TIMEOUT_S)
        if 200 <= resp.status_code < 300:
            return True, f"HTTP {resp.status_code}", False
        detail = f"HTTP {resp.status_code}: {(resp.text or '')[:200]}"
        # 408/429/5xx → retry; other 4xx are permanent (bad URL / rejected)
        retryable = resp.status_code in (408, 429) or resp.status_code >= 500
        return False, detail, retryable
    except requests.RequestException as exc:
        return False, str(exc)[:300], True


def update_delivery_status(owner_id: str, ok: bool, detail: str) -> None:
    ref = _db_client().collection("customers").document(owner_id)
    if ok:
        ref.set(
            {
                "integrations": {
                    "hits_webhook": {
                        "last_success_at": SERVER_TIMESTAMP,
                        "last_error": None,
                    }
                }
            },
            merge=True,
        )
    else:
        ref.set(
            {
                "integrations": {
                    "hits_webhook": {
                        "last_error_at": SERVER_TIMESTAMP,
                        "last_error": detail[:500],
                    }
                }
            },
            merge=True,
        )


def should_skip_hit(hit: dict) -> Optional[str]:
    if hit.get("suspected_bot"):
        return "suspected_bot"
    link_id = str(hit.get("link_id") or "")
    if link_id.startswith("monitor-test"):
        return "monitor_test"
    if hit.get("is_demo"):
        return "demo"
    return None


# ---------------------------------------------------------------------------
# Firestore event parsing
# ---------------------------------------------------------------------------

_OMIT = object()


def parse_event(cloud_event: CloudEvent) -> firestoredata.DocumentEventData:
    payload = firestoredata.DocumentEventData()
    payload._pb.ParseFromString(cloud_event.data)
    return payload


def doc_path_parts(name: str) -> list[str]:
    parts = name.split("/")
    try:
        idx = parts.index("documents")
    except ValueError:
        return []
    return parts[idx + 1 :]


def _value_to_python(value: firestoredata.Value) -> Any:
    kind = value._pb.WhichOneof("value_type")
    if kind is None or kind == "null_value":
        return _OMIT
    if kind == "string_value":
        return value.string_value
    if kind == "boolean_value":
        return value.boolean_value
    if kind == "integer_value":
        return int(value.integer_value)
    if kind == "double_value":
        return value.double_value
    if kind == "timestamp_value":
        return value.timestamp_value
    if kind == "reference_value":
        return value.reference_value
    if kind == "bytes_value":
        return _OMIT
    if kind == "geo_point_value":
        gp = value.geo_point_value
        return {"lat": gp.latitude, "lon": gp.longitude}
    if kind == "array_value":
        out = []
        for item in value.array_value.values:
            converted = _value_to_python(item)
            if converted is not _OMIT:
                out.append(converted)
        return out
    if kind == "map_value":
        return _fields_to_dict(value.map_value.fields)
    return _OMIT


def _fields_to_dict(fields) -> dict:
    out: dict[str, Any] = {}
    for key, value in fields.items():
        converted = _value_to_python(value)
        if converted is not _OMIT:
            out[key] = converted
    return out


def document_to_dict(document: firestoredata.Document) -> dict:
    return _fields_to_dict(document.fields)


def extract_hit_from_event(cloud_event: CloudEvent) -> tuple[Optional[str], Optional[dict]]:
    """Return (hit_id, hit_dict) from a Firestore created event."""
    # Prefer protobuf decode (same as typesense_sync)
    try:
        if isinstance(cloud_event.data, (bytes, bytearray)):
            payload = parse_event(cloud_event)
            parts = doc_path_parts(payload.value.name)
            if len(parts) >= 2 and parts[0] == "hits":
                return parts[1], document_to_dict(payload.value)
    except Exception as exc:
        logger.warning("protobuf parse failed, falling back: %s", exc)

    event_subject = getattr(cloud_event, "subject", None) or ""
    hit_id = None
    if event_subject and "/" in event_subject:
        parts = event_subject.split("/")
        if len(parts) >= 3 and parts[0] == "documents" and parts[1] == "hits":
            hit_id = parts[2]

    if hit_id:
        snap = _db_client().collection("hits").document(hit_id).get()
        if snap.exists:
            return hit_id, snap.to_dict() or {}
    return hit_id, None


# ---------------------------------------------------------------------------
# Entry point 1: deliver_hit (Firestore)
# ---------------------------------------------------------------------------

@functions_framework.cloud_event
def deliver_hit(cloud_event: CloudEvent) -> None:
    hit_id, hit = extract_hit_from_event(cloud_event)
    if not hit_id or not hit:
        logger.error("Could not extract hit from event (hit_id=%s)", hit_id)
        return

    skip = should_skip_hit(hit)
    if skip:
        logger.info("Skipping hit %s (%s)", hit_id, skip)
        return

    owner_id = hit.get("owner_id")
    if not owner_id or not isinstance(owner_id, str):
        logger.warning("Hit %s missing owner_id", hit_id)
        return

    customer_snap = _db_client().collection("customers").document(owner_id).get()
    if not customer_snap.exists:
        logger.info("No customer doc for owner_id=%s", owner_id)
        return

    cfg = get_webhook_config(customer_snap.to_dict() or {})
    if not cfg.get("enabled"):
        logger.debug("Webhook disabled for %s", owner_id)
        return

    url = (cfg.get("url") or "").strip()
    secret = (cfg.get("secret") or "").strip()
    if not url or not secret:
        logger.debug("Webhook incomplete for %s (url/secret)", owner_id)
        return

    payload = build_hit_payload(hit_id, hit)
    ok, detail, retryable = deliver_payload(url, secret, payload)
    try:
        update_delivery_status(owner_id, ok, detail)
    except Exception as exc:
        logger.warning("Failed to update delivery status: %s", exc)

    if ok:
        logger.info("Delivered hit %s to owner %s (%s)", hit_id, owner_id, detail)
    else:
        logger.warning("Delivery failed hit %s owner %s: %s", hit_id, owner_id, detail)
        if retryable:
            raise RuntimeError(f"Webhook delivery failed: {detail}")


# ---------------------------------------------------------------------------
# Entry point 2: manage_config (HTTP)
# ---------------------------------------------------------------------------

def _cors_headers(request: Request) -> dict:
    origin = request.headers.get("Origin") or "*"
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": "GET, PUT, POST, OPTIONS",
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


def _verify_uid(id_token: str, project_id: str) -> str:
    _ensure_firebase_app()
    decoded = fb_auth.verify_id_token(id_token, check_revoked=True)
    uid = decoded.get("uid") or decoded.get("sub")
    if not uid:
        raise ValueError("Token missing uid/sub claim")
    token_aud = decoded.get("aud")
    if project_id and token_aud and token_aud != project_id:
        raise fb_auth.InvalidIdTokenError(
            f"Token audience {token_aud!r} does not match project {project_id!r}"
        )
    return uid


def _public_config(cfg: dict, include_secret: Optional[str] = None) -> dict:
    out = {
        "enabled": bool(cfg.get("enabled")),
        "url": cfg.get("url"),
        "secret_masked": mask_secret(cfg.get("secret")),
        "created_at": _ts_iso(cfg.get("created_at")),
        "updated_at": _ts_iso(cfg.get("updated_at")),
        "last_success_at": _ts_iso(cfg.get("last_success_at")),
        "last_error_at": _ts_iso(cfg.get("last_error_at")),
        "last_error": cfg.get("last_error"),
    }
    if include_secret:
        out["secret"] = include_secret
    return out


def _load_customer_cfg(uid: str) -> tuple[firestore.DocumentReference, dict, dict]:
    ref = _db_client().collection("customers").document(uid)
    snap = ref.get()
    data = snap.to_dict() if snap.exists else {}
    return ref, data or {}, get_webhook_config(data or {})


@functions_framework.http
def manage_config(request: Request):
    if request.method == "OPTIONS":
        resp = Response(status=204)
        for key, value in _cors_headers(request).items():
            resp.headers[key] = value
        return resp

    token = _bearer_token(request)
    if not token:
        return _json({"error": "Missing bearer token"}, 401, request)

    project_id = PROJECT_ID
    if not project_id:
        return _json({"error": "PROJECT_ID is not configured"}, 500, request)

    try:
        uid = _verify_uid(token, project_id)
    except (
        fb_auth.InvalidIdTokenError,
        fb_auth.ExpiredIdTokenError,
        fb_auth.RevokedIdTokenError,
        fb_auth.CertificateFetchError,
        ValueError,
    ) as err:
        logger.warning("auth failed: %s", err)
        return _json({"error": "Invalid or expired Firebase ID token"}, 401, request)

    try:
        if request.method == "GET":
            _, _, cfg = _load_customer_cfg(uid)
            return _json({"webhook": _public_config(cfg)}, 200, request)

        if request.method == "PUT":
            body = request.get_json(silent=True) or {}
            url = body.get("url")
            enabled = body.get("enabled")

            ref, _, cfg = _load_customer_cfg(uid)
            updates: dict[str, Any] = {"updated_at": SERVER_TIMESTAMP}

            if url is not None:
                url = str(url).strip()
                if url and not is_https_url(url):
                    return _json({"error": "url must be an https URL"}, 400, request)
                updates["url"] = url or None

            if enabled is not None:
                updates["enabled"] = bool(enabled)

            new_secret = None
            if not cfg.get("secret"):
                new_secret = generate_secret()
                updates["secret"] = new_secret
                if not cfg.get("created_at"):
                    updates["created_at"] = SERVER_TIMESTAMP

            ref.set({"integrations": {"hits_webhook": updates}}, merge=True)
            _, _, cfg_after = _load_customer_cfg(uid)
            return _json(
                {"webhook": _public_config(cfg_after, include_secret=new_secret)},
                200,
                request,
            )

        if request.method == "POST":
            body = request.get_json(silent=True) or {}
            action = (body.get("action") or "").strip().lower()

            if action == "rotate_secret":
                ref, _, cfg = _load_customer_cfg(uid)
                new_secret = generate_secret()
                ref.set(
                    {
                        "integrations": {
                            "hits_webhook": {
                                "secret": new_secret,
                                "updated_at": SERVER_TIMESTAMP,
                                "created_at": cfg.get("created_at") or SERVER_TIMESTAMP,
                            }
                        }
                    },
                    merge=True,
                )
                _, _, cfg_after = _load_customer_cfg(uid)
                return _json(
                    {"webhook": _public_config(cfg_after, include_secret=new_secret)},
                    200,
                    request,
                )

            if action == "test":
                _, _, cfg = _load_customer_cfg(uid)
                url = (cfg.get("url") or "").strip()
                secret = (cfg.get("secret") or "").strip()
                if not url or not secret:
                    return _json(
                        {"error": "Configure url and secret before testing"},
                        400,
                        request,
                    )
                if not cfg.get("enabled"):
                    return _json(
                        {"error": "Enable the webhook before testing"},
                        400,
                        request,
                    )
                payload = build_test_payload(uid)
                ok, detail, _retryable = deliver_payload(url, secret, payload)
                try:
                    update_delivery_status(uid, ok, detail)
                except Exception as exc:
                    logger.warning("Failed to update delivery status: %s", exc)
                status = 200 if ok else 502
                return _json(
                    {"ok": ok, "detail": detail, "payload": payload},
                    status,
                    request,
                )

            return _json(
                {"error": "Unknown action; use rotate_secret or test"},
                400,
                request,
            )

        return _json({"error": "Method not allowed"}, 405, request)
    except Exception as err:
        logger.exception("manage_config error: %s", err)
        return _json({"error": "Internal error"}, 500, request)
