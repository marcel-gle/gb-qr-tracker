# main.py
# Firestore -> Typesense live sync (Gen 2 Cloud Functions).
#
# Two entry points share this module, each deployed as its own function with a
# single Firestore "written" trigger (Eventarc allows one path-pattern per fn):
#
#   sync_businesses              <- businesses/{businessId}
#   sync_customer_businesses     <- customers/{customerId}/businesses/{docId}
#
# Each event is parsed from the protobuf payload so create/update/delete all work
# from the event itself (deletes have no doc to re-read). The matching Typesense
# document is upserted (create/update) or deleted, keyed by a deterministic id so
# retried events stay idempotent.
#
# Deploy: see config.businesses.<env>.sh / config.customer_businesses.<env>.sh
# and ../../deploy.sh.

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import functions_framework
import typesense
from cloudevents.http import CloudEvent
from google.events.cloud import firestore as firestoredata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("typesense_sync")

# ---------- Configuration (no hardcoding) ----------
TYPESENSE_HOST = os.environ.get("TYPESENSE_HOST", "")
TYPESENSE_PORT = os.environ.get("TYPESENSE_PORT", "443")
TYPESENSE_PROTOCOL = os.environ.get("TYPESENSE_PROTOCOL", "https")
TYPESENSE_API_KEY = os.environ.get("TYPESENSE_API_KEY", "")

# Changing these two names (env only) is how we cut over from dev to prod.
BUSINESSES_COLLECTION = os.environ.get("BUSINESSES_COLLECTION", "businesses_test")
CUSTOMER_BUSINESSES_COLLECTION = os.environ.get(
    "CUSTOMER_BUSINESSES_COLLECTION", "customer_businesses_test"
)

_ts_client: Optional[typesense.Client] = None


def get_client() -> typesense.Client:
    """Lazily build a write-capable Typesense client (reused across invocations)."""
    global _ts_client
    if _ts_client is None:
        if not TYPESENSE_HOST or not TYPESENSE_API_KEY:
            raise RuntimeError(
                "Missing Typesense config: set TYPESENSE_HOST and TYPESENSE_API_KEY"
            )
        _ts_client = typesense.Client(
            {
                "nodes": [
                    {
                        "host": TYPESENSE_HOST,
                        "port": TYPESENSE_PORT,
                        "protocol": TYPESENSE_PROTOCOL,
                    }
                ],
                "api_key": TYPESENSE_API_KEY,
                "connection_timeout_seconds": 10,
            }
        )
    return _ts_client


# ---------- Protobuf event helpers ----------
def parse_event(cloud_event: CloudEvent) -> firestoredata.DocumentEventData:
    """Decode the Firestore Gen 2 protobuf payload into DocumentEventData."""
    payload = firestoredata.DocumentEventData()
    payload._pb.ParseFromString(cloud_event.data)
    return payload


def doc_path_parts(name: str) -> list[str]:
    """Return the document path segments after `.../documents/`.

    `name` looks like:
      projects/<p>/databases/<db>/documents/<collection>/<id>[/<sub>/<id> ...]
    """
    parts = name.split("/")
    try:
        idx = parts.index("documents")
    except ValueError:
        return []
    return parts[idx + 1 :]


# Sentinel returned for null/unset values so callers can drop the field entirely
# rather than writing a typed null into Typesense.
_OMIT = object()


def _value_to_python(value: firestoredata.Value) -> Any:
    """Convert a single Firestore protobuf Value to a native Python value.

    Returns the sentinel `_OMIT` for null/unset so callers can drop the field
    rather than writing a typed null into Typesense.
    """
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
        # proto-plus surfaces this as a DatetimeWithNanoseconds (datetime subclass).
        return int(value.timestamp_value.timestamp() * 1000)
    if kind == "reference_value":
        # Full document path -> store only the referenced doc's id.
        return value.reference_value.split("/")[-1]
    if kind == "bytes_value":
        return _OMIT  # Typesense can't index raw bytes; drop it.
    if kind == "geo_point_value":
        gp = value.geo_point_value
        return [gp.latitude, gp.longitude]
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
    """Convert a protobuf map<string, Value> to a plain dict, omitting nulls."""
    out: dict[str, Any] = {}
    for key, value in fields.items():
        converted = _value_to_python(value)
        if converted is not _OMIT:
            out[key] = converted
    return out


def document_to_dict(document: firestoredata.Document) -> dict:
    """Convert a Firestore protobuf Document's fields to a clean Python dict.

    DocumentReferences become `<ref>.id` strings and Timestamps become epoch ms,
    so the result is directly writable to Typesense.
    """
    return _fields_to_dict(document.fields)


# ---------- Typesense write helpers ----------
def upsert_document(collection: str, doc: dict) -> None:
    """Idempotent upsert by the document's deterministic `id`."""
    try:
        get_client().collections[collection].documents.upsert(doc)
        logger.info("upsert ok: collection=%s id=%s", collection, doc.get("id"))
    except Exception as exc:  # re-raise so the platform retries
        logger.error(
            "upsert FAILED: collection=%s id=%s error=%s",
            collection,
            doc.get("id"),
            getattr(exc, "args", exc),
        )
        raise


def delete_document(collection: str, doc_id: str) -> None:
    """Delete by id; treat 'not found' as success (already gone)."""
    try:
        get_client().collections[collection].documents[doc_id].delete()
        logger.info("delete ok: collection=%s id=%s", collection, doc_id)
    except typesense.exceptions.ObjectNotFound:
        logger.info(
            "delete no-op (already absent): collection=%s id=%s", collection, doc_id
        )
    except Exception as exc:  # re-raise so the platform retries
        logger.error(
            "delete FAILED: collection=%s id=%s error=%s",
            collection,
            doc_id,
            getattr(exc, "args", exc),
        )
        raise


def _is_delete(payload: firestoredata.DocumentEventData) -> bool:
    """A write with no after-state (empty value.name) is a delete."""
    return not bool(payload.value.name)


# ---------- Entry point 1: businesses ----------
@functions_framework.cloud_event
def sync_businesses(cloud_event: CloudEvent) -> None:
    """Sync businesses/{businessId} -> Typesense BUSINESSES_COLLECTION.

    Triggered by: google.cloud.firestore.document.v1.written
    """
    payload = parse_event(cloud_event)

    if _is_delete(payload):
        parts = doc_path_parts(payload.old_value.name)
        doc_id = parts[-1] if parts else None
        if not doc_id:
            logger.error("businesses delete: could not resolve doc id from event")
            return
        logger.info("businesses delete: id=%s", doc_id)
        delete_document(BUSINESSES_COLLECTION, doc_id)
        return

    parts = doc_path_parts(payload.value.name)
    doc_id = parts[-1] if parts else None
    if not doc_id:
        logger.error("businesses upsert: could not resolve doc id from event")
        return

    doc = document_to_dict(payload.value)
    # Typesense id is ALWAYS the Firestore doc id (matches the backfill, and keeps
    # upsert/delete ids identical). The business_id field still rides through.
    doc["id"] = doc_id

    doc["business_name"] = str(doc.get("business_name") or "")

    owner_ids = doc.get("ownerIds")
    if not isinstance(owner_ids, list) or len(owner_ids) == 0:
        logger.error(
            "businesses upsert SKIPPED: id=%s missing access field ownerIds (got %r)",
            doc_id,
            owner_ids,
        )
        return

    logger.info("businesses upsert: id=%s ownerIds=%d", doc_id, len(owner_ids))
    upsert_document(BUSINESSES_COLLECTION, doc)


# ---------- Entry point 2: customer_businesses ----------
@functions_framework.cloud_event
def sync_customer_businesses(cloud_event: CloudEvent) -> None:
    """Sync customers/{customerId}/businesses/{docId} -> CUSTOMER_BUSINESSES_COLLECTION.

    Triggered by: google.cloud.firestore.document.v1.written
    """
    payload = parse_event(cloud_event)

    source_name = payload.old_value.name if _is_delete(payload) else payload.value.name
    parts = doc_path_parts(source_name)
    # Expect: customers/<customerId>/businesses/<docId>
    if len(parts) < 4 or parts[0] != "customers" or parts[2] != "businesses":
        logger.error(
            "customer_businesses: unexpected path %r (parts=%r)", source_name, parts
        )
        return

    customer_id = parts[1]
    sub_doc_id = parts[3]
    ts_id = f"{customer_id}__{sub_doc_id}"

    if _is_delete(payload):
        logger.info("customer_businesses delete: id=%s", ts_id)
        delete_document(CUSTOMER_BUSINESSES_COLLECTION, ts_id)
        return

    doc = document_to_dict(payload.value)
    doc["id"] = ts_id
    # owner_id is the path segment -> always present, the access boundary.
    doc["owner_id"] = customer_id

    # business_ref (DocumentReference) -> business_id string. document_to_dict
    # already converted any reference to its id; normalize the field name.
    if "business_ref" in doc and "business_id" not in doc:
        doc["business_id"] = doc.pop("business_ref")
    else:
        doc.pop("business_ref", None)

    doc["name"] = str(doc.get("name") or "")
    for field in ("email", "phone"):
        if field in doc and doc[field] is not None:
            doc[field] = str(doc[field])
        else:
            doc.pop(field, None)

    for count_field in ("campaign_count", "hit_count"):
        if count_field in doc and doc[count_field] is not None:
            try:
                doc[count_field] = int(doc[count_field])
            except (TypeError, ValueError):
                doc.pop(count_field, None)

    if not doc.get("owner_id"):
        logger.error(
            "customer_businesses upsert SKIPPED: id=%s missing access field owner_id",
            ts_id,
        )
        return

    logger.info("customer_businesses upsert: id=%s owner_id=%s", ts_id, customer_id)
    upsert_document(CUSTOMER_BUSINESSES_COLLECTION, doc)
