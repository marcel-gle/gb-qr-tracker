import hashlib
import os
import re
import time
from datetime import datetime, timezone

import functions_framework
import firebase_admin
from firebase_admin import firestore, initialize_app

if not firebase_admin._apps:
    initialize_app()

db = firestore.client()
COL_INBOUND_LEADS = db.collection("inbound_leads")
SERVER_TIMESTAMP = firestore.SERVER_TIMESTAMP
Increment = firestore.Increment

# Max number of lightweight lead snapshots to keep on the business overlay
LEAD_RECENT_MAX = 5

GENERIC_DOMAINS = {
    "gmail.com", "googlemail.com",
    "hotmail.com", "hotmail.de",
    "outlook.com", "outlook.de",
    "yahoo.com", "yahoo.de",
    "web.de", "gmx.de", "gmx.net",
    "icloud.com", "me.com",
    "t-online.de", "freenet.de",
    "aol.com"
}


def extract_domain(email: str) -> str | None:
    """Extracts and normalises domain from an email address."""
    try:
        return email.strip().lower().split("@")[1]
    except IndexError:
        return None


def normalize_phone(phone: str | None) -> str | None:
    """Strip to digits only. Returns None if empty."""
    if phone is None:
        return None
    digits = re.sub(r"\D", "", phone)
    return digits if digits else None


def _str_or_none(s: str | None) -> str | None:
    if s is None:
        return None
    v = (s or "").strip()
    return v if v else None


def _field_by_type(fields: list, field_type: str) -> str | None:
    """Get value from OnePage fields array by fieldType."""
    if not fields or not isinstance(fields, list):
        return None
    for f in fields:
        if isinstance(f, dict) and (f.get("fieldType") or "").strip().lower() == field_type.strip().lower():
            v = f.get("value")
            return _str_or_none(v) if v is not None else None
    return None


def _field_by_label(fields: list, label_substring: str) -> str | None:
    """Get value from OnePage fields array by label containing given substring."""
    if not fields or not isinstance(fields, list):
        return None
    label_sub = label_substring.strip().lower()
    for f in fields:
        if isinstance(f, dict):
            lbl = (f.get("label") or "").strip().lower()
            if label_sub in lbl or lbl in label_sub:
                v = f.get("value")
                return _str_or_none(v) if v is not None else None
    return None


def parse_onepage_webhook(lead_data_body: dict, lead_data_header: dict | None) -> dict | None:
    """
    Parse OnePage lead.created webhook payload into flat lead fields.
    Accepts lead_data_body as either:
      - { "type": "lead.created", "data": { ... } } (body only), or
      - { "body": { "type": "lead.created", "data": { ... } }, "headers": { ... } } (full webhook item).
    Returns a dict with: email, salutation, first_name, last_name, phone, company_name,
    referrer, dedupe_key, raw (full payload for debugging). Returns None if not a valid OnePage payload.
    """
    if not isinstance(lead_data_body, dict):
        return None
    # Unwrap if n8n sent the full webhook item as lead_data_body
    if "body" in lead_data_body and isinstance(lead_data_body.get("body"), dict):
        lead_data_header = lead_data_header or (lead_data_body.get("headers") if isinstance(lead_data_body.get("headers"), dict) else None)
        lead_data_body = lead_data_body["body"]
    data = lead_data_body.get("data") if lead_data_body.get("type") == "lead.created" else lead_data_body
    if not isinstance(data, dict):
        data = lead_data_body
    fields = data.get("fields") if isinstance(data.get("fields"), list) else []
    source = data.get("source") if isinstance(data.get("source"), dict) else {}
    page = source.get("page") if isinstance(source.get("page"), dict) else {}

    email = _field_by_type(fields, "email") or _field_by_label(fields, "e-mail") or _field_by_label(fields, "email")
    first_name = _field_by_type(fields, "fname") or _field_by_label(fields, "vorname")
    last_name = _field_by_type(fields, "lname") or _field_by_label(fields, "nachname")
    phone = _field_by_type(fields, "phone") or _field_by_label(fields, "handy") or _field_by_label(fields, "telefon") or _field_by_label(fields, "phone")
    salutation = _field_by_label(fields, "anrede")
    company_name = _field_by_label(fields, "firma") or _field_by_label(fields, "unternehmen") or _field_by_label(fields, "company")

    referrer = None
    if page:
        referrer = _str_or_none(page.get("slug")) or _str_or_none(page.get("title"))

    dedupe_key = _str_or_none(data.get("id"))

    raw = {"onepage_body": lead_data_body}
    if lead_data_header and isinstance(lead_data_header, dict):
        raw["onepage_headers"] = lead_data_header

    return {
        "email": email,
        "salutation": salutation,
        "first_name": first_name,
        "last_name": last_name,
        "phone": phone,
        "company_name": company_name,
        "referrer": referrer,
        "dedupe_key": dedupe_key,
        "raw": raw,
    }


def build_lead_payload(
    *,
    customer_id: str,
    owner_id: str,
    customer_ref: firestore.DocumentReference,
    salutation: str | None,
    first_name: str | None,
    last_name: str | None,
    email: str | None,
    phone: str | None,
    company_name: str | None,
    referrer: str | None,
    email_norm: str | None,
    phone_norm: str | None,
    match_status: str,
    match_confidence: float | None,
    match_reasons: list[str],
    business_id: str | None,
    business_ref: firestore.DocumentReference | None,
    link_id: str | None,
    campaign_ref: firestore.DocumentReference | None,
    target_ref: firestore.DocumentReference | None,
    dedupe_key: str,
    raw: dict | None,
) -> dict:
    """Build the lead document payload (excluding created_at, updated_at, seen_count)."""
    return {
        "salutation": salutation,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "company_name": company_name,
        "referrer": referrer,
        "email_norm": email_norm,
        "phone_norm": phone_norm,
        "owner_id": owner_id,
        "customer_ref": customer_ref,
        "match_status": match_status,
        "match_confidence": match_confidence,
        "match_reasons": match_reasons,
        "business_id": business_id,
        "business_ref": business_ref,
        "link_id": link_id,
        "campaign_ref": campaign_ref,
        "target_ref": target_ref,
        "dedupe_key": dedupe_key,
        "raw": raw,
    }


@functions_framework.http
def find_business(request):
    """
    Matches an incoming lead to a business by email domain and persists an inbound_lead doc.

    Input (flat):
        POST { "customer_id": "...", "email": "...", optional: salutation, first_name, ... }

    Input (OnePage webhook from n8n):
        POST { "customer_id": "...", "lead_data_body": { "type": "lead.created", "data": { "id", "fields", "source", ... } }, "lead_data_header": { ... } }
        Fields are parsed from data.fields (by fieldType: email, fname, lname, phone; by label: Anrede, etc.); data.id → dedupe_key; source.page.slug/title → referrer.

    Matching steps:
        1. Extract domain from lead email
        2. Reject generic domains (gmail, hotmail etc.) immediately
        3. Query businesses where domain == lead_domain

    Output (match):
        { "match": true, "businessId": "...", "businessName": "...", "leadId": "..." }

    Output (no match):
        { "match": false, "reason": "...", "leadId": "..." }
    """

    secret = request.headers.get("X-Internal-Secret", "")
    if secret != os.environ.get("INTERNAL_SECRET"):
        return ({"error": "Unauthorized"}, 401)

    if request.method != "POST":
        return ({"error": "Only POST allowed"}, 405)

    body = request.get_json(silent=True)
    if not body:
        return ({"error": "Invalid or missing JSON body"}, 400)

    customer_id = (body.get("customer_id") or "").strip()
    lead_data_body = body.get("lead_data_body")
    lead_data_header = body.get("lead_data_header")

    # --- Support OnePage webhook format: { customer_id, lead_data_body, lead_data_header } ---
    if isinstance(lead_data_body, dict):
        onepage = parse_onepage_webhook(lead_data_body, lead_data_header if isinstance(lead_data_header, dict) else None)
        if onepage:
            email = (onepage.get("email") or "").strip().lower()
            salutation = onepage.get("salutation")
            first_name = onepage.get("first_name")
            last_name = onepage.get("last_name")
            phone = onepage.get("phone")
            company_name = onepage.get("company_name")
            referrer = onepage.get("referrer")
            request_dedupe_key = onepage.get("dedupe_key")
            raw = onepage.get("raw")
            # Top-level overrides (optional when using OnePage)
            if body.get("owner_id"):
                owner_id = (body.get("owner_id") or "").strip() or customer_id
            else:
                owner_id = customer_id
            link_id = _str_or_none(body.get("link_id"))
            campaign_id = (body.get("campaign_id") or "").strip() or None
            target_id = (body.get("target_id") or "").strip() or None
        else:
            email = (body.get("email") or "").strip().lower()
            salutation = _str_or_none(body.get("salutation"))
            first_name = _str_or_none(body.get("first_name"))
            last_name = _str_or_none(body.get("last_name"))
            phone = _str_or_none(body.get("phone"))
            company_name = _str_or_none(body.get("company_name"))
            referrer = _str_or_none(body.get("referrer"))
            request_dedupe_key = (body.get("dedupe_key") or "").strip() or None
            raw = body.get("raw") if isinstance(body.get("raw"), dict) else None
            owner_id = (body.get("owner_id") or "").strip() or customer_id
            link_id = _str_or_none(body.get("link_id"))
            campaign_id = (body.get("campaign_id") or "").strip() or None
            target_id = (body.get("target_id") or "").strip() or None
    else:
        email = (body.get("email") or "").strip().lower()
        salutation = _str_or_none(body.get("salutation"))
        first_name = _str_or_none(body.get("first_name"))
        last_name = _str_or_none(body.get("last_name"))
        phone = _str_or_none(body.get("phone"))
        company_name = _str_or_none(body.get("company_name"))
        referrer = _str_or_none(body.get("referrer"))
        owner_id = (body.get("owner_id") or "").strip() or customer_id
        link_id = _str_or_none(body.get("link_id"))
        campaign_id = (body.get("campaign_id") or "").strip() or None
        target_id = (body.get("target_id") or "").strip() or None
        request_dedupe_key = (body.get("dedupe_key") or "").strip() or None
        raw = body.get("raw") if isinstance(body.get("raw"), dict) else None

    if not customer_id:
        return ({"error": "customer_id is required"}, 400)
    if not email:
        return ({"error": "email is required (provide it or use lead_data_body with an email field)"}, 400)

    email_norm = email
    phone_norm = normalize_phone(phone)

    customer_ref = db.collection("customers").document(customer_id)
    campaign_ref: firestore.DocumentReference | None = None
    target_ref: firestore.DocumentReference | None = None
    if campaign_id:
        campaign_ref = db.collection("campaigns").document(campaign_id)
        if target_id:
            target_ref = campaign_ref.collection("targets").document(target_id)

    # --- Idempotency: look up existing lead by dedupe_key ---
    existing_lead_ref = None
    if request_dedupe_key:
        existing = (
            COL_INBOUND_LEADS.where("customer_ref", "==", customer_ref)
            .where("dedupe_key", "==", request_dedupe_key)
            .limit(1)
            .get()
        )
        if existing:
            existing_lead_ref = existing[0].reference

    # --- Run domain matching ---
    match_status = "unmatched"
    match_confidence: float | None = None
    match_reasons: list[str] = []
    business_id: str | None = None
    business_ref: firestore.DocumentReference | None = None
    domain: str | None = None
    response_match = False
    response_reason = ""
    response_business_id = None
    response_business_name = ""
    response_domain = None

    try:
        domain = extract_domain(email)
        if not domain:
            match_status = "unmatched"
            match_reasons = ["invalid_email"]
            response_reason = "invalid_email"
        elif domain in GENERIC_DOMAINS:
            match_status = "unmatched"
            match_reasons = ["generic_domain"]
            response_reason = "generic_domain"
            response_domain = domain
        else:
            docs = (
                db.collection(f"customers/{customer_id}/businesses")
                .where("domain", "==", domain)
                .limit(1)
                .stream()
            )
            matched_doc = next(docs, None)
            if not matched_doc:
                match_status = "unmatched"
                match_reasons = ["no_match"]
                response_reason = "no_match"
                response_domain = domain
            else:
                data = matched_doc.to_dict()
                match_status = "matched"
                match_confidence = 1.0
                match_reasons = ["domain_match"]
                business_id = matched_doc.id
                business_ref = db.collection("customers").document(customer_id).collection("businesses").document(matched_doc.id)
                response_match = True
                response_business_id = matched_doc.id
                response_business_name = (data or {}).get("name", "")
                response_domain = domain
    except Exception as e:
        match_status = "error"
        match_reasons = [str(e)]

    # --- Dedupe key for new doc if not from request ---
    dedupe_key = request_dedupe_key or hashlib.sha256(
        f"{customer_id}|{email_norm}|{time.time_ns()}".encode()
    ).hexdigest()

    lead_payload = build_lead_payload(
        customer_id=customer_id,
        owner_id=owner_id,
        customer_ref=customer_ref,
        salutation=salutation,
        first_name=first_name,
        last_name=last_name,
        email=email or None,
        phone=phone,
        company_name=company_name,
        referrer=referrer,
        email_norm=email_norm or None,
        phone_norm=phone_norm,
        match_status=match_status,
        match_confidence=match_confidence,
        match_reasons=match_reasons,
        business_id=business_id,
        business_ref=business_ref,
        link_id=link_id,
        campaign_ref=campaign_ref,
        target_ref=target_ref,
        dedupe_key=dedupe_key,
        raw=raw,
    )

    try:
        if existing_lead_ref:
            existing_lead_ref.update({
                "updated_at": SERVER_TIMESTAMP,
                "seen_count": firestore.Increment(1),
                "match_status": match_status,
                "match_confidence": match_confidence,
                "match_reasons": match_reasons,
                "business_id": business_id,
                "business_ref": business_ref,
            })
            lead_id = existing_lead_ref.id
        else:
            new_ref = COL_INBOUND_LEADS.add({
                **lead_payload,
                "created_at": SERVER_TIMESTAMP,
                "updated_at": SERVER_TIMESTAMP,
                "seen_count": 1,
            })
            lead_id = new_ref[1].id
    except Exception as e:
        return ({"error": str(e)}, 500)

    if match_status == "error":
        return ({"error": match_reasons[0] if match_reasons else "Unknown error", "leadId": lead_id}, 500)

    if response_match:
        # Update business overlay: lead totals and recent snapshots
        try:
            inbound_lead_ref = COL_INBOUND_LEADS.document(lead_id)
            overlay_ref = db.collection("customers").document(customer_id).collection("businesses").document(response_business_id)
            leadmagnet_key = None
            if campaign_ref:
                leadmagnet_key = f"campaign:{campaign_ref.id}"
            elif link_id:
                leadmagnet_key = f"link:{link_id}"
            # Use datetime in array; Firestore does not allow SERVER_TIMESTAMP inside array elements
            new_snapshot = {
                "inbound_ref": inbound_lead_ref,
                "created_at": datetime.now(timezone.utc),
                "leadmagnet_key": leadmagnet_key,
                "campaign_ref": campaign_ref,
                "target_ref": target_ref,
                "link_id": link_id,
                "email": email or None,
                "phone": phone,
            }
            snap = overlay_ref.get()
            existing = (snap.to_dict() or {}).get("lead_recent") or []
            if not isinstance(existing, list):
                existing = []
            lead_recent = [new_snapshot] + existing[: LEAD_RECENT_MAX - 1]
            overlay_ref.set({
                "lead_total_count": Increment(1),
                "lead_last_at": SERVER_TIMESTAMP,
                "lead_last_inbound_ref": inbound_lead_ref,
                "lead_recent": lead_recent,
            }, merge=True)
        except Exception as e:
            # Don't fail the request; lead is already stored
            print(f"[n8n_find_business] Business overlay update failed: {e}")

        return ({
            "match": True,
            "businessId": response_business_id,
            "businessName": response_business_name,
            "domain": response_domain,
            "leadId": lead_id,
        }, 200)

    return ({
        "match": False,
        "reason": response_reason,
        "domain": response_domain,
        "leadId": lead_id,
    }, 200)
