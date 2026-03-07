# Email reminder scheduler: runs on schedule, evaluates each customer's
# notifications (newScansWithinHours, unscannedToday, weeklyDigest), queries
# data, and sends HTML emails via Google SMTP relay.

import os
import logging
import ssl
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import functions_framework
from flask import Request
from google.cloud import firestore

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_db: Optional[firestore.Client] = None

# Config from environment
PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT")
REMINDER_SECRET = os.environ.get("REMINDER_SECRET", "")
# Google SMTP relay (smtp-relay.gmail.com, port 587, STARTTLS)
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp-relay.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_APP_PASSWORD = os.environ.get("SMTP_APP_PASSWORD", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "noreply@example.com")
FROM_NAME = os.environ.get("FROM_NAME", "QR Tracker")
# Default send time for unscannedToday when not set (local time)
DEFAULT_UNSCANNED_SEND_AT = "20:00"
# Window in minutes: send if current time is within [sendAt, sendAt + WINDOW_MINUTES]
SEND_WINDOW_MINUTES = 15
# Max hits to fetch per query
MAX_HITS_NEW_SCANS = 500
MAX_HITS_WEEKLY = 5000
# URLs for CTA and footer (set via env or leave #)
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "#")
SETTINGS_URL = os.environ.get("SETTINGS_URL", "#")
UNSUBSCRIBE_URL = os.environ.get("UNSUBSCRIBE_URL", "#")

# German weekday and month names for date formatting
_WEEKDAY_DE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
_MONTH_DE = ["", "Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"]
_AVATAR_COLORS = ["#1a7fa8", "#22a06b", "#e8871a", "#9333ea", "#0e7a5f"]


def _get_db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(project=PROJECT_ID)
    return _db


def _ts_to_datetime(ts: Any) -> Optional[datetime]:
    """Convert Firestore timestamp to timezone-aware UTC datetime."""
    if ts is None:
        return None
    if hasattr(ts, "timestamp"):
        return datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc)
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
    return None


def _authenticate_request(request: Request) -> bool:
    """Validate caller: Cloud Scheduler (User-Agent) or REMINDER_SECRET in header/query."""
    if not request:
        return False
    user_agent = request.headers.get("User-Agent", "")
    if "Google-Cloud-Scheduler" in user_agent:
        return True
    secret = request.headers.get("X-Reminder-Secret") or request.args.get("secret")
    if REMINDER_SECRET and secret == REMINDER_SECRET:
        return True
    return False


def _customer_now(customer_tz: str) -> Tuple[datetime, datetime]:
    """Return (now_utc, now_local) for the customer's timezone."""
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(customer_tz)
    except Exception:
        tz = timezone.utc
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(tz)
    return now_utc, now_local


def _parse_send_at(s: Optional[str], default: str = "09:00") -> Tuple[int, int]:
    """Parse 'HH:MM' to (hour, minute)."""
    if not s or not isinstance(s, str):
        s = default
    parts = s.strip().split(":")
    h = int(parts[0]) if parts else 9
    m = int(parts[1]) if len(parts) > 1 else 0
    return h, m


def _in_send_window(now_local: datetime, send_at: str, window_minutes: int = SEND_WINDOW_MINUTES) -> bool:
    """True if now_local time is within [sendAt, sendAt + window_minutes]."""
    h, m = _parse_send_at(send_at)
    start = now_local.replace(hour=h, minute=m, second=0, microsecond=0)
    end = start + timedelta(minutes=window_minutes)
    t = now_local.time()
    return start.time() <= t <= end.time()


def _format_date_time(now_local: datetime) -> str:
    """Format as e.g. 'Sonntag, 22. Februar 2026 · 08:00 Uhr' (German)."""
    wd = now_local.weekday()
    day_name = _WEEKDAY_DE[wd] if 0 <= wd < 7 else ""
    month = now_local.month
    month_name = _MONTH_DE[month] if 1 <= month <= 12 else str(month)
    return f"{day_name}, {now_local.day}. {month_name} {now_local.year} · {now_local.strftime('%H:%M')} Uhr"


def _greeting(now_local: datetime, display_name: str) -> str:
    """German greeting: Guten Morgen before ~12, else Hallo."""
    name = (display_name or "").strip() or "du"
    if 5 <= now_local.hour < 12:
        return f"Guten Morgen, {name} 👋"
    return f"Hallo {name},"


def _initials(name: str, fallback: str = "?") -> str:
    """First 2 letters (uppercase) for avatar, or fallback."""
    if not name or not isinstance(name, str):
        return (fallback or "?")[:2].upper()
    parts = name.strip().split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()[:2]
    return (name[:2] if len(name) >= 2 else name).upper()[:2]


def _avatar_color(index: int) -> str:
    return _AVATAR_COLORS[index % len(_AVATAR_COLORS)]


def _escape_html(s: str) -> str:
    """Escape for HTML text content."""
    if not s:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# ---------- Data queries ----------
#
# What the function queries (Firestore):
#
# 1. newScansWithinHours email:
#    - Collection: hits
#    - Query: owner_id == customer_uid AND ts >= (now_utc - hours)
#    - Order: ts DESC, limit MAX_HITS_NEW_SCANS (500)
#    - Returns: list of hit docs (link_id, ts, campaign_name, business_ref.id)
#
# 2. unscannedToday (scans not contacted) email:
#    - Collection: customers/{uid}/businesses (customer overlays)
#    - Query: stream all docs; filter in memory: hit_count > 0 AND (no contacted_at OR last_hit_at > contacted_at)
#    - Then: for each uncontacted overlay, ref.get() on business_ref to read canonical business_name
#    - Returns: list of uncontacted businesses with business_name resolved
#
# 3. weeklyDigest email:
#    - Collection: hits
#    - Query: owner_id == customer_uid AND ts >= (now_utc - 7 days)
#    - Order: ts DESC, limit MAX_HITS_WEEKLY (5000)
#    - Aggregates in memory: total count, by_day (date -> count), by_campaign (name -> count); top 10 campaigns
#    - Returns: { total_scans, scans_by_day, top_campaigns, days }
#
# Normal run: streams collection "customers" (all customer docs) and for each runs the above queries
# only when that customer has the corresponding notification enabled and "should send" passes.
# Test mode: fetches only the customer doc where email == test_email (single query), then runs
# the same data queries (hits, customers/uid/businesses) only for that uid.


def _query_new_scans_within_hours(
    db: firestore.Client,
    owner_id: str,
    hours: int,
) -> List[Dict[str, Any]]:
    """Return list of hits for owner in the last N hours (for newScansWithinHours email)."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    logger.info(
        "query_new_scans_within_hours: collection=hits owner_id=%s hours=%s since=%s",
        owner_id, hours, since.isoformat(),
    )
    hits_ref = db.collection("hits")
    query = (
        hits_ref.where("owner_id", "==", owner_id)
        .where("ts", ">=", since)
        .order_by("ts", direction=firestore.Query.DESCENDING)
        .limit(MAX_HITS_NEW_SCANS)
    )
    out = []
    for doc in query.stream():
        d = doc.to_dict() or {}
        ts = _ts_to_datetime(d.get("ts"))
        out.append({
            "link_id": d.get("link_id") or "",
            "ts": ts,
            "campaign_name": d.get("campaign_name") or "",
            "business_id": d.get("business_ref").id if getattr(d.get("business_ref"), "id", None) else None,
        })
    logger.info("query_new_scans_within_hours: returned %d hits", len(out))
    return out


def _query_uncontacted_businesses(
    db: firestore.Client,
    owner_id: str,
) -> List[Dict[str, Any]]:
    """
    Return businesses with hits that have not been contacted.
    Option A: overlay has hit_count > 0 and (no contacted_at or last_hit_at > contacted_at).
    """
    overlays_ref = db.collection("customers").document(owner_id).collection("businesses")
    logger.info(
        "query_uncontacted_businesses: collection=customers/%s/businesses streaming overlays",
        owner_id,
    )
    uncontacted = []
    for doc in overlays_ref.stream():
        d = doc.to_dict() or {}
        hit_count = d.get("hit_count") or 0
        if hit_count <= 0:
            continue
        last_hit_at = _ts_to_datetime(d.get("last_hit_at"))
        contacted_at = _ts_to_datetime(d.get("contacted_at"))
        if contacted_at is not None and last_hit_at is not None and last_hit_at <= contacted_at:
            continue
        uncontacted.append({
            "business_id": doc.id,
            "business_ref": d.get("business_ref"),
            "last_hit_at": last_hit_at,
            "hit_count": hit_count,
            "name": d.get("name"),
            "email": d.get("email"),
        })
    # Resolve business names from canonical businesses (dedupe refs)
    seen_refs: Dict[str, Any] = {}
    for x in uncontacted:
        ref = x.get("business_ref")
        if ref is not None:
            seen_refs[ref.id] = ref
    names_by_id = {}
    for ref in seen_refs.values():
        if ref is None:
            continue
        try:
            snap = ref.get()
            if snap.exists:
                names_by_id[ref.id] = (snap.to_dict() or {}).get("business_name") or ref.id
        except Exception:
            names_by_id[ref.id] = ref.id
    for row in uncontacted:
        row["business_name"] = row.get("name") or names_by_id.get(row["business_id"], row["business_id"])
    logger.info(
        "query_uncontacted_businesses: owner_id=%s returned %d uncontacted businesses (fetched %d canonical names)",
        owner_id, len(uncontacted), len(names_by_id),
    )
    return uncontacted


def _query_weekly_digest(
    db: firestore.Client,
    owner_id: str,
    days: int = 7,
) -> Dict[str, Any]:
    """Aggregate hits for owner in the last N days for weekly digest."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    logger.info(
        "query_weekly_digest: collection=hits owner_id=%s days=%s since=%s",
        owner_id, days, since.isoformat(),
    )
    hits_ref = db.collection("hits")
    query = (
        hits_ref.where("owner_id", "==", owner_id)
        .where("ts", ">=", since)
        .order_by("ts", direction=firestore.Query.DESCENDING)
        .limit(MAX_HITS_WEEKLY)
    )
    total = 0
    by_day: Dict[str, int] = {}
    by_campaign: Dict[str, int] = {}
    for doc in query.stream():
        d = doc.to_dict() or {}
        total += 1
        ts = _ts_to_datetime(d.get("ts"))
        if ts:
            day_key = ts.date().isoformat()
            by_day[day_key] = by_day.get(day_key, 0) + 1
        camp = d.get("campaign_name") or "(unknown)"
        by_campaign[camp] = by_campaign.get(camp, 0) + 1
    top_campaigns = sorted(by_campaign.items(), key=lambda x: -x[1])[:10]
    logger.info(
        "query_weekly_digest: owner_id=%s total_scans=%d campaigns=%d",
        owner_id, total, len(by_campaign),
    )
    return {
        "total_scans": total,
        "scans_by_day": by_day,
        "top_campaigns": top_campaigns,
        "days": days,
    }


# ---------- Templates and render ----------


def _load_template(name: str) -> str:
    """Load HTML template from templates/ directory next to main.py."""
    base = Path(__file__).resolve().parent
    path = base / "templates" / f"{name}.html"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _render_new_scans(
    hits: List[Dict[str, Any]],
    hours: int,
    display_name: str,
    now_local: datetime,
) -> str:
    html = _load_template("new_scans_within_hours")
    if not html:
        html = (
            "<!DOCTYPE html><html><body>"
            "<h1>{{count}} neue Scans in den letzten {{hours}} Stunden</h1>"
            "{{scan_list}}</body></html>"
        )
    # Build scan-item list (show first 3, then "+ N weitere" row)
    show_count = 3
    campaigns_set = {h.get("campaign_name") or "(unknown)" for h in hits}
    campaigns_count = len(campaigns_set)
    scan_items = []
    for i, h in enumerate(hits[:show_count]):
        ts_str = h["ts"].strftime("%H:%M Uhr") if h.get("ts") else ""
        name = h.get("business_id") or h.get("campaign_name") or "Scan"
        initials = _initials(str(name), "S")
        color = _avatar_color(i)
        scan_items.append(
            f'<div class="scan-item">'
            f'<div class="scan-left">'
            f'<div class="scan-avatar" style="background:{color};">{initials}</div>'
            f'<div><div class="scan-name">{_escape_html(name)}</div>'
            f'<div class="scan-url">Kampagne: {_escape_html(h.get("campaign_name") or "—")}</div></div>'
            f'</div><div class="scan-time">{ts_str}</div></div>'
        )
    rest = len(hits) - show_count
    if rest > 0:
        scan_items.append(
            f'<div class="scan-item" style="background:#f5f7fa; border-style:dashed;">'
            f'<div class="scan-left" style="color:#8a96a3; font-size:13px;">+ {rest} weitere neue Scans im Zeitfenster</div><div></div></div>'
        )
    scan_list = "\n".join(scan_items) if scan_items else "<p>Keine neuen Scans.</p>"
    percent_change = ""  # Optional: e.g. " Das ist +85 % im Vergleich zum Tagesdurchschnitt."
    replacements = {
        "{{count}}": str(len(hits)),
        "{{hours}}": str(hours),
        "{{date_time}}": _format_date_time(now_local),
        "{{greeting}}": _greeting(now_local, display_name),
        "{{scan_list}}": scan_list,
        "{{campaigns_count}}": str(campaigns_count),
        "{{percent_change}}": percent_change,
        "{{dashboard_url}}": DASHBOARD_URL,
        "{{settings_url}}": SETTINGS_URL,
        "{{unsubscribe_url}}": UNSUBSCRIBE_URL,
    }
    for k, v in replacements.items():
        html = html.replace(k, v)
    return html


def _render_unscanned_today(
    businesses: List[Dict[str, Any]],
    display_name: str,
    now_local: datetime,
) -> str:
    html = _load_template("unscanned_today")
    if not html:
        html = (
            "<!DOCTYPE html><html><body>"
            "<h1>{{count}} Scans ohne Kontakt</h1>{{business_list}}</body></html>"
        )
    show_count = 3
    items = []
    for i, b in enumerate(businesses[:show_count]):
        name = b.get("business_name") or b.get("business_id") or "—"
        initials = _initials(str(name), "?")
        color = _avatar_color(i)
        ts_val = b.get("last_hit_at")
        if ts_val:
            ts_str = ts_val.strftime("%H:%M Uhr") if hasattr(ts_val, "strftime") else str(ts_val)
        else:
            ts_str = "—"
        campaign = "—"  # Campaign could be resolved from links if needed
        items.append(
            f'<div class="scan-item">'
            f'<div class="scan-left">'
            f'<div class="scan-avatar" style="background:{color};">{_escape_html(initials)}</div>'
            f'<div><div class="scan-name">{_escape_html(name)}</div>'
            f'<div class="scan-url">Kampagne: {_escape_html(campaign)}</div></div>'
            f'</div><div class="scan-time">{ts_str}</div></div>'
        )
    rest = len(businesses) - show_count
    if rest > 0:
        items.append(
            f'<div class="scan-item" style="background:#f5f7fa; border-style:dashed;">'
            f'<div class="scan-left" style="color:#8a96a3; font-size:13px;">+ {rest} weitere Aufrufe ohne Kontakt</div><div></div></div>'
        )
    business_list = "\n".join(items) if items else "<p>Keine offenen Scans.</p>"
    replacements = {
        "{{count}}": str(len(businesses)),
        "{{date_time}}": _format_date_time(now_local),
        "{{greeting}}": _greeting(now_local, display_name),
        "{{business_list}}": business_list,
        "{{dashboard_url}}": DASHBOARD_URL,
        "{{settings_url}}": SETTINGS_URL,
        "{{unsubscribe_url}}": UNSUBSCRIBE_URL,
    }
    for k, v in replacements.items():
        html = html.replace(k, v)
    return html


def _render_weekly_digest(
    data: Dict[str, Any],
    display_name: str,
    now_local: datetime,
) -> str:
    html = _load_template("weekly_digest")
    if not html:
        html = (
            "<!DOCTYPE html><html><body>"
            "<h1>Deine Woche im Überblick</h1>"
            "{{total_scans}} {{scans_by_day}} {{campaigns_table}}</body></html>"
        )
    total = data.get("total_scans", 0)
    by_day = data.get("scans_by_day") or {}
    top_campaigns = data.get("top_campaigns") or []
    # Aggregate by weekday for progress bars
    from datetime import date as date_type
    by_weekday: Dict[int, int] = {}
    for day_key, c in by_day.items():
        try:
            d = date_type.fromisoformat(day_key)
            by_weekday[d.weekday()] = by_weekday.get(d.weekday(), 0) + c
        except Exception:
            pass
    max_day = max(by_weekday.values(), default=1)
    # Date range string (e.g. "16. – 22. Februar 2026 · KW 8")
    try:
        end_date = now_local.date() if hasattr(now_local, "date") else now_local
        start_date = end_date - timedelta(days=6)
        month_name = _MONTH_DE[end_date.month] if 1 <= end_date.month <= 12 else str(end_date.month)
        iso_week = end_date.isocalendar()[1]
        date_range = f"{start_date.day}. – {end_date.day}. {month_name} {end_date.year} · KW {iso_week}"
    except Exception:
        date_range = _format_date_time(now_local)
    # Campaign table rows
    campaign_rows = []
    for camp, count in top_campaigns:
        campaign_rows.append(
            f"<tr><td><strong>{_escape_html(camp)}</strong></td>"
            f"<td>{count}</td><td>—</td>"
            f'<td><span class="trend flat">→ —</span></td></tr>'
        )
    campaigns_table = "\n".join(campaign_rows) if campaign_rows else "<tr><td colspan=\"4\">Keine Kampagnen in dieser Woche.</td></tr>"
    # Progress rows for each weekday (Montag=0 … Sonntag=6)
    progress_rows = []
    day_names_de = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    for wd in range(7):
        day_count = by_weekday.get(wd, 0)
        pct = (int(100 * day_count / max_day)) if max_day else 0
        progress_rows.append(
            f'<div class="progress-row">'
            f'<div class="progress-label"><span>{day_names_de[wd]}</span><strong>{day_count}</strong></div>'
            f'<div class="progress-track"><div class="progress-fill" style="width:{min(100, pct)}%; background:#1a7fa8;"></div></div>'
            f'</div>'
        )
    scans_by_day_html = "\n".join(progress_rows)
    replacements = {
        "{{total_scans}}": str(total),
        "{{days}}": str(data.get("days", 7)),
        "{{date_range}}": date_range,
        "{{greeting}}": _greeting(now_local, display_name),
        "{{uncontacted_count}}": "—",
        "{{contacted_count}}": "—",
        "{{campaigns_table}}": campaigns_table,
        "{{scans_by_day}}": scans_by_day_html,
        "{{dashboard_url}}": DASHBOARD_URL,
        "{{settings_url}}": SETTINGS_URL,
        "{{unsubscribe_url}}": UNSUBSCRIBE_URL,
    }
    for k, v in replacements.items():
        html = html.replace(k, v)
    return html


# ---------- Email send ----------


def _send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send email via Google SMTP relay (STARTTLS on port 587). Returns True on success."""
    if not SMTP_USER or not SMTP_APP_PASSWORD:
        logger.warning("SMTP not configured (SMTP_USER / SMTP_APP_PASSWORD); skipping send to %s", to_email)
        return False

    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>" if FROM_NAME else FROM_EMAIL
    msg["To"] = to_email

    # Important for STARTTLS
    context = ssl.create_default_context()

    try:
        logger.info(
            "_send_email: to=%s subject=%s body_len=%d host=%s port=%s from=%s user=%s",
            to_email, subject, len(html_body), SMTP_HOST, SMTP_PORT, FROM_EMAIL, SMTP_USER,
        )

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            # Enable temporarily if you want full SMTP transcript in logs
            # s.set_debuglevel(1)

            # 1) Explicit greeting
            s.ehlo()

            # 2) Upgrade to TLS with a real SSL context
            s.starttls(context=context)

            # 3) Re-EHLO after TLS
            s.ehlo()

            # 4) SMTP AUTH
            s.login(SMTP_USER, SMTP_APP_PASSWORD)

            # 5) Explicit envelope sender/rcpt
            s.send_message(msg, from_addr=FROM_EMAIL, to_addrs=[to_email])

        logger.info("Email sent successfully to %s subject=%s", to_email, subject)
        return True

    except Exception as e:
        logger.exception("SMTP send failed to=%s subject=%s error=%s", to_email, subject, e)
        return False


# ---------- Should-send and lastSentAt ----------


def _should_send_new_scans(
    config: Dict[str, Any],
    now_utc: datetime,
    last_sent_at: Optional[datetime],
    hours: int,
) -> bool:
    """Send at most once per N hours."""
    if last_sent_at is None:
        return True
    return (now_utc - last_sent_at).total_seconds() >= hours * 3600


def _should_send_unscanned_today(
    config: Dict[str, Any],
    now_local: datetime,
    last_sent_at: Optional[datetime],
) -> bool:
    """Send at most once per day; only when in send window (e.g. 20:00 local)."""
    send_at = (config or {}).get("sendAt") or DEFAULT_UNSCANNED_SEND_AT
    if not _in_send_window(now_local, send_at):
        return False
    if last_sent_at is None:
        return True
    last_local = last_sent_at.astimezone(now_local.tzinfo) if now_local.tzinfo else last_sent_at
    return last_local.date() < now_local.date()


def _should_send_weekly_digest(
    config: Dict[str, Any],
    now_local: datetime,
    last_sent_at: Optional[datetime],
) -> bool:
    """Send only on configured day at sendAt, at most once per week."""
    day_of_week = (config or {}).get("dayOfWeek", 0)
    send_at = (config or {}).get("sendAt") or "07:00"
    if now_local.weekday() != day_of_week:
        return False
    if not _in_send_window(now_local, send_at):
        return False
    if last_sent_at is None:
        return True
    return (now_local - last_sent_at.astimezone(now_local.tzinfo)).days >= 6


# ---------- Main run ----------


def _process_customer(db: firestore.Client, customer_id: str, data: Dict[str, Any]) -> None:
    email = (data.get("email") or "").strip()
    if not email:
        logger.debug("_process_customer: skip customer_id=%s (no email)", customer_id)
        return
    if data.get("is_active") is False:
        logger.debug("_process_customer: skip customer_id=%s (is_active=false)", customer_id)
        return
    logger.info("_process_customer: processing customer_id=%s email=%s", customer_id, email)
    settings = data.get("settings") or {}
    tz = settings.get("timezone") or "Europe/Berlin"
    notifications = data.get("notifications") or {}
    now_utc, now_local = _customer_now(tz)

    customer_ref = db.collection("customers").document(customer_id)
    updates: Dict[str, Any] = {}

    display_name = (data.get("display_name") or data.get("email") or "").strip() or "du"
    # ----- newScansWithinHours -----
    nsw = notifications.get("newScansWithinHours") or {}
    if nsw.get("enabled"):
        hours = int(nsw.get("hours") or 6)
        last_sent = _ts_to_datetime(nsw.get("lastSentAt"))
        should = _should_send_new_scans(nsw, now_utc, last_sent, hours)
        logger.info(
            "_process_customer: newScansWithinHours enabled=true hours=%s last_sent=%s should_send=%s",
            hours, last_sent, should,
        )
        if should:
            hits = _query_new_scans_within_hours(db, customer_id, hours)
            logger.info("_process_customer: newScansWithinHours hits_count=%d", len(hits))
            if hits:
                body = _render_new_scans(hits, hours, display_name, now_local)
                if _send_email(
                    email,
                    f"New scans in the last {hours} hours ({len(hits)} scans)",
                    body,
                ):
                    updates["notifications.newScansWithinHours.lastSentAt"] = firestore.SERVER_TIMESTAMP
    else:
        logger.debug("_process_customer: newScansWithinHours enabled=%s", bool(nsw.get("enabled")))

    # ----- unscannedToday (scans not contacted) -----
    ust = notifications.get("unscannedToday") or {}
    if ust.get("enabled"):
        last_sent = _ts_to_datetime(ust.get("lastSentAt"))
        should = _should_send_unscanned_today(ust, now_local, last_sent)
        logger.info(
            "_process_customer: unscannedToday enabled=true last_sent=%s should_send=%s",
            last_sent, should,
        )
        if should:
            businesses = _query_uncontacted_businesses(db, customer_id)
            logger.info("_process_customer: unscannedToday businesses_count=%d", len(businesses))
            if businesses:
                body = _render_unscanned_today(businesses, display_name, now_local)
                if _send_email(
                    email,
                    f"Scans not yet contacted ({len(businesses)} businesses)",
                    body,
                ):
                    updates["notifications.unscannedToday.lastSentAt"] = firestore.SERVER_TIMESTAMP
    else:
        logger.debug("_process_customer: unscannedToday enabled=%s", bool(ust.get("enabled")))

    # ----- weeklyDigest -----
    wd = notifications.get("weeklyDigest") or {}
    if wd.get("enabled"):
        last_sent = _ts_to_datetime(wd.get("lastSentAt"))
        should = _should_send_weekly_digest(wd, now_local, last_sent)
        logger.info(
            "_process_customer: weeklyDigest enabled=true dayOfWeek=%s last_sent=%s should_send=%s",
            wd.get("dayOfWeek"), last_sent, should,
        )
        if should:
            digest_data = _query_weekly_digest(db, customer_id, 7)
            logger.info("_process_customer: weeklyDigest total_scans=%d", digest_data.get("total_scans", 0))
            body = _render_weekly_digest(digest_data, display_name, now_local)
            if _send_email(
                email,
                "Your weekly digest",
                body,
            ):
                updates["notifications.weeklyDigest.lastSentAt"] = firestore.SERVER_TIMESTAMP
    else:
        logger.debug("_process_customer: weeklyDigest enabled=%s", bool(wd.get("enabled")))

    if updates:
        logger.info("_process_customer: updating customer_id=%s keys=%s", customer_id, list(updates.keys()))
        customer_ref.update(updates)
    else:
        logger.debug("_process_customer: no updates for customer_id=%s", customer_id)


def _process_customer_test(
    db: firestore.Client,
    customer_id: str,
    data: Dict[str, Any],
    test_type: str,
) -> Dict[str, int]:
    """
    Test mode: send selected email type(s) for one customer without schedule checks
    and without updating lastSentAt. Returns dict with sent counts: newScans, unscannedToday, weeklyDigest.
    """
    logger.info(
        "_process_customer_test: customer_id=%s test_type=%s",
        customer_id, test_type,
    )
    email = (data.get("email") or "").strip()
    if not email:
        logger.warning("_process_customer_test: skip customer_id=%s (no email)", customer_id)
        return {}
    if data.get("is_active") is False:
        logger.warning("_process_customer_test: skip customer_id=%s (is_active=false)", customer_id)
        return {}
    settings = data.get("settings") or {}
    tz = settings.get("timezone") or "Europe/Berlin"
    notifications = data.get("notifications") or {}
    now_utc, now_local = _customer_now(tz)
    display_name = (data.get("display_name") or data.get("email") or "").strip() or "du"

    want_all = test_type == "all"
    want_new_scans = want_all or test_type == "newscans"
    want_unscanned = want_all or test_type == "unscannedtoday"
    want_weekly = want_all or test_type == "weeklydigest"
    logger.info(
        "_process_customer_test: want_new_scans=%s want_unscanned=%s want_weekly=%s",
        want_new_scans, want_unscanned, want_weekly,
    )

    sent: Dict[str, int] = {"newScans": 0, "unscannedToday": 0, "weeklyDigest": 0}

    if want_new_scans:
        nsw = notifications.get("newScansWithinHours") or {}
        hours = int(nsw.get("hours") or 6)
        logger.info("_process_customer_test: sending newScans hours=%s", hours)
        hits = _query_new_scans_within_hours(db, customer_id, hours)
        body = _render_new_scans(hits, hours, display_name, now_local)
        if _send_email(
            email,
            f"[Test] New scans in the last {hours} hours ({len(hits)} scans)",
            body,
        ):
            sent["newScans"] = 1

    if want_unscanned:
        logger.info("_process_customer_test: sending unscannedToday")
        businesses = _query_uncontacted_businesses(db, customer_id)
        body = _render_unscanned_today(businesses, display_name, now_local)
        if _send_email(
            email,
            f"[Test] Scans not yet contacted ({len(businesses)} businesses)",
            body,
        ):
            sent["unscannedToday"] = 1

    if want_weekly:
        logger.info("_process_customer_test: sending weeklyDigest")
        digest_data = _query_weekly_digest(db, customer_id, 7)
        body = _render_weekly_digest(digest_data, display_name, now_local)
        if _send_email(email, "[Test] Your weekly digest", body):
            sent["weeklyDigest"] = 1

    logger.info("_process_customer_test: done customer_id=%s sent=%s", customer_id, sent)
    return sent


@functions_framework.http
def run_reminders(request: Request):
    """HTTP entry point: validate caller, stream customers, evaluate and send reminders."""
    if request.method == "OPTIONS":
        return ("", 204, {"Access-Control-Allow-Methods": "GET, OPTIONS"})

    if not _authenticate_request(request):
        return ("Unauthorized", 401, {"Content-Type": "text/plain"})

    test_email = (request.args.get("test_email") or "").strip()
    test_type = (request.args.get("test_type") or "all").strip().lower()

    db = _get_db()

    if test_email:
        # Test mode: query only the customer doc where email == test_email (single read)
        logger.info(
            "run_reminders: test_mode=true test_email=%s test_type=%s querying customers where email==test_email (single doc)",
            test_email, test_type,
        )
        query = db.collection("customers").where("email", "==", test_email).limit(1)
        docs = list(query.stream())
        if not docs:
            logger.warning("run_reminders: test_mode no customer found for test_email=%s", test_email)
            return (
                f"No customer found for test_email={test_email}",
                200,
                {"Content-Type": "text/plain"},
            )
        doc = docs[0]
        data = doc.to_dict() or {}
        logger.info("run_reminders: test_mode found customer_id=%s (single doc query)", doc.id)
        sent: Dict[str, int] = {"newScans": 0, "unscannedToday": 0, "weeklyDigest": 0}
        try:
            result = _process_customer_test(db, doc.id, data, test_type)
            for k in sent:
                sent[k] = result.get(k, 0)
        except Exception as e:
            logger.exception("Error in test mode for customer %s: %s", doc.id, e)
        parts = [f"{k}={v}" for k, v in sent.items() if v]
        summary = "OK; test sent: " + ", ".join(parts) if parts else "OK; test sent: (none)"
        logger.info("run_reminders: test_mode finished summary=%s", summary)
        return (summary, 200, {"Content-Type": "text/plain"})

    logger.info("run_reminders: normal mode streaming all customers")
    count = 0
    for doc in db.collection("customers").stream():
        count += 1
        try:
            _process_customer(db, doc.id, doc.to_dict() or {})
        except Exception as e:
            logger.exception("Error processing customer %s: %s", doc.id, e)
    logger.info("run_reminders: normal mode finished processed %d customers", count)
    return ("OK", 200, {"Content-Type": "text/plain"})
