"""
Send personalized PDF letters via onlinebrief24.de API.

Workflow:
- Read contacts from a CSV file (one row per letter).
- For each contact:
  - Select the correct PDF template based on the `Template` field.
  - Generate a QR code from `QR Code URL`.
  - Place QR code and tracking URL text on configured positions in the PDF.
  - Upload the resulting PDF to https://api.onlinebrief24.de/v1/printjobs.
  - Ensure idempotency via a local sent-log and a deterministic job_key.

This script is designed to handle large batches (e.g. 30,000+ letters)
while avoiding duplicate sends.

Usage (example):

1. Prepare a CSV with at least these columns (names are case-insensitive; common
   lettershop exports such as `Entscheider 1 Vorname`, `PLZ`, `Ort`, and
   `Namenszeile` / `Namenszeile 1` are accepted — see HEADER_TO_CANONICAL):
   `QR Code URL`, `Tracking Code URL`, `Template`, `Vorname`, `Nachname`,
   `Unternehmen`, `Straße`, `Hausnummer`, `Postleitzahl`, `Stadt`.

2. Create/adjust a config under `scripts/send_letter/configs/` (e.g. one per customer/sending) so that each
   `Template` value in the CSV maps to a PDF filename and coordinate positions.

3. Set the required environment variables (test mode by default):

   export ONLINEBRIEF24_API_KEY=...
   export ONLINEBRIEF24_API_SECRET=...
   export ONLINEBRIEF24_MODE=test

4. Run the script, e.g.:

   python scripts/send_letter/send_letters_onlinebrief24.py \\
       path/to/contacts.csv \\
       --templates-dir path/to/templates \\
       --config scripts/send_letter/configs/letter_templates_config.json \\
       --campaign-id 2026-03-mailing \\
       --save-pdfs-dir ./debug_pdfs
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    # PDF and QR code libraries (install if missing):
    #   pip install qrcode[pil] pypdf reportlab
    import qrcode
    from pypdf import PdfReader, PdfWriter
    from reportlab.lib.colors import CMYKColor
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfgen import canvas
    from PIL import Image
except ImportError as e:  # pragma: no cover - import guard
    missing = str(e)
    print(
        "❌ Required libraries not installed. Please run:\n"
        "    pip install qrcode[pil] pypdf reportlab\n"
        f"Import error was: {missing}",
        file=sys.stderr,
    )
    raise

# Try to load ONLINEBRIEF24_* and other settings from a project .env file
try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()
except ImportError:
    # If python-dotenv is not installed, we just rely on the process env
    pass


# 13-digit parameter string for onlinebrief24 filename (SFTP/API).
# See: https://www.onlinebrief24.de/storage/downloads/onlinebrief24.de_sftp_schnittstelle.pdf
# Pos 1: Druck 1=farbig, 2: Modus 1=duplex, 3: Kuvert 0=DIN lang, 4: Versandzone 1=national,
# 5: Einschreiben 0, 6: Zahlschein 0, 7-13: Reserve 0.
ONLINEBRIEF24_FILENAME_PREFIX = "1100000000000"

# Kostenstelle (cost center) in filename: max 18 chars, wrapped in #...# per doc.
KOSTENSTELLE_MAX_LEN = 18


def _onlinebrief24_filename(
    campaign_id: Optional[str],
    contact_index: int,
    job_key: str,
) -> str:
    """
    Build filename for onlinebrief24 in their required format:
    13-digit-prefix + '-' + uniquefilename + '#' + kostenstelle + '#.pdf'
    Kostenstelle = campaign_id (max 18 chars) for cost allocation in Abrechnung.
    """
    kostenstelle = (campaign_id or "letter")[:KOSTENSTELLE_MAX_LEN]
    unique_part = f"letter_{contact_index:06d}_{job_key[:8]}"
    return f"{ONLINEBRIEF24_FILENAME_PREFIX}-{unique_part}#{kostenstelle}#.pdf"


REQUIRED_CONTACT_FIELDS = [
    "QR Code URL",
    "Tracking Code URL",
    "Template",
    "Vorname",
    "Nachname",
    "Unternehmen",
    "Straße",
    "Hausnummer",
    "Postleitzahl",
    "Stadt",
]

# Case-insensitive header aliases → canonical field names (used for CSV parsing).
# Handles comma- or tab-separated files and variants like "QR-link", "Unternehmensname", "Temlpate".
HEADER_TO_CANONICAL: Dict[str, str] = {
    "qr code url": "QR Code URL",
    "qr-link": "QR Code URL",
    "qr link": "QR Code URL",
    "tracking_link": "QR Code URL",
    "tracking code url": "Tracking Code URL",
    "tracking-link": "Tracking Code URL",
    "tracking link": "Tracking Code URL",
    "tracking_url": "Tracking Code URL",
    "domain_tracking_url": "Tracking Code URL",
    "template": "Template",
    "temlpate": "Template",  # common typo
    "unternehmen": "Unternehmen",
    "unternehmensname": "Unternehmen",
    "company_name": "Unternehmen",
    "vorname": "Vorname",
    "first_name": "Vorname",
    "nachname": "Nachname",
    "last_name": "Nachname",
    "straße": "Straße",
    "strasse": "Straße",
    "street": "Straße",
    "anrede": "Anrede",
    "salutation": "Anrede",
    "hausnummer": "Hausnummer",
    "house_number": "Hausnummer",
    "postleitzahl": "Postleitzahl",
    "postcode": "Postleitzahl",
    "plz": "Postleitzahl",
    "zip": "Postleitzahl",
    "zip code": "Postleitzahl",
    "stadt": "Stadt",
    "city": "Stadt",
    "ort": "Stadt",
    "ortschaft": "Stadt",
    # German B2B / lettershop list columns (e.g. Ocean.io style)
    "entscheider 1 vorname": "Vorname",
    "entscheider 1 nachname": "Nachname",
    "entscheider 1 anrede": "Anrede",
    # Single line — Namenszeile 2/3 stay as separate keys until fill helper runs
    "namenszeile": "Unternehmen",
}


@dataclass
class TemplatePositionConfig:
    pdf_filename: str
    qr_positions: List[Dict[str, Any]]
    tracking_text_positions: List[Dict[str, Any]]
    date_positions: List[Dict[str, Any]]
    full_name_positions: List[Dict[str, Any]]
    street_positions: List[Dict[str, Any]]
    city_positions: List[Dict[str, Any]]
    business_name_positions: List[Dict[str, Any]]
    salutation_positions: List[Dict[str, Any]]


class SentLog:
    """
    Thread-safe local log of sent / skipped jobs to ensure idempotency.
    Backed by a CSV file with at least a 'job_key' column.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._seen_keys: set[str] = set()
        self._initialised = False
        self._init_from_disk()

    def _init_from_disk(self) -> None:
        if not self.path.exists():
            return
        with self._lock:
            try:
                with self.path.open("r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        job_key = row.get("job_key")
                        if job_key:
                            self._seen_keys.add(job_key)
                self._initialised = True
            except Exception as e:  # pragma: no cover - defensive
                print(f"⚠️ Could not read existing sent-log '{self.path}': {e}")

    def has(self, job_key: str) -> bool:
        with self._lock:
            return job_key in self._seen_keys

    def record(
        self,
        job_key: str,
        status: str,
        contact_index: int,
        api_job_id: Optional[int] = None,
        tracking_code: Optional[str] = None,
        notice: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Append a record to the sent-log CSV in a thread-safe way.
        """
        timestamp = datetime.utcnow().isoformat()
        with self._lock:
            file_exists = self.path.exists()
            try:
                with self.path.open("a", newline="", encoding="utf-8") as f:
                    fieldnames = [
                        "timestamp",
                        "contact_index",
                        "job_key",
                        "status",
                        "api_job_id",
                        "tracking_code",
                        "notice",
                        "error_message",
                    ]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(
                        {
                            "timestamp": timestamp,
                            "contact_index": contact_index,
                            "job_key": job_key,
                            "status": status,
                            "api_job_id": api_job_id or "",
                            "tracking_code": tracking_code or "",
                            "notice": notice or "",
                            "error_message": error_message or "",
                        }
                    )
                self._seen_keys.add(job_key)
            except Exception as e:  # pragma: no cover - defensive
                print(f"⚠️ Could not write to sent-log '{self.path}': {e}")


class RateLimiter:
    """
    Simple thread-safe rate limiter: max_calls per period_seconds.
    Used to respect the onlinebrief24 API rate limit.
    """

    def __init__(self, max_calls: int, period_seconds: int) -> None:
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self._lock = threading.Lock()
        self._timestamps: List[float] = []

    def acquire(self) -> None:
        with self._lock:
            now = time.time()
            # Drop timestamps older than the period
            cutoff = now - self.period_seconds
            self._timestamps = [t for t in self._timestamps if t > cutoff]

            if len(self._timestamps) >= self.max_calls:
                sleep_for = self._timestamps[0] + self.period_seconds - now
                if sleep_for > 0:
                    time.sleep(sleep_for)
                now = time.time()
                cutoff = now - self.period_seconds
                self._timestamps = [t for t in self._timestamps if t > cutoff]

            self._timestamps.append(time.time())


class OnlineBrief24Client:
    """
    Minimal client for the onlinebrief24.de API (v1).
    Only the /v1/printjobs POST endpoint is used here.
    """

    BASE_URL = "https://api.onlinebrief24.de/v1"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        mode: str,
        rate_limiter: Optional[RateLimiter] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.mode = mode
        self.rate_limiter = rate_limiter
        self.session = session or requests.Session()

    def _auth_object(self, override_mode: Optional[str] = None) -> Dict[str, Any]:
        return {
            "apiKey": self.api_key,
            "apiSecret": self.api_secret,
            "mode": override_mode or self.mode,
        }

    def upload_letter(
        self,
        pdf_bytes: bytes,
        filename_original: Optional[str],
        notice: Optional[str],
        cost_unit: Optional[str],
        specification: Optional[Dict[str, Any]] = None,
        mode_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Upload a single PDF as a printjob.
        Returns parsed JSON response.
        """
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()

        base64_file = base64.b64encode(pdf_bytes).decode("ascii")
        checksum = hashlib.md5(base64_file.encode("utf-8")).hexdigest()

        letter_spec = specification or {
            "color": "4",  # color
            "mode": "duplex",
            "shipping": "national",
        }

        payload: Dict[str, Any] = {
            "auth": self._auth_object(override_mode=mode_override),
            "letter": {
                "base64_file": base64_file,
                "base64_file_checksum": checksum,
                "specification": letter_spec,
            },
        }

        if filename_original:
            payload["letter"]["filename_original"] = filename_original
        if notice:
            payload["letter"]["notice"] = notice
        if cost_unit:
            payload["letter"]["cost_unit"] = cost_unit

        url = f"{self.BASE_URL}/printjobs"
        resp = self.session.post(
            url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"onlinebrief24 API returned status {resp.status_code}: {resp.text}"
            )
        data = resp.json()
        if data.get("status") != 200:
            raise RuntimeError(f"onlinebrief24 API error: {data}")
        return data


def _normalize_header(header: str) -> str:
    """Return canonical field name for a CSV header (case-insensitive, strip)."""
    raw = (header or "").strip()
    # Strip BOM so "\ufeffcompany_name" still maps to Unternehmen
    if raw.startswith("\ufeff"):
        raw = raw[1:]
    key = raw.lower()
    return HEADER_TO_CANONICAL.get(key, raw)


def _lettershop_fill_missing_company(contact: Dict[str, str]) -> None:
    """
    If Unternehmen is empty, use the first non-empty Namenszeile / Namenszeile N
    column (common in German address exports). Keys are matched case-insensitively.
    """
    if (contact.get("Unternehmen") or "").strip():
        return
    lower_to_actual = {(k or "").strip().lower(): k for k in contact}
    for want in ("namenszeile", "namenszeile 1", "namenszeile 2", "namenszeile 3"):
        actual = lower_to_actual.get(want)
        if not actual:
            continue
        v = (contact.get(actual) or "").strip()
        if v:
            contact["Unternehmen"] = v
            return


def _detect_delimiter(sample: str) -> str:
    """
    Detect CSV delimiter from a sample (comma, tab, or semicolon).
    Uses the delimiter that yields the most columns on the first line, so
    semicolon-separated files (e.g. German Excel export) are handled correctly.
    """
    first_line = sample.split("\n")[0]
    if not first_line:
        return ","
    best_delimiter = ","
    best_count = 1
    for delim in (";", "\t", ","):
        count = len(first_line.split(delim))
        if count > best_count:
            best_count = count
            best_delimiter = delim
    return best_delimiter


def load_contacts_csv(path: Path) -> Tuple[List[Dict[str, str]], str]:
    """
    Load contacts from a CSV/TSV file. Headers are case-insensitive and
    mapped to canonical names (e.g. QR-link → QR Code URL, Unternehmensname
    → Unternehmen). Supports comma-, tab-, or semicolon-separated. Returns
    (list of contact dicts with canonical keys, delimiter used).
    """
    # utf-8-sig strips BOM if present (Excel sometimes exports with BOM)
    text = path.read_text(encoding="utf-8-sig")
    if not text.strip():
        return [], ","

    # Detect delimiter from first line(s)
    sample = text[:8192] if len(text) > 8192 else text
    delimiter = _detect_delimiter(sample)

    from io import StringIO

    reader = csv.reader(StringIO(text), delimiter=delimiter)
    raw_headers = next(reader)

    # Decide how to map tracking-related headers:
    # - If a 'domain_tracking_url' column is present, use it for 'Tracking Code URL'
    #   and keep any 'tracking_url' column separate.
    # - If no 'domain_tracking_url' is present, treat 'tracking_url' as 'Tracking Code URL'.
    raw_lower = [((h or "").strip().lower()) for h in raw_headers]
    has_domain_tracking = any(h == "domain_tracking_url" for h in raw_lower)

    canonical_headers: List[str] = []
    for h, h_lower in zip(raw_headers, raw_lower):
        if h_lower == "tracking_url" and has_domain_tracking:
            # Keep as-is so it does not override 'domain_tracking_url' mapping
            canonical_headers.append(h)
        else:
            canonical_headers.append(_normalize_header(h))

    contacts: List[Dict[str, str]] = []
    for row in reader:
        contact: Dict[str, str] = {}
        for i, value in enumerate(row):
            if i < len(canonical_headers):
                key = canonical_headers[i]
                contact[key] = (value.strip() if value else "")
        _lettershop_fill_missing_company(contact)
        contacts.append(contact)

    return contacts, delimiter


def load_template_config(path: Path) -> Dict[str, TemplatePositionConfig]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    config: Dict[str, TemplatePositionConfig] = {}
    for key, value in raw.items():
        if key.startswith("_"):
            continue
        pdf_filename = value.get("pdf_filename")
        if not pdf_filename:
            raise ValueError(f"Template '{key}' is missing 'pdf_filename'")
        qr_positions = value.get("qr_positions") or []
        tracking_text_positions = value.get("tracking_text_positions") or []
        config[key] = TemplatePositionConfig(
            pdf_filename=pdf_filename,
            qr_positions=qr_positions,
            tracking_text_positions=tracking_text_positions,
            date_positions=value.get("date_positions") or [],
            full_name_positions=value.get("full_name_positions") or [],
            street_positions=value.get("street_positions") or [],
            city_positions=value.get("city_positions") or [],
            business_name_positions=value.get("business_name_positions") or [],
            salutation_positions=value.get("salutation_positions") or [],
        )
    return config


def generate_qr_image(qr_url: str) -> Image.Image:
    """Generate a QR code image in CMYK for print-ready PDFs."""
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )
    qr.add_data(qr_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    if not isinstance(img, Image.Image):
        img = img.convert("RGB")
    # Convert to CMYK for commercial printing (black = 0,0,0,100% K)
    img = img.convert("CMYK")
    return img


# CMYK solid black for print (0, 0, 0, 100% K) – no CMY to avoid registration issues
_PRINT_BLACK_CMYK = CMYKColor(0, 0, 0, 1)


def compose_letter_pdf(
    template_path: Path,
    template_cfg: TemplatePositionConfig,
    qr_url: str,
    tracking_url: str,
    date_text: str,
    salutation: str,
    full_name: str,
    street_line: str,
    city_line: str,
    business_name: str,
) -> bytes:
    """
    Load template PDF and overlay QR code + tracking URL text
    according to the template configuration. Overlay is drawn in CMYK
    and optimized for commercial printing.
    """
    reader = PdfReader(str(template_path))
    writer = PdfWriter()

    # Mark output as print-optimized (CMYK)
    writer.add_metadata({
        "/Producer": "send_letters_onlinebrief24 (CMYK print)",
        "/Creator": "send_letters_onlinebrief24",
    })

    qr_img = generate_qr_image(qr_url)
    qr_reader = ImageReader(qr_img)

    # Group overlay positions per page index
    qr_by_page: Dict[int, List[Dict[str, Any]]] = {}
    txt_by_page: Dict[int, List[Dict[str, Any]]] = {}
    date_by_page: Dict[int, List[Dict[str, Any]]] = {}
    full_name_by_page: Dict[int, List[Dict[str, Any]]] = {}
    street_by_page: Dict[int, List[Dict[str, Any]]] = {}
    city_by_page: Dict[int, List[Dict[str, Any]]] = {}
    business_by_page: Dict[int, List[Dict[str, Any]]] = {}
    salutation_by_page: Dict[int, List[Dict[str, Any]]] = {}
    for cfg in template_cfg.qr_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        qr_by_page.setdefault(page_idx, []).append(cfg)
    for cfg in template_cfg.tracking_text_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        txt_by_page.setdefault(page_idx, []).append(cfg)
    for cfg in template_cfg.date_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        date_by_page.setdefault(page_idx, []).append(cfg)
    for cfg in template_cfg.full_name_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        full_name_by_page.setdefault(page_idx, []).append(cfg)
    for cfg in template_cfg.street_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        street_by_page.setdefault(page_idx, []).append(cfg)
    for cfg in template_cfg.city_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        city_by_page.setdefault(page_idx, []).append(cfg)
    for cfg in template_cfg.business_name_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        business_by_page.setdefault(page_idx, []).append(cfg)
    for cfg in template_cfg.salutation_positions:
        page_idx = int(cfg.get("page", 1)) - 1
        salutation_by_page.setdefault(page_idx, []).append(cfg)

    for i, page in enumerate(reader.pages):
        page_obj = page
        width = float(page_obj.mediabox.width)
        height = float(page_obj.mediabox.height)

        needs_overlay = (
            i in qr_by_page
            or i in txt_by_page
            or i in date_by_page
            or i in full_name_by_page
            or i in street_by_page
            or i in city_by_page
            or i in business_by_page
            or i in salutation_by_page
        )
        if needs_overlay:
            from io import BytesIO

            buffer = BytesIO()
            c = canvas.Canvas(buffer, pagesize=(width, height))

            for cfg in qr_by_page.get(i, []):
                x = float(cfg.get("x", 0))
                y = float(cfg.get("y", 0))
                size = float(cfg.get("size", 100))
                c.drawImage(
                    qr_reader,
                    x,
                    y,
                    width=size,
                    height=size,
                    preserveAspectRatio=True,
                    mask="auto",
                )

            # All text in CMYK black for print
            c.setFillColor(_PRINT_BLACK_CMYK)

            def _draw_text(text: str, cfg: Dict[str, Any], y_offset: float = 0.0) -> None:
                x = float(cfg.get("x", 0))
                base_y = float(cfg.get("y", 0))
                y = base_y - y_offset
                font_size = float(cfg.get("font_size", 10))
                align = str(cfg.get("align", "left")).lower()
                c.setFont("Helvetica", font_size)
                if align in {"center", "right"}:
                    text_width = c.stringWidth(text, "Helvetica", font_size)
                    if align == "center":
                        x_draw = x - text_width / 2.0
                    else:  # right
                        x_draw = x - text_width
                else:
                    x_draw = x
                c.drawString(x_draw, y, text)

            def _split_business_name(
                text: str,
                font_size: float,
                max_width: float,
            ) -> Tuple[List[str], float]:
                """
                Wrap a business name into as many lines as needed so that no
                rendered line exceeds max_width. Returns (lines, line_spacing).
                """
                if not text or max_width <= 0:
                    return [text], 0.0

                def _width(s: str) -> float:
                    return c.stringWidth(s, "Helvetica", font_size)

                if _width(text) <= max_width:
                    return [text], 0.0

                words = text.split()
                if not words:
                    return [text], 0.0

                def _split_long_token(token: str) -> List[str]:
                    parts: List[str] = []
                    remaining = token
                    while remaining:
                        if _width(remaining) <= max_width:
                            parts.append(remaining)
                            break
                        split_at = 1
                        for idx in range(1, len(remaining) + 1):
                            candidate = remaining[:idx]
                            if _width(candidate) <= max_width:
                                split_at = idx
                            else:
                                break
                        parts.append(remaining[:split_at])
                        remaining = remaining[split_at:]
                    return parts

                lines: List[str] = []
                current_line = ""
                for word in words:
                    candidate = word if not current_line else f"{current_line} {word}"
                    if _width(candidate) <= max_width:
                        current_line = candidate
                        continue

                    if current_line:
                        lines.append(current_line)
                        current_line = ""

                    if _width(word) <= max_width:
                        current_line = word
                        continue

                    token_parts = _split_long_token(word)
                    lines.extend(token_parts[:-1])
                    current_line = token_parts[-1]

                if current_line:
                    lines.append(current_line)

                line_spacing = font_size + 2.0
                return lines or [text], line_spacing

            # Static tracking text (typically the URL)
            for cfg in txt_by_page.get(i, []):
                _draw_text(tracking_url, cfg)

            # Dynamic text fields above the address block
            for cfg in date_by_page.get(i, []):
                _draw_text(date_text, cfg)

            for cfg in salutation_by_page.get(i, []):
                _draw_text(salutation, cfg)

            # Business name may wrap to multiple lines; track how much we shift
            # the following address lines down.
            wrap_offset = 0.0
            for cfg in business_by_page.get(i, []):
                font_size = float(cfg.get("font_size", 10))
                configured_max_width = float(cfg.get("max_width", 0) or 0.0)
                max_width = width * 0.5
                if configured_max_width > 0:
                    max_width = min(configured_max_width, max_width)

                if max_width > 0:
                    lines, line_spacing = _split_business_name(
                        business_name,
                        font_size,
                        max_width,
                    )
                else:
                    lines, line_spacing = [business_name], 0.0

                for line_index, line in enumerate(lines):
                    cfg_line = dict(cfg)
                    cfg_line["y"] = float(cfg.get("y", 0)) - (line_spacing * line_index)
                    _draw_text(line, cfg_line)

                wrap_offset = max(wrap_offset, line_spacing * max(0, len(lines) - 1))

            # Address lines (name, street, city) move down if business name wrapped
            for cfg in full_name_by_page.get(i, []):
                _draw_text(full_name, cfg, wrap_offset)

            for cfg in street_by_page.get(i, []):
                _draw_text(street_line, cfg, wrap_offset)

            for cfg in city_by_page.get(i, []):
                _draw_text(city_line, cfg, wrap_offset)

            c.showPage()
            c.save()
            buffer.seek(0)

            overlay_reader = PdfReader(buffer)
            overlay_page = overlay_reader.pages[0]
            page_obj.merge_page(overlay_page)

        writer.add_page(page_obj)

    from io import BytesIO

    out_buf = BytesIO()
    writer.write(out_buf)
    return out_buf.getvalue()


_CMYK_CHECK_CACHE: Dict[Path, bool] = {}


def ensure_template_cmyk(template_path: Path) -> None:
    """
    Best-effort check that the base template PDF does not use obvious RGB
    colorspaces. If suspicious RGB markers are found, raise an error so the
    run can be aborted and the template can be re-exported as CMYK.
    """
    if template_path in _CMYK_CHECK_CACHE:
        return

    try:
        data = template_path.read_bytes()
    except Exception as e:
        raise ValueError(f"Could not read template PDF '{template_path}': {e}")

    # Helpful debug output: show template page size so positions in the
    # config can be chosen more easily.
    try:
        reader = PdfReader(str(template_path))
        if reader.pages:
            page0 = reader.pages[0]
            width = float(page0.mediabox.width)
            height = float(page0.mediabox.height)
            print(
                f"Template '{template_path.name}' page 1 size: "
                f"{width:.1f} pt x {height:.1f} pt"
            )
    except Exception:
        # Size info is for convenience only; ignore errors here.
        pass

    # Simple heuristic: abort if common RGB colorspace names are present.
    # This won't catch every case, but it is conservative enough to avoid
    # obviously non-CMYK templates.
    suspicious_tokens = [b"DeviceRGB", b"CalRGB"]
    for token in suspicious_tokens:
        if token in data:
            raise ValueError(
                f"Template PDF '{template_path}' appears to use RGB colorspace "
                f"(found token '{token.decode('ascii')}'). Please export this "
                "template as a CMYK-only PDF before running the script."
            )

    _CMYK_CHECK_CACHE[template_path] = True


def build_job_key(
    contact: Dict[str, str],
    campaign_id: Optional[str],
) -> str:
    relevant = {
        "Template": contact.get("Template", ""),
        "QR Code URL": contact.get("QR Code URL", ""),
        "Tracking Code URL": contact.get("Tracking Code URL", ""),
        "Vorname": contact.get("Vorname", ""),
        "Nachname": contact.get("Nachname", ""),
        "Unternehmen": contact.get("Unternehmen", ""),
        "Straße": contact.get("Straße", ""),
        "Hausnummer": contact.get("Hausnummer", ""),
        "Postleitzahl": contact.get("Postleitzahl", ""),
        "Stadt": contact.get("Stadt", ""),
        "campaign_id": campaign_id or "",
    }
    payload = json.dumps(relevant, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_contact(contact: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    for field in REQUIRED_CONTACT_FIELDS:
        value = (contact.get(field) or "").strip()
        if not value:
            return False, f"Missing required field '{field}'"
    return True, None


def process_contact(
    contact_index: int,
    contact: Dict[str, str],
    templates_dir: Path,
    templates_cfg: Dict[str, TemplatePositionConfig],
    sent_log: SentLog,
    api_client: Optional[OnlineBrief24Client],
    campaign_id: Optional[str],
    save_pdfs_dir: Optional[Path],
    upload_enabled: bool,
    mode_override: Optional[str],
    idempotency_enabled: bool,
) -> Tuple[str, Optional[int], Optional[str], Optional[str]]:
    """
    Process a single contact:
    - validate
    - check idempotency
    - compose PDF
    - upload via API
    Returns (status, api_job_id, job_key, error_message).
    """
    job_key = build_job_key(contact, campaign_id)

    if idempotency_enabled and sent_log.has(job_key):
        return "already_logged", None, job_key, None

    is_valid, error = validate_contact(contact)
    if not is_valid:
        sent_log.record(
            job_key=job_key,
            status="invalid_contact",
            contact_index=contact_index,
            error_message=error,
        )
        return "invalid_contact", None, job_key, error

    template_key = contact.get("Template", "").strip()
    if template_key not in templates_cfg:
        msg = f"Template key '{template_key}' not found in config"
        sent_log.record(
            job_key=job_key,
            status="template_missing",
            contact_index=contact_index,
            error_message=msg,
        )
        return "template_missing", None, job_key, msg

    cfg = templates_cfg[template_key]
    template_path = templates_dir / cfg.pdf_filename
    if not template_path.exists():
        msg = f"Template PDF not found: {template_path}"
        sent_log.record(
            job_key=job_key,
            status="template_pdf_missing",
            contact_index=contact_index,
            error_message=msg,
        )
        return "template_pdf_missing", None, job_key, msg

    qr_url = contact.get("QR Code URL", "").strip()
    tracking_url = contact.get("Tracking Code URL", "").strip()

    # Dynamic text fields
    today = datetime.today().strftime("%d.%m.%Y")
    full_name_parts = [
        (contact.get("Vorname") or "").strip(),
        (contact.get("Nachname") or "").strip(),
    ]
    full_name = " ".join(p for p in full_name_parts if p)
    street_line = " ".join(
        p
        for p in [
            (contact.get("Straße") or "").strip(),
            (contact.get("Hausnummer") or "").strip(),
        ]
        if p
    )
    city_line = " ".join(
        p
        for p in [
            (contact.get("Postleitzahl") or "").strip(),
            (contact.get("Stadt") or "").strip(),
        ]
        if p
    )
    business_name = (contact.get("Unternehmen") or "").strip()

    # Build salutation / Anrede
    # Header normalization already maps variants like "salutation" → "Anrede"
    # and "last_name" → "Nachname", so we only read the canonical keys here.
    raw_anrede = (contact.get("Anrede") or "").strip()
    last_name = (contact.get("Nachname") or "").strip()
    salutation = ""
    if last_name:
        lower_anrede = raw_anrede.lower()
        if "herr" in lower_anrede and "frau" not in lower_anrede:
            salutation = f"geehrter Herr {last_name},"
        elif "frau" in lower_anrede:
            salutation = f"geehrte Frau {last_name},"
    if not salutation:
        # Fallback: use provided Anrede as-is, or a generic greeting.
        if raw_anrede:
            salutation = raw_anrede
        elif full_name:
            salutation = f"Guten Tag {full_name},"

    try:
        pdf_bytes = compose_letter_pdf(
            template_path=template_path,
            template_cfg=cfg,
            qr_url=qr_url,
            tracking_url=tracking_url,
            date_text=today,
            salutation=salutation,
            full_name=full_name,
            street_line=street_line,
            city_line=city_line,
            business_name=business_name,
        )
    except Exception as e:
        msg = f"PDF composition failed: {e}"
        sent_log.record(
            job_key=job_key,
            status="pdf_error",
            contact_index=contact_index,
            error_message=msg,
        )
        return "pdf_error", None, job_key, msg

    # Build onlinebrief24 filename once (format: prefix-uniquefilename#kostenstelle#.pdf)
    filename_original = _onlinebrief24_filename(
        campaign_id=campaign_id,
        contact_index=contact_index,
        job_key=job_key,
    )

    if save_pdfs_dir is not None:
        try:
            save_pdfs_dir.mkdir(parents=True, exist_ok=True)
            (save_pdfs_dir / filename_original).write_bytes(pdf_bytes)
        except Exception as e:
            print(f"⚠️ Could not save debug PDF for index {contact_index}: {e}")

    if not upload_enabled:
        # In PDF-only mode we do not upload or record a successful entry
        # in the sent-log so that a later run with uploads enabled can
        # still send this job.
        return "pdf_generated", None, job_key, None

    notice_job_key = job_key[:16]
    campaign_part = f"campaign={campaign_id}" if campaign_id else "campaign=none"
    notice = f"{campaign_part};job={notice_job_key};index={contact_index}"
    cost_unit = campaign_id

    if api_client is None:
        msg = "API client is not configured"
        sent_log.record(
            job_key=job_key,
            status="api_error",
            contact_index=contact_index,
            error_message=msg,
        )
        return "api_error", None, job_key, msg

    try:
        response = api_client.upload_letter(
            pdf_bytes=pdf_bytes,
            filename_original=filename_original,
            notice=notice,
            cost_unit=cost_unit,
            specification=None,
            mode_override=mode_override,
        )
        data = response.get("data") or {}
        api_job_id = data.get("id")

        tracking_code: Optional[str] = None
        items = data.get("items") or []
        if items and isinstance(items, list):
            tracking_code = items[0].get("tracking_code")

        sent_log.record(
            job_key=job_key,
            status="sent",
            contact_index=contact_index,
            api_job_id=api_job_id,
            tracking_code=tracking_code,
            notice=notice,
        )
        return "sent", api_job_id, job_key, None
    except Exception as e:
        msg = f"API error: {e}"
        sent_log.record(
            job_key=job_key,
            status="api_error",
            contact_index=contact_index,
            error_message=msg,
        )
        return "api_error", None, job_key, msg


def run(
    contacts_csv: Path,
    templates_dir: Path,
    config_path: Path,
    sent_log_path: Path,
    campaign_id: Optional[str],
    mode_override: Optional[str],
    limit: Optional[int],
    start_index: int,
    max_count: Optional[int],
    save_pdfs_dir: Optional[Path],
    max_workers: int,
    max_calls_per_minute: int,
    idempotency_enabled: bool,
    upload_enabled: bool,
) -> int:
    if not contacts_csv.exists():
        print(f"❌ Contacts CSV not found: {contacts_csv}")
        return 1
    if not templates_dir.exists():
        print(f"❌ Templates directory not found: {templates_dir}")
        return 1
    if not config_path.exists():
        print(f"❌ Template config file not found: {config_path}")
        return 1

    print("=" * 80)
    print("SEND LETTERS VIA ONLINEBRIEF24.DE")
    print("=" * 80)
    print(f"Contacts CSV : {contacts_csv}")
    print(f"Templates dir: {templates_dir}")
    print(f"Config file  : {config_path}")
    print(f"Sent-log     : {sent_log_path}")
    print(f"Campaign ID  : {campaign_id or '(none)'}")
    print(f"Mode override: {mode_override or '(env default)'}")
    print(f"Max workers  : {max_workers}")
    print(f"Max calls/min: {max_calls_per_minute}")
    print(f"Upload       : {'enabled' if upload_enabled else 'disabled (PDF-only mode)'}")
    print("=" * 80)

    client: Optional[OnlineBrief24Client]
    if upload_enabled:
        api_key = os.environ.get("ONLINEBRIEF24_API_KEY")
        api_secret = os.environ.get("ONLINEBRIEF24_API_SECRET")
        default_mode = os.environ.get("ONLINEBRIEF24_MODE", "test")
        if not api_key or not api_secret:
            print(
                "❌ ONLINEBRIEF24_API_KEY and/or ONLINEBRIEF24_API_SECRET are not set "
                "in the environment."
            )
            return 1

        mode = mode_override or default_mode
        print(f"Using API mode: {mode}")

        rate_limiter = RateLimiter(
            max_calls=max_calls_per_minute,
            period_seconds=60,
        )
        client = OnlineBrief24Client(
            api_key=api_key,
            api_secret=api_secret,
            mode=mode,
            rate_limiter=rate_limiter,
        )
    else:
        client = None
        print(
            "Upload disabled: running in PDF-only mode; no Onlinebrief24 API calls will be made."
        )

    try:
        templates_cfg = load_template_config(config_path)
    except Exception as e:
        print(f"❌ Failed to load template config: {e}")
        return 1

    # Ensure that all referenced template PDFs appear to be CMYK-only.
    # If not, abort the run so no jobs are sent with an RGB base.
    try:
        checked_files: set[Path] = set()
        for key, cfg in templates_cfg.items():
            template_pdf_path = templates_dir / cfg.pdf_filename
            if template_pdf_path in checked_files:
                continue
            ensure_template_cmyk(template_pdf_path)
            checked_files.add(template_pdf_path)
    except Exception as e:
        print(f"❌ Template color space check failed: {e}")
        return 1

    sent_log = SentLog(sent_log_path)

    # Load contacts (case-insensitive headers, comma or tab separated)
    try:
        contacts, _delimiter = load_contacts_csv(contacts_csv)
    except Exception as e:
        print(f"❌ Failed to read contacts CSV: {e}")
        return 1

    if not contacts:
        print("⚠️ No contacts found in CSV.")
        return 0

    missing = [f for f in REQUIRED_CONTACT_FIELDS if f not in contacts[0]]
    if missing:
        found = sorted(contacts[0].keys())
        print(f"❌ CSV is missing required columns (after header mapping): {', '.join(missing)}")
        print(f"   Required: {', '.join(REQUIRED_CONTACT_FIELDS)}")
        print(f"   Found in first row: {', '.join(found)}")
        return 1

    total_contacts = len(contacts)
    if start_index < 0:
        start_index = 0
    if start_index >= total_contacts:
        print(f"⚠️ start_index {start_index} is beyond end of contacts ({total_contacts}).")
        return 0

    end_index = total_contacts
    if max_count is not None:
        end_index = min(total_contacts, start_index + max_count)
    if limit is not None:
        end_index = min(end_index, start_index + limit)

    selected_contacts = list(range(start_index, end_index))
    print(f"Processing contacts indices from {start_index} to {end_index - 1} (total {len(selected_contacts)})")

    if not upload_enabled and save_pdfs_dir is None:
        print(
            "⚠️ Upload is disabled and --save-pdfs-dir was not set; generated PDFs will not be saved."
        )

    from concurrent.futures import ThreadPoolExecutor, as_completed

    stats = {
        "sent": 0,
        "pdf_generated": 0,
        "already_logged": 0,
        "invalid_contact": 0,
        "template_missing": 0,
        "template_pdf_missing": 0,
        "pdf_error": 0,
        "api_error": 0,
        "other_error": 0,
    }

    def submit_contact(executor: ThreadPoolExecutor, idx: int) -> Any:
        contact = contacts[idx]
        return executor.submit(
            process_contact,
            idx,
            contact,
            templates_dir,
            templates_cfg,
            sent_log,
            client,
            campaign_id,
            save_pdfs_dir,
            upload_enabled,
            mode_override,
            idempotency_enabled,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {submit_contact(executor, idx): idx for idx in selected_contacts}

        for future in as_completed(futures):
            idx = futures[future]
            try:
                status, api_job_id, job_key, error_message = future.result()
            except Exception as e:
                stats["other_error"] += 1
                print(f"[{idx}] ❌ Unexpected error: {e}")
                continue

            stats.setdefault(status, 0)
            stats[status] += 1

            if status == "sent":
                print(f"[{idx}] ✅ Sent (job_id={api_job_id}, job_key={job_key[:8]}...)")
            elif status == "pdf_generated":
                print(f"[{idx}] ✅ PDF generated (job_key={job_key[:8]}...)")
            elif status == "already_logged":
                print(f"[{idx}] ⏭  Skipped (already in sent-log, job_key={job_key[:8]}...)")
            else:
                print(f"[{idx}] ⚠️ {status}: {error_message}")

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    for key, value in sorted(stats.items()):
        print(f"{key:22s}: {value}")
    print("=" * 80)

    return 0


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Send personalized PDF letters via onlinebrief24.de API.",
    )
    parser.add_argument(
        "contacts_csv",
        help="Path to input contacts CSV file.",
    )
    parser.add_argument(
        "--templates-dir",
        required=True,
        help="Directory containing PDF templates.",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to JSON config defining template positions.",
    )
    parser.add_argument(
        "--sent-log",
        default="sent_letters.csv",
        help="Path to local CSV sent-log file (default: sent_letters.csv).",
    )
    parser.add_argument(
        "--no-idempotency",
        action="store_true",
        help="Disable idempotency based on the sent-log; do not skip already-logged contacts.",
    )
    parser.add_argument(
        "--campaign-id",
        default=None,
        help="Optional campaign identifier used in job_key and API notice.",
    )
    parser.add_argument(
        "--mode",
        dest="mode_override",
        choices=["test", "live"],
        default=None,
        help="Override ONLINEBRIEF24_MODE for this run (test or live).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of contacts to process.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="Zero-based start index in the CSV (default: 0).",
    )
    parser.add_argument(
        "--max-count",
        type=int,
        default=None,
        help="Maximum number of contacts starting from start-index.",
    )
    parser.add_argument(
        "--save-pdfs-dir",
        type=str,
        default=None,
        help="Optional directory to store generated PDFs for debugging.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel workers (default: 4).",
    )
    parser.add_argument(
        "--max-calls-per-minute",
        type=int,
        default=100,
        help="Maximum API calls per minute to respect rate limits (default: 100).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help=(
            "Actually upload letters to onlinebrief24.de. "
            "If omitted, only generate PDFs (and save them if --save-pdfs-dir is set)."
        ),
    )

    args = parser.parse_args(argv[1:])

    contacts_csv = Path(args.contacts_csv)
    templates_dir = Path(args.templates_dir)
    config_path = Path(args.config)
    sent_log_path = Path(args.sent_log)
    save_pdfs_dir = Path(args.save_pdfs_dir) if args.save_pdfs_dir else None

    return run(
        contacts_csv=contacts_csv,
        templates_dir=templates_dir,
        config_path=config_path,
        sent_log_path=sent_log_path,
        campaign_id=args.campaign_id,
        mode_override=args.mode_override,
        limit=args.limit,
        start_index=args.start_index,
        max_count=args.max_count,
        save_pdfs_dir=save_pdfs_dir,
        max_workers=args.max_workers,
        max_calls_per_minute=args.max_calls_per_minute,
        idempotency_enabled=not args.no_idempotency,
        upload_enabled=bool(args.upload),
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

