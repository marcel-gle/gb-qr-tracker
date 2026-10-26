from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]

NO_TRACKING_DOMAIN = "none"


def parse_tracking_domain(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Read the required tracking domain field.

    Returns (tracking_url_prefix, error). "none" gives (None, None): no prefix,
    so upload_processor prints the default ihr-brief.de. Otherwise the value is
    kept as typed, because it is printed on the letter as "<value>/<code>".
    """
    value = (raw or "").strip()
    if not value:
        return None, 'Tracking domain is required. Type "none" to print on ihr-brief.de.'
    if value.lower() == NO_TRACKING_DOMAIN:
        return None, None
    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = parsed.hostname or ""
    if parsed.scheme not in ("http", "https") or "." not in host or " " in value:
        return None, f"Tracking domain {raw!r} is not a domain like go.rocket-letter.de."
    return value.rstrip("/"), None


def run_firestore_upload(
    *,
    env: str,
    owner_id: str,
    campaign_code: str,
    campaign_name: str,
    input_csv: Path,
    templates_dir: Path,
    destination: str,
    campaign_id: str = "",
    upload: bool = False,
    tracking_url_prefix: Optional[str] = None,
) -> tuple[int, str, str]:
    cmd: List[str] = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "business" / "local_process_upload.py"),
        "--env",
        env,
        "--owner-id",
        owner_id,
        "--campaign-code",
        campaign_code,
        "--campaign-name",
        campaign_name,
        "--input-csv",
        str(input_csv),
        "--templates-dir",
        str(templates_dir),
        "--destination",
        destination,
    ]
    if campaign_id:
        cmd.extend(["--campaign-id", campaign_id])
    if tracking_url_prefix:
        cmd.extend(["--tracking-url-prefix", tracking_url_prefix])
    if upload:
        cmd.append("--upload")

    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    return result.returncode, result.stdout or "", result.stderr or ""


def run_pdf_generation(
    *,
    contacts_csv: Path,
    templates_dir: Path,
    config_path: Path,
    save_pdfs_dir: Path,
    campaign_id: str = "",
    limit: Optional[int] = None,
) -> tuple[int, str, str]:
    cmd: List[str] = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "send_letter" / "send_letters_onlinebrief24.py"),
        str(contacts_csv),
        "--templates-dir",
        str(templates_dir),
        "--config",
        str(config_path),
        "--mode",
        "test",
        "--save-pdfs-dir",
        str(save_pdfs_dir),
    ]
    if campaign_id:
        cmd.extend(["--campaign-id", campaign_id])
    if limit and limit > 0:
        cmd.extend(["--limit", str(limit)])

    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    return result.returncode, result.stdout or "", result.stderr or ""
