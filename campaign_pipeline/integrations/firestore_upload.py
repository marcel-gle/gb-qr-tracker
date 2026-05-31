from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]


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
