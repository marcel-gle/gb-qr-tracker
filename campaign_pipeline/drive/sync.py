from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from google.oauth2.credentials import Credentials

from .client import DriveClient
from .config import FOLDER_MIME, SKIP_PUSH_NAMES, SKIP_PUSH_SUFFIXES, local_root_for_drive_folder

logger = logging.getLogger(__name__)

MANIFEST_NAME = ".drive_sync_manifest.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _should_push_file(path: Path) -> bool:
    if path.name in SKIP_PUSH_NAMES:
        return False
    if path.name == MANIFEST_NAME:
        return False
    return path.suffix not in SKIP_PUSH_SUFFIXES


class DriveSync:
    """Mirror a Drive folder tree to a local cache and push local changes back."""

    def __init__(self, credentials: Credentials, root_folder_id: str) -> None:
        self.client = DriveClient(credentials)
        self.root_folder_id = root_folder_id
        self.local_root = local_root_for_drive_folder(root_folder_id)

    def manifest_path(self, campaign_dir: Path) -> Path:
        return campaign_dir / ".pipeline" / MANIFEST_NAME

    def load_manifest(self, campaign_dir: Path) -> dict[str, Any]:
        path = self.manifest_path(campaign_dir)
        if not path.exists():
            return {"files": {}}
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("files"), dict):
                return data
        except (json.JSONDecodeError, OSError):
            pass
        return {"files": {}}

    def save_manifest(self, campaign_dir: Path, manifest: dict[str, Any]) -> None:
        path = self.manifest_path(campaign_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        manifest["updated_at"] = _utc_now_iso()
        with path.open("w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

    def pull_root(self) -> dict[str, int]:
        """Download all campaign folders under the configured Drive root."""
        self.local_root.mkdir(parents=True, exist_ok=True)
        downloaded = 0
        for item in self.client.list_children(self.root_folder_id):
            if item.get("mimeType") != FOLDER_MIME:
                continue
            name = str(item.get("name") or "")
            if not name or name.startswith("."):
                continue
            stats = self.pull_campaign(name)
            downloaded += stats.get("downloaded", 0)
        return {"downloaded": downloaded}

    def pull_campaign(self, campaign_name: str) -> dict[str, int]:
        folder = self.client.find_child(self.root_folder_id, campaign_name, mime_type=FOLDER_MIME)
        if not folder:
            return {"downloaded": 0, "skipped": 0}
        local_campaign = self.local_root / campaign_name
        manifest = self.load_manifest(local_campaign)
        downloaded, skipped = self._pull_tree(str(folder["id"]), local_campaign, manifest, prefix="")
        self.save_manifest(local_campaign, manifest)
        return {"downloaded": downloaded, "skipped": skipped}

    def _pull_tree(
        self,
        drive_folder_id: str,
        local_folder: Path,
        manifest: dict[str, Any],
        *,
        prefix: str,
    ) -> tuple[int, int]:
        local_folder.mkdir(parents=True, exist_ok=True)
        files_map: dict[str, Any] = manifest.setdefault("files", {})
        downloaded = 0
        skipped = 0
        for item in self.client.list_children(drive_folder_id):
            name = str(item.get("name") or "")
            if not name:
                continue
            rel = f"{prefix}{name}" if not prefix else f"{prefix}/{name}"
            if item.get("mimeType") == FOLDER_MIME:
                d, s = self._pull_tree(str(item["id"]), local_folder / name, manifest, prefix=rel)
                downloaded += d
                skipped += s
                continue
            if not self.client.is_syncable_file(item):
                skipped += 1
                continue
            dest = local_folder / name
            remote_mtime = str(item.get("modifiedTime") or "")
            entry = files_map.get(rel, {})
            if dest.exists() and entry.get("modifiedTime") == remote_mtime:
                skipped += 1
                continue
            self.client.download_file(str(item["id"]), dest)
            files_map[rel] = {
                "id": item["id"],
                "modifiedTime": remote_mtime,
                "md5Checksum": item.get("md5Checksum"),
            }
            downloaded += 1
        return downloaded, skipped

    def push_campaign(self, campaign_dir: Path) -> dict[str, int]:
        campaign_dir = campaign_dir.resolve()
        if not campaign_dir.is_dir():
            raise FileNotFoundError(f"Campaign directory not found: {campaign_dir}")
        try:
            rel_name = campaign_dir.relative_to(self.local_root.resolve()).parts[0]
        except ValueError as exc:
            raise ValueError(f"Campaign dir {campaign_dir} is not under Drive cache {self.local_root}") from exc

        drive_campaign_id = self.client.ensure_folder(self.root_folder_id, rel_name)
        manifest = self.load_manifest(campaign_dir)
        uploaded, skipped = self._push_tree(campaign_dir, drive_campaign_id, manifest, prefix="")
        self.save_manifest(campaign_dir, manifest)
        return {"uploaded": uploaded, "skipped": skipped}

    def _push_tree(
        self,
        local_folder: Path,
        drive_folder_id: str,
        manifest: dict[str, Any],
        *,
        prefix: str,
    ) -> tuple[int, int]:
        files_map: dict[str, Any] = manifest.setdefault("files", {})
        uploaded = 0
        skipped = 0

        for path in sorted(local_folder.rglob("*")):
            if not path.is_file() or not _should_push_file(path):
                continue
            rel = str(path.relative_to(local_folder)).replace("\\", "/")
            rel_key = f"{prefix}/{rel}" if prefix else rel

            parts = Path(rel).parts
            parent_drive_id = drive_folder_id
            if len(parts) > 1:
                parent_drive_id = self._ensure_drive_path(drive_folder_id, parts[:-1])

            entry = files_map.get(rel_key, {})
            local_mtime = path.stat().st_mtime
            if entry.get("local_mtime") == local_mtime and entry.get("id"):
                skipped += 1
                continue

            drive_id = self.client.upload_file(path, parent_drive_id, drive_file_id=entry.get("id"))
            meta = self.client.get_file(drive_id)
            files_map[rel_key] = {
                "id": drive_id,
                "modifiedTime": meta.get("modifiedTime"),
                "md5Checksum": meta.get("md5Checksum"),
                "local_mtime": local_mtime,
            }
            uploaded += 1

        return uploaded, skipped

    def _ensure_drive_path(self, root_drive_id: str, parts: tuple[str, ...]) -> str:
        current = root_drive_id
        for part in parts:
            current = self.client.ensure_folder(current, part)
        return current

    def ensure_campaign_on_drive(self, campaign_name: str) -> Path:
        """Ensure local cache folder exists and has Drive folder; return local path."""
        local_campaign = self.local_root / campaign_name
        local_campaign.mkdir(parents=True, exist_ok=True)
        for sub in ("lists", "lists/incoming", "templates", "pdf_output", ".pipeline"):
            (local_campaign / sub).mkdir(parents=True, exist_ok=True)
        self.client.ensure_folder(self.root_folder_id, campaign_name)
        return local_campaign

    def list_campaign_names(self) -> list[str]:
        remote = self.client.list_folder_names(self.root_folder_id)
        local = []
        if self.local_root.exists():
            local = [p.name for p in self.local_root.iterdir() if p.is_dir() and not p.name.startswith(".")]
        return sorted(set(remote) | set(local))
