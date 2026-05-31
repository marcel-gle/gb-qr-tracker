from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials

from .config import FOLDER_MIME

GOOGLE_EXPORT_SKIP_PREFIX = "application/vnd.google-apps."


class DriveClient:
    def __init__(self, credentials: Credentials) -> None:
        self._service = build("drive", "v3", credentials=credentials, cache_discovery=False)

    @property
    def service(self):
        return self._service

    def get_file(self, file_id: str, *, fields: str = "id,name,mimeType,modifiedTime,md5Checksum,parents") -> dict[str, Any]:
        return self._service.files().get(fileId=file_id, fields=fields).execute()

    def list_children(self, folder_id: str) -> list[dict[str, Any]]:
        q = f"'{folder_id}' in parents and trashed=false"
        items: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            resp = (
                self._service.files()
                .list(
                    q=q,
                    spaces="drive",
                    fields="nextPageToken, files(id,name,mimeType,modifiedTime,md5Checksum)",
                    pageToken=page_token,
                    pageSize=200,
                )
                .execute()
            )
            items.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return items

    def list_folder_names(self, folder_id: str) -> list[str]:
        return sorted(
            f["name"]
            for f in self.list_children(folder_id)
            if f.get("mimeType") == FOLDER_MIME and f.get("name") and not str(f["name"]).startswith(".")
        )

    def find_child(self, parent_id: str, name: str, *, mime_type: str | None = None) -> dict[str, Any] | None:
        safe_name = name.replace("'", "\\'")
        q = f"'{parent_id}' in parents and name='{safe_name}' and trashed=false"
        if mime_type:
            q += f" and mimeType='{mime_type}'"
        resp = self._service.files().list(q=q, spaces="drive", fields="files(id,name,mimeType,modifiedTime,md5Checksum)", pageSize=1).execute()
        files = resp.get("files", [])
        return files[0] if files else None

    def ensure_folder(self, parent_id: str, name: str) -> str:
        existing = self.find_child(parent_id, name, mime_type=FOLDER_MIME)
        if existing:
            return str(existing["id"])
        created = (
            self._service.files()
            .create(
                body={"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]},
                fields="id",
            )
            .execute()
        )
        return str(created["id"])

    def download_file(self, file_id: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        request = self._service.files().get_media(fileId=file_id)
        with dest.open("wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

    def upload_file(self, local_path: Path, parent_id: str, *, drive_file_id: str | None = None) -> str:
        media = MediaFileUpload(str(local_path), resumable=True)
        if drive_file_id:
            updated = (
                self._service.files()
                .update(fileId=drive_file_id, media_body=media, fields="id")
                .execute()
            )
            return str(updated["id"])
        created = (
            self._service.files()
            .create(
                body={"name": local_path.name, "parents": [parent_id]},
                media_body=media,
                fields="id",
            )
            .execute()
        )
        return str(created["id"])

    @staticmethod
    def is_syncable_file(item: dict[str, Any]) -> bool:
        mime = str(item.get("mimeType") or "")
        if mime == FOLDER_MIME:
            return False
        if mime.startswith(GOOGLE_EXPORT_SKIP_PREFIX):
            return False
        return True
