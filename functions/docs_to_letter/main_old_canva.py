"""
Google Docs → Canva migration Cloud Function.

HTTP routes:
- GET /health
- GET /migrate?doc_id={google_doc_id}
- GET /oauth/google/callback
- GET /oauth/canva/callback
"""

from __future__ import annotations

import base64
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import functions_framework
import google.auth.transport.requests
import requests
from flask import Request, Response, jsonify, redirect
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import Flow

import io  # placed after imports used by type checkers


logger = logging.getLogger(__name__)
if not logger.handlers:
    # In Cloud Functions, root logger is configured, but for local runs ensure basic config.
    logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class ConfigError(RuntimeError):
    """Configuration error (missing or invalid environment variables)."""


class GoogleDocsError(RuntimeError):
    """Wraps errors from Google Docs/Drive APIs."""


class CanvaApiError(RuntimeError):
    """Wraps errors from Canva API."""


class MigrationError(RuntimeError):
    """Top-level migration orchestration error."""


@dataclass
class DocsToCanvaConfig:
    """Configuration for the Google Docs → Canva migration function."""

    google_client_id: str
    google_client_secret: str
    google_redirect_uri: str

    canva_client_id: str
    canva_client_secret: str
    canva_redirect_uri: str

    canva_api_base: str = "https://api.canva.com/rest/v1"
    canva_auth_url: str = "https://www.canva.com/api/oauth/authorize"
    canva_token_url: str = "https://api.canva.com/rest/v1/oauth/token"

    timeout_seconds: int = 540
    tmp_dir: str = "/tmp"

    @classmethod
    def from_env(cls) -> "DocsToCanvaConfig":
        """Create configuration from environment variables and validate them."""

        def _get(name: str) -> str:
            val = os.environ.get(name)
            if not val:
                raise ConfigError(f"Missing required environment variable: {name}")
            return val

        cfg = cls(
            google_client_id=_get("GOOGLE_CLIENT_ID"),
            google_client_secret=_get("GOOGLE_CLIENT_SECRET"),
            google_redirect_uri=_get("GOOGLE_REDIRECT_URI"),
            canva_client_id=_get("CANVA_CLIENT_ID"),
            canva_client_secret=_get("CANVA_CLIENT_SECRET"),
            canva_redirect_uri=_get("CANVA_REDIRECT_URI"),
        )
        return cfg


class GoogleDocsExporter:
    """Helper for interacting with Google Docs/Drive APIs for read-only export."""

    DOCS_SCOPE = "https://www.googleapis.com/auth/documents.readonly"
    DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"

    def __init__(self, client_id: str, client_secret: str, token_dict: Dict[str, Any]):
        """
        Parameters
        ----------
        client_id:
            OAuth client ID for the Google app.
        client_secret:
            OAuth client secret.
        token_dict:
            Token payload from google-auth / Flow.credentials as a dict.
        """
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_dict = token_dict or {}
        self._credentials: Optional[Credentials] = None
        self._docs_service = None
        self._drive_service = None

    # Credential management -------------------------------------------------

    def set_credentials(self, token_dict: Dict[str, Any]) -> None:
        """Update the internal credential token dictionary."""
        self._token_dict = token_dict or {}
        self._credentials = None  # force re-build on next use

    def _build_credentials(self) -> Credentials:
        if self._credentials is not None:
            return self._credentials

        if not self._token_dict:
            raise GoogleDocsError("Missing Google OAuth token dictionary.")

        # token_dict may already contain all required fields produced by Flow.
        creds = Credentials.from_authorized_user_info(self._token_dict)
        self._credentials = creds
        return creds

    def _ensure_valid_credentials(self) -> Credentials:
        creds = self._build_credentials()
        if creds.expired and creds.refresh_token:
            logger.info("Refreshing Google OAuth access token")
            req = google.auth.transport.requests.Request()
            try:
                creds.refresh(req)
            except Exception as exc:  # noqa: BLE001
                raise GoogleDocsError(f"Failed to refresh Google token: {exc}") from exc
            # Update internal token dict with refreshed data
            self._token_dict = json.loads(creds.to_json())
        return creds

    def _docs(self):
        if self._docs_service is None:
            creds = self._ensure_valid_credentials()
            self._docs_service = build("docs", "v1", credentials=creds, cache_discovery=False)
        return self._docs_service

    def _drive(self):
        if self._drive_service is None:
            creds = self._ensure_valid_credentials()
            self._drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return self._drive_service

    # Public API ------------------------------------------------------------

    def get_document_metadata(self, doc_id: str) -> Dict[str, Any]:
        """Fetch basic metadata (title, revision) for a Google Doc."""
        if not doc_id:
            raise GoogleDocsError("Document ID must not be empty.")
        try:
            logger.info("Fetching Google Doc metadata", extra={"op": "google_metadata", "doc_id": doc_id})
            doc = self._docs().documents().get(documentId=doc_id).execute()
            title = doc.get("title") or doc_id
            revision_id = doc.get("revisionId")
            return {
                "title": title,
                "document_id": doc_id,
                "revision_id": revision_id,
            }
        except HttpError as exc:
            status = getattr(exc, "status_code", None) or getattr(exc, "resp", {}).status if hasattr(exc, "resp") else None
            body = getattr(exc, "content", b"")[:512]
            logger.exception(
                "Google Docs API error while fetching metadata",
                extra={"op": "google_metadata", "doc_id": doc_id, "status": status},
            )
            raise GoogleDocsError(f"Docs API error (status={status}): {body!r}") from exc
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Unexpected error while fetching Google Doc metadata",
                extra={"op": "google_metadata", "doc_id": doc_id},
            )
            raise GoogleDocsError(f"Unexpected error while fetching metadata: {exc}") from exc

    def export_pdf_to_file(self, doc_id: str, output_path: str) -> str:
        """Export the given document as PDF to disk (local testing helper)."""
        if not doc_id:
            raise GoogleDocsError("Document ID must not be empty.")
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        attempts = 0
        backoff = 1.0
        while True:
            attempts += 1
            try:
                logger.info(
                    "Exporting Google Doc to PDF (file)",
                    extra={"op": "google_export_file", "doc_id": doc_id, "attempt": attempts},
                )
                request = self._drive().files().export(fileId=doc_id, mimeType="application/pdf")
                fh = io.FileIO(output_path, mode="wb")
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.info(
                            "PDF export progress",
                            extra={
                                "op": "google_export_file",
                                "doc_id": doc_id,
                                "progress": int(status.progress() * 100),
                            },
                        )
                return output_path
            except HttpError as exc:
                status = getattr(exc, "status_code", None) or getattr(exc, "resp", {}).status if hasattr(exc, "resp") else None
                body = getattr(exc, "content", b"")[:512]
                logger.warning(
                    "Google Drive export_media error; will retry if attempts remain",
                    extra={"op": "google_export_file", "doc_id": doc_id, "status": status, "attempt": attempts},
                )
                if attempts >= 3:
                    raise GoogleDocsError(f"Drive export error (status={status}): {body!r}") from exc
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Unexpected error during PDF export; will retry if attempts remain",
                    extra={"op": "google_export_file", "doc_id": doc_id, "attempt": attempts},
                )
                if attempts >= 3:
                    raise GoogleDocsError(f"Unexpected error while exporting PDF: {exc}") from exc
            time.sleep(backoff)
            backoff *= 2

    def export_pdf_bytes(self, doc_id: str) -> bytes:
        """Export the given document as PDF and return raw bytes."""
        if not doc_id:
            raise GoogleDocsError("Document ID must not be empty.")

        attempts = 0
        backoff = 1.0
        while True:
            attempts += 1
            try:
                logger.info(
                    "Exporting Google Doc to PDF (bytes)",
                    extra={"op": "google_export_bytes", "doc_id": doc_id, "attempt": attempts},
                )
                request = self._drive().files().export(fileId=doc_id, mimeType="application/pdf")
                buf = io.BytesIO()
                downloader = MediaIoBaseDownload(buf, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.info(
                            "PDF export progress",
                            extra={
                                "op": "google_export_bytes",
                                "doc_id": doc_id,
                                "progress": int(status.progress() * 100),
                            },
                        )
                return buf.getvalue()
            except HttpError as exc:
                status = getattr(exc, "status_code", None) or getattr(exc, "resp", {}).status if hasattr(exc, "resp") else None
                body = getattr(exc, "content", b"")[:512]
                logger.warning(
                    "Google Drive export_media error; will retry if attempts remain",
                    extra={"op": "google_export_bytes", "doc_id": doc_id, "status": status, "attempt": attempts},
                )
                if attempts >= 3:
                    raise GoogleDocsError(f"Drive export error (status={status}): {body!r}") from exc
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Unexpected error during PDF export; will retry if attempts remain",
                    extra={"op": "google_export_bytes", "doc_id": doc_id, "attempt": attempts},
                )
                if attempts >= 3:
                    raise GoogleDocsError(f"Unexpected error while exporting PDF: {exc}") from exc
            time.sleep(backoff)
            backoff *= 2


class CanvaImporter:
    """Helper for OAuth and import operations against Canva API."""

    SCOPES = ["design:content:read", "design:content:write", "design:meta:read"]

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        api_base: str,
        token_url: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.api_base = api_base.rstrip("/")
        self.token_url = token_url
        self.access_token = access_token
        self.refresh_token = refresh_token

    # OAuth helpers ---------------------------------------------------------

    def build_authorization_url(self, state: str) -> str:
        """Generate Canva OAuth authorization URL with state parameter."""
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.SCOPES),
            "state": state,
        }
        url = f"{DocsToCanvaConfig.canva_auth_url}?{urlencode(params)}"  # type: ignore[attr-defined]
        
        logger.info(
            "Built Canva authorization URL",
            extra={
                "op": "canva_build_auth_url",
                "redirect_uri": self.redirect_uri,
                "client_id": self.client_id[:20] + "..." if len(self.client_id) > 20 else self.client_id,
                "state": state,
                "url_preview": url[:200],
            },
        )
        
        return url

    @staticmethod
    def exchange_code_for_token(
        code: str,
        redirect_uri: str,
        client_id: str,
        client_secret: str,
        token_url: str,
    ) -> Dict[str, Any]:
        """Exchange authorization code for Canva access token."""
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        
        logger.info(
            "Exchanging Canva authorization code for token",
            extra={
                "op": "canva_token",
                "token_url": token_url,
                "redirect_uri": redirect_uri,
                "has_code": bool(code),
                "code_length": len(code) if code else 0,
            },
        )
        
        try:
            resp = requests.post(token_url, data=payload, timeout=10)
        except requests.exceptions.Timeout:
            logger.error("Canva token exchange timed out", extra={"op": "canva_token"})
            raise CanvaApiError("Canva token exchange timed out") from None
        except requests.exceptions.RequestException as exc:  # noqa: BLE001
            logger.exception(
                "Failed to call Canva token endpoint",
                extra={"op": "canva_token", "error_type": type(exc).__name__},
            )
            raise CanvaApiError(f"Failed to reach Canva token endpoint: {exc}") from exc
        
        logger.info(
            "Canva token exchange response",
            extra={
                "op": "canva_token",
                "status_code": resp.status_code,
                "response_size": len(resp.content),
            },
        )
        
        if resp.status_code != 200:
            body = resp.text[:1024]
            logger.error(
                "Canva token exchange failed",
                extra={
                    "op": "canva_token",
                    "status_code": resp.status_code,
                    "response_body": body,
                    "response_headers": dict(resp.headers),
                },
            )
            raise CanvaApiError(f"Canva token exchange failed (status={resp.status_code}): {body}")
        
        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            logger.error(
                "Failed to parse Canva token response as JSON",
                extra={"op": "canva_token", "response_text": resp.text[:512]},
            )
            raise CanvaApiError(f"Canva token response invalid JSON: {resp.text[:512]}") from exc
        
        logger.info(
            "Canva token exchange successful",
            extra={
                "op": "canva_token",
                "has_access_token": bool(data.get("access_token")),
                "has_refresh_token": bool(data.get("refresh_token")),
                "expires_in": data.get("expires_in"),
            },
        )
        
        return data

    def refresh_access_token(self) -> None:
        """Refresh Canva access token using refresh_token."""
        if not self.refresh_token:
            raise CanvaApiError("No Canva refresh_token available to refresh access token.")
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        logger.warning("Refreshing Canva access token", extra={"op": "canva_refresh"})
        try:
            resp = requests.post(self.token_url, data=payload, timeout=10)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to call Canva refresh endpoint", extra={"op": "canva_refresh"})
            raise CanvaApiError(f"Failed to refresh Canva token: {exc}") from exc
        if resp.status_code != 200:
            body = resp.text[:512]
            logger.error(
                "Canva token refresh failed",
                extra={"op": "canva_refresh", "status": resp.status_code},
            )
            raise CanvaApiError(f"Canva token refresh failed (status={resp.status_code}): {body}")
        data = resp.json()
        self.access_token = data.get("access_token") or self.access_token
        self.refresh_token = data.get("refresh_token") or self.refresh_token

    # Import operations -----------------------------------------------------

    def _auth_header(self) -> Dict[str, str]:
        if not self.access_token:
            raise CanvaApiError("Missing Canva access token.")
        return {"Authorization": f"Bearer {self.access_token}"}

    def import_pdf(self, pdf_bytes: bytes, title: str) -> str:
        """Start Canva import job for a PDF and return job ID."""
        if not pdf_bytes:
            raise CanvaApiError("PDF content is empty.")
        
        pdf_size = len(pdf_bytes)
        logger.info(
            "Preparing Canva import",
            extra={
                "op": "canva_import_prepare",
                "pdf_size_bytes": pdf_size,
                "pdf_size_mb": round(pdf_size / (1024 * 1024), 2),
                "title": title[:100],  # Log first 100 chars of title
            },
        )
        
        title_b64 = base64.b64encode(title.encode("utf-8")).decode("ascii")
        metadata = {
            "title_base64": title_b64,
            "mime_type": "application/pdf",
        }
        metadata_json = json.dumps(metadata)
        
        headers = {
            "Content-Type": "application/octet-stream",
            "Import-Metadata": metadata_json,
            **self._auth_header(),
        }
        
        # Validate PDF is actually a PDF (check magic bytes)
        if pdf_bytes[:4] != b"%PDF":
            logger.error(
                "PDF validation failed - file does not start with PDF magic bytes",
                extra={"op": "canva_import_start", "first_bytes": pdf_bytes[:20].hex()},
            )
            raise CanvaApiError("Invalid PDF file: file does not start with PDF header")
        
        # Log request details (without sensitive data)
        url = f"{self.api_base}/imports"
        logger.info(
            "Sending Canva import request",
            extra={
                "op": "canva_import_start",
                "url": url,
                "pdf_size_bytes": pdf_size,
                "has_auth_header": bool(headers.get("Authorization")),
                "metadata": metadata_json,
                "content_type": headers.get("Content-Type"),
                "auth_header_prefix": headers.get("Authorization", "")[:20] + "..." if headers.get("Authorization") else None,
            },
        )
        
        try:
            resp = requests.post(url, headers=headers, data=pdf_bytes, timeout=60)
        except requests.exceptions.Timeout:
            logger.error("Canva import request timed out", extra={"op": "canva_import_start", "timeout": 60})
            raise CanvaApiError("Canva import request timed out after 60 seconds") from None
        except requests.exceptions.RequestException as exc:  # noqa: BLE001
            logger.exception(
                "Failed to call Canva imports endpoint",
                extra={"op": "canva_import_start", "error_type": type(exc).__name__},
            )
            raise CanvaApiError(f"Failed to call Canva imports endpoint: {exc}") from exc
        
        # Log response details
        logger.info(
            "Canva import response received",
            extra={
                "op": "canva_import_response",
                "status_code": resp.status_code,
                "response_headers": dict(resp.headers),
                "response_size": len(resp.content),
            },
        )
        
        if resp.status_code not in (200, 202):
            # Log full response for debugging
            body = resp.text
            body_preview = body[:2048]  # First 2KB for logging
            
            # Try to extract error message from HTML if it's an HTML error page
            error_message = body_preview
            if "<html" in body.lower() or "<body" in body.lower():
                # It's an HTML error page - try to extract text content
                import re
                # Look for common error message patterns in HTML
                title_match = re.search(r"<title[^>]*>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
                h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", body, re.IGNORECASE | re.DOTALL)
                error_match = re.search(r"error[^>]*>([^<]+)", body, re.IGNORECASE)
                
                extracted_parts = []
                if title_match:
                    extracted_parts.append(f"Title: {title_match.group(1).strip()}")
                if h1_match:
                    extracted_parts.append(f"H1: {h1_match.group(1).strip()}")
                if error_match:
                    extracted_parts.append(f"Error: {error_match.group(1).strip()}")
                
                if extracted_parts:
                    error_message = " | ".join(extracted_parts)
            
            logger.error(
                "Canva import request failed",
                extra={
                    "op": "canva_import_start",
                    "status_code": resp.status_code,
                    "response_body": body_preview,
                    "response_body_full_length": len(body),
                    "response_headers": dict(resp.headers),
                    "content_type": resp.headers.get("Content-Type", ""),
                    "is_html_response": "<html" in body.lower() or "<body" in body.lower(),
                },
            )
            raise CanvaApiError(
                f"Canva import failed (status={resp.status_code}): {error_message}"
            )
        
        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            logger.error(
                "Failed to parse Canva response as JSON",
                extra={
                    "op": "canva_import_start",
                    "response_text": resp.text[:512],
                },
            )
            raise CanvaApiError(f"Canva returned invalid JSON: {resp.text[:512]}") from exc
        
        logger.info(
            "Canva import response parsed",
            extra={"op": "canva_import_start", "response_keys": list(data.keys())},
        )
        
        job_id = data.get("id") or data.get("job_id")
        if not job_id:
            logger.error(
                "Canva import response missing job ID",
                extra={"op": "canva_import_start", "response_data": data},
            )
            raise CanvaApiError(f"Canva import response did not include a job ID. Response: {json.dumps(data)[:512]}")
        
        logger.info("Canva import job created", extra={"op": "canva_import_start", "job_id": job_id})
        return job_id

    def poll_import_job(
        self,
        job_id: str,
        max_attempts: int = 60,
        interval_seconds: int = 2,
    ) -> List[Dict[str, Any]]:
        """Poll Canva import job until completion and return created designs."""
        url = f"{self.api_base}/imports/{job_id}"
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            try:
                resp = requests.get(url, headers=self._auth_header(), timeout=10)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Error polling Canva import job; will retry",
                    extra={"op": "canva_import_poll", "job_id": job_id, "attempt": attempts},
                )
                if attempts >= max_attempts:
                    raise CanvaApiError(f"Failed polling Canva job after {attempts} attempts: {exc}") from exc
                time.sleep(interval_seconds)
                continue

            if resp.status_code != 200:
                body = resp.text[:512]
                logger.error(
                    "Canva job poll failed",
                    extra={"op": "canva_import_poll", "job_id": job_id, "status": resp.status_code},
                )
                raise CanvaApiError(f"Canva job poll failed (status={resp.status_code}): {body}")

            data = resp.json()
            status = data.get("status")
            logger.info(
                "Canva job poll result",
                extra={"op": "canva_import_poll", "job_id": job_id, "status": status, "attempt": attempts},
            )
            if status in ("pending", "in_progress"):
                time.sleep(interval_seconds)
                continue
            if status == "success":
                designs = data.get("designs") or data.get("resources") or []
                normalized: List[Dict[str, Any]] = []
                for d in designs:
                    normalized.append(
                        {
                            "design_id": d.get("id") or d.get("design_id"),
                            "edit_url": d.get("edit_url") or d.get("links", {}).get("edit"),
                            "view_url": d.get("view_url") or d.get("links", {}).get("view"),
                        }
                    )
                return normalized
            if status == "failed":
                error_info = data.get("error") or data
                raise CanvaApiError(f"Canva import job failed: {json.dumps(error_info)[:512]}")

            # Unknown status – keep polling for a bit, then fail.
            time.sleep(interval_seconds)

        raise TimeoutError(f"Canva import job did not complete within {max_attempts * interval_seconds} seconds.")


# In-memory state store for OAuth flows (MVP only; non-persistent across cold starts).
OAUTH_STATE: Dict[str, Dict[str, Any]] = {}


def _build_google_flow(cfg: DocsToCanvaConfig, state: str) -> Flow:
    """Create a google-auth-oauthlib Flow from env config."""
    client_config = {
        "web": {
            "client_id": cfg.google_client_id,
            "client_secret": cfg.google_client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [cfg.google_redirect_uri],
        }
    }
    flow = Flow.from_client_config(
        client_config,
        scopes=[GoogleDocsExporter.DOCS_SCOPE, GoogleDocsExporter.DRIVE_SCOPE],
        state=state,
    )
    flow.redirect_uri = cfg.google_redirect_uri
    return flow


def _html_page(title: str, body_html: str, status: int = 200) -> Response:
    """Render a minimal styled HTML page."""
    html = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
      body {{
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background-color: #f5f5f7;
        margin: 0;
        padding: 0;
      }}
      .container {{
        max-width: 720px;
        margin: 40px auto;
        background: #ffffff;
        border-radius: 12px;
        box-shadow: 0 12px 30px rgba(0,0,0,0.06);
        padding: 32px 40px;
      }}
      h1 {{
        font-size: 24px;
        margin-top: 0;
        margin-bottom: 16px;
      }}
      p {{
        line-height: 1.5;
        color: #444;
      }}
      .design-list a {{
        color: #2563eb;
        text-decoration: none;
      }}
      .design-list a:hover {{
        text-decoration: underline;
      }}
      .footer {{
        margin-top: 24px;
        font-size: 13px;
        color: #777;
      }}
      .tag {{
        display: inline-block;
        padding: 2px 8px;
        border-radius: 999px;
        font-size: 11px;
        background: #eff6ff;
        color: #1d4ed8;
        margin-left: 8px;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      {body_html}
      <div class="footer">
        You can safely close this window once you are done.
      </div>
    </div>
  </body>
</html>
""".strip()
    return Response(html, status=status, mimetype="text/html")


def _error_response(message: str, status: int = 400) -> Response:
    """Return JSON or HTML error depending on Accept header."""
    # In Cloud Functions, we don't have direct Accept parsing here, so just send HTML + JSON convenience.
    payload = {"error": message}
    # Use HTML page for browsers, JSON is still visible via devtools.
    body_html = f"<h1>Something went wrong</h1><p>{message}</p>"
    resp = _html_page("Error", body_html, status=status)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    resp.headers["X-Error-JSON"] = json.dumps(payload)
    return resp


def _get_route_path(request: Request) -> str:
    # Normalize to path component only, no query params.
    return urlparse(request.path or "/").path


def _start_migration(request: Request, cfg: DocsToCanvaConfig) -> Response:
    doc_id = (request.args.get("doc_id") or "").strip()
    if not doc_id:
        return _error_response('Missing required query parameter "doc_id".', status=400)

    state = secrets.token_urlsafe(32)
    OAUTH_STATE[state] = {
        "document_id": doc_id,
        "step": "google_auth",
        "google_token": None,
        "canva_token": None,
    }
    logger.info("Starting migration flow", extra={"op": "migrate_start", "doc_id": doc_id, "state": state})

    flow = _build_google_flow(cfg, state=state)
    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    return redirect(authorization_url, code=302)


def _handle_google_callback(request: Request, cfg: DocsToCanvaConfig) -> Response:
    logger.info(
        "Google OAuth callback received",
        extra={
            "op": "google_oauth_callback",
            "has_error": bool(request.args.get("error")),
            "has_code": bool(request.args.get("code")),
            "has_state": bool(request.args.get("state")),
        },
    )
    
    error = request.args.get("error")
    if error:
        # User may have denied consent.
        error_description = request.args.get("error_description", "")
        logger.error(
            "Google OAuth error in callback",
            extra={"op": "google_oauth", "error": error, "error_description": error_description},
        )
        return _error_response(f"Google OAuth failed or was cancelled: {error}", status=400)

    code = request.args.get("code")
    state = request.args.get("state")
    if not code or not state:
        logger.warning(
            "Missing parameters in Google callback",
            extra={"op": "google_oauth", "has_code": bool(code), "has_state": bool(state)},
        )
        return _error_response("Missing code or state in Google OAuth callback.", status=400)
    
    logger.info("Looking up OAuth state", extra={"op": "google_oauth", "state": state})
    flow_state = OAUTH_STATE.get(state)
    if not flow_state:
        logger.warning(
            "Invalid state in Google callback",
            extra={
                "op": "google_oauth",
                "state": state,
                "available_states_count": len(OAUTH_STATE),
                "available_states": list(OAUTH_STATE.keys())[:5],
            },
        )
        return _error_response("Invalid or expired OAuth state for Google callback.", status=400)

    logger.info(
        "Exchanging Google OAuth code for token",
        extra={"op": "google_oauth", "state": state, "code_length": len(code) if code else 0},
    )
    
    try:
        flow = _build_google_flow(cfg, state=state)
        logger.info("Google Flow built, fetching token", extra={"op": "google_oauth", "state": state})
        flow.fetch_token(code=code)
        creds = flow.credentials
        token_dict = json.loads(creds.to_json())
        flow_state["google_token"] = token_dict
        flow_state["step"] = "canva_auth"
        logger.info(
            "Google OAuth completed successfully",
            extra={
                "op": "google_oauth",
                "state": state,
                "has_access_token": bool(token_dict.get("token")),
                "has_refresh_token": bool(token_dict.get("refresh_token")),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Google OAuth token exchange failed",
            extra={
                "op": "google_oauth",
                "state": state,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        return _error_response(f"Failed to complete Google OAuth: {exc}", status=500)

    # Redirect to Canva OAuth
    logger.info(
        "Building Canva authorization URL",
        extra={
            "op": "canva_oauth_redirect",
            "state": state,
            "canva_redirect_uri": cfg.canva_redirect_uri,
            "google_redirect_uri": cfg.google_redirect_uri,  # Log both for comparison
        },
    )
    
    # Validate that Canva redirect URI is different from Google redirect URI
    if cfg.canva_redirect_uri == cfg.google_redirect_uri:
        logger.error(
            "Canva redirect URI matches Google redirect URI - this is incorrect!",
            extra={
                "op": "canva_oauth_redirect",
                "redirect_uri": cfg.canva_redirect_uri,
            },
        )
        return _error_response(
            "Configuration error: CANVA_REDIRECT_URI must be different from GOOGLE_REDIRECT_URI. "
            f"Both are set to: {cfg.canva_redirect_uri}",
            status=500,
        )
    
    # Validate that Canva redirect URI contains /canva/callback
    if "/canva/callback" not in cfg.canva_redirect_uri:
        logger.error(
            "Canva redirect URI does not contain /canva/callback",
            extra={
                "op": "canva_oauth_redirect",
                "canva_redirect_uri": cfg.canva_redirect_uri,
            },
        )
        return _error_response(
            f"Configuration error: CANVA_REDIRECT_URI must end with /oauth/canva/callback. "
            f"Current value: {cfg.canva_redirect_uri}",
            status=500,
        )
    
    try:
        canva = CanvaImporter(
            client_id=cfg.canva_client_id,
            client_secret=cfg.canva_client_secret,
            redirect_uri=cfg.canva_redirect_uri,
            api_base=cfg.canva_api_base,
            token_url=cfg.canva_token_url,
        )
        canva_auth_url = canva.build_authorization_url(state=state)
        logger.info(
            "Redirecting to Canva OAuth",
            extra={
                "op": "canva_oauth_redirect",
                "state": state,
                "canva_auth_url": canva_auth_url[:200],  # Log first 200 chars of URL
            },
        )
        return redirect(canva_auth_url, code=302)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to build Canva authorization URL",
            extra={"op": "canva_oauth_redirect", "state": state, "error_type": type(exc).__name__},
        )
        return _error_response(f"Failed to redirect to Canva OAuth: {exc}", status=500)


def _handle_canva_callback(request: Request, cfg: DocsToCanvaConfig) -> Response:
    error = request.args.get("error")
    if error:
        error_description = request.args.get("error_description", "")
        logger.error(
            "Canva OAuth error received",
            extra={"op": "canva_oauth", "error": error, "error_description": error_description},
        )
        return _error_response(f"Canva OAuth failed or was cancelled: {error}. {error_description}", status=400)

    code = request.args.get("code")
    state = request.args.get("state")
    if not code or not state:
        logger.warning(
            "Missing parameters in Canva callback",
            extra={"op": "canva_oauth", "has_code": bool(code), "has_state": bool(state)},
        )
        return _error_response("Missing code or state in Canva OAuth callback.", status=400)
    
    flow_state = OAUTH_STATE.get(state)
    if not flow_state:
        logger.warning(
            "Invalid state in Canva callback",
            extra={"op": "canva_oauth", "state": state, "available_states": list(OAUTH_STATE.keys())[:5]},
        )
        return _error_response("Invalid or expired OAuth state for Canva callback.", status=400)

    logger.info(
        "Processing Canva OAuth callback",
        extra={
            "op": "canva_oauth",
            "state": state,
            "has_google_token": bool(flow_state.get("google_token")),
            "redirect_uri": cfg.canva_redirect_uri,
        },
    )

    try:
        token_data = CanvaImporter.exchange_code_for_token(
            code=code,
            redirect_uri=cfg.canva_redirect_uri,
            client_id=cfg.canva_client_id,
            client_secret=cfg.canva_client_secret,
            token_url=cfg.canva_token_url,
        )
        flow_state["canva_token"] = token_data
        flow_state["step"] = "ready_to_migrate"
        logger.info(
            "Canva OAuth completed successfully",
            extra={
                "op": "canva_oauth",
                "state": state,
                "has_access_token": bool(token_data.get("access_token")),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Canva OAuth token exchange failed",
            extra={
                "op": "canva_oauth",
                "state": state,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        return _error_response(f"Failed to complete Canva OAuth: {exc}", status=500)

    # Perform the actual migration.
    logger.info("Starting migration after OAuth completion", extra={"op": "migrate", "state": state})
    try:
        doc_id = flow_state["document_id"]
        google_token = flow_state["google_token"]
        canva_token = flow_state["canva_token"]
        
        logger.info(
            "Migration inputs validated",
            extra={
                "op": "migrate",
                "doc_id": doc_id,
                "has_google_token": bool(google_token),
                "has_canva_token": bool(canva_token),
            },
        )
        
        result = _perform_migration(doc_id, google_token, canva_token, cfg, state)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Migration failed",
            extra={
                "op": "migrate",
                "state": state,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        # Ensure we don't leak state forever.
        OAUTH_STATE.pop(state, None)
        return _error_response(f"Migration failed: {exc}", status=500)

    # Clean up state best-effort.
    OAUTH_STATE.pop(state, None)

    designs = result["designs"]
    title = result["title"]

    items_html = []
    for idx, d in enumerate(designs, start=1):
        edit = d.get("edit_url")
        view = d.get("view_url")
        design_id = d.get("design_id") or f"design-{idx}"
        row = "<li>"
        row += f"<strong>{design_id}</strong>"
        if edit:
            row += f' – <a href="{edit}" target="_blank" rel="noopener noreferrer">Edit in Canva</a>'
        if view:
            row += f' · <a href="{view}" target="_blank" rel="noopener noreferrer">View</a>'
        row += "</li>"
        items_html.append(row)

    list_html = "<ul class=\"design-list\">" + "".join(items_html) + "</ul>" if items_html else "<p>No designs were returned.</p>"
    body_html = f"""
<h1>Document migrated to Canva <span class="tag">Success</span></h1>
<p><strong>Document title:</strong> {title}</p>
<p><strong>Designs created:</strong> {len(designs)}</p>
{list_html}
"""
    return _html_page("Migration complete", body_html, status=200)


def _perform_migration(
    doc_id: str,
    google_token: Dict[str, Any],
    canva_token: Dict[str, Any],
    cfg: DocsToCanvaConfig,
    state: str,
) -> Dict[str, Any]:
    """Perform full migration: metadata fetch, PDF export, Canva import and poll."""
    if not doc_id:
        raise MigrationError("Missing document ID for migration.")
    if not google_token:
        raise MigrationError("Missing Google token in migration state.")
    if not canva_token:
        raise MigrationError("Missing Canva token in migration state.")

    logger.info(
        "Starting migration orchestration",
        extra={
            "op": "migrate",
            "doc_id": doc_id,
            "state": state,
            "has_google_token": bool(google_token),
            "has_canva_token": bool(canva_token),
            "canva_has_access_token": bool(canva_token.get("access_token")),
        },
    )

    # Step 1: Initialize Google exporter and fetch metadata
    logger.info("Step 1: Initializing Google Docs exporter", extra={"op": "migrate", "step": 1})
    exporter = GoogleDocsExporter(
        client_id=cfg.google_client_id,
        client_secret=cfg.google_client_secret,
        token_dict=google_token,
    )
    
    logger.info("Step 2: Fetching document metadata", extra={"op": "migrate", "step": 2})
    metadata = exporter.get_document_metadata(doc_id)
    title = metadata.get("title") or doc_id
    logger.info(
        "Document metadata retrieved",
        extra={"op": "migrate", "title": title, "doc_id": doc_id, "revision_id": metadata.get("revision_id")},
    )

    # Step 2: Export PDF
    logger.info("Step 3: Exporting document to PDF", extra={"op": "migrate", "step": 3})
    pdf_bytes = exporter.export_pdf_bytes(doc_id)
    logger.info(
        "PDF export completed",
        extra={"op": "migrate", "pdf_size_bytes": len(pdf_bytes), "pdf_size_mb": round(len(pdf_bytes) / (1024 * 1024), 2)},
    )

    # Step 3: Initialize Canva importer
    logger.info("Step 4: Initializing Canva importer", extra={"op": "migrate", "step": 4})
    importer = CanvaImporter(
        client_id=cfg.canva_client_id,
        client_secret=cfg.canva_client_secret,
        redirect_uri=cfg.canva_redirect_uri,
        api_base=cfg.canva_api_base,
        token_url=cfg.canva_token_url,
        access_token=canva_token.get("access_token"),
        refresh_token=canva_token.get("refresh_token"),
    )
    logger.info(
        "Canva importer initialized",
        extra={
            "op": "migrate",
            "api_base": cfg.canva_api_base,
            "has_access_token": bool(importer.access_token),
            "has_refresh_token": bool(importer.refresh_token),
        },
    )

    # Step 4: Import to Canva
    logger.info("Step 5: Starting Canva import", extra={"op": "migrate", "step": 5})
    try:
        job_id = importer.import_pdf(pdf_bytes, title=title)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Canva import failed",
            extra={
                "op": "migrate",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "pdf_size_bytes": len(pdf_bytes),
                "title": title,
            },
        )
        raise
    
    # Step 5: Poll for completion
    logger.info("Step 6: Polling Canva import job", extra={"op": "migrate", "step": 6, "job_id": job_id})
    try:
        designs = importer.poll_import_job(job_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Canva import job polling failed",
            extra={"op": "migrate", "job_id": job_id, "error_type": type(exc).__name__},
        )
        raise

    logger.info(
        "Migration orchestration completed",
        extra={"op": "migrate", "doc_id": doc_id, "state": state, "design_count": len(designs)},
    )
    return {"title": title, "designs": designs}


@functions_framework.http
def migrate_document(request: Request) -> Response:
    """HTTP entry point for Google Docs → Canva migration."""
    # Health endpoint for uptime checks
    if (request.path or "").strip("/") == "health":
        return Response("ok", mimetype="text/plain")

    # Log incoming request
    route = _get_route_path(request)
    logger.info(
        "Incoming request",
        extra={
            "op": "request",
            "method": request.method,
            "path": request.path,
            "route": route,
            "query_string": request.query_string.decode() if request.query_string else None,
        },
    )

    try:
        cfg = DocsToCanvaConfig.from_env()
        logger.debug("Configuration loaded successfully", extra={"op": "config"})
    except ConfigError as exc:
        logger.error(
            "Configuration error for migrate_document",
            extra={"op": "config", "error": str(exc)},
        )
        return _error_response(str(exc), status=500)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Unexpected error loading configuration",
            extra={"op": "config", "error_type": type(exc).__name__},
        )
        return _error_response(f"Configuration error: {exc}", status=500)

    try:
        if route.endswith("/migrate"):
            logger.info("Routing to start_migration", extra={"op": "routing"})
            return _start_migration(request, cfg)
        if route.endswith("/oauth/google/callback"):
            logger.info("Routing to google_callback", extra={"op": "routing"})
            return _handle_google_callback(request, cfg)
        if route.endswith("/oauth/canva/callback"):
            logger.info("Routing to canva_callback", extra={"op": "routing"})
            return _handle_canva_callback(request, cfg)

        # Fallback: show simple info / help.
        if route in ("/", ""):
            body_html = """
<h1>Google Docs → Canva migration</h1>
<p>Use <code>/migrate?doc_id=YOUR_DOC_ID</code> to start a migration flow.</p>
"""
            return _html_page("Docs to Canva", body_html, status=200)

        logger.warning("Route not found", extra={"op": "routing", "route": route})
        return _error_response("Not found", status=404)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Unhandled error in migrate_document",
            extra={
                "op": "migrate_document",
                "route": route,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        return _error_response(f"Unexpected error: {exc}", status=500)


