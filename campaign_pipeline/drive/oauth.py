from __future__ import annotations

import json
from typing import Any, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from .config import DRIVE_SCOPES, client_secret_path, token_path


def _load_token_data() -> dict[str, Any] | None:
    path = token_path()
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, OSError):
        return None


def get_credentials(*, refresh: bool = True) -> Optional[Credentials]:
    """Return valid credentials or None if the user has not connected yet."""
    creds: Credentials | None = None
    data = _load_token_data()
    if data:
        creds = Credentials.from_authorized_user_info(data, scopes=list(DRIVE_SCOPES))

    if creds and creds.expired and creds.refresh_token and refresh:
        creds.refresh(Request())
        _save_credentials(creds)

    if creds and creds.valid:
        return creds
    return None


def _save_credentials(creds: Credentials) -> None:
    token_path().write_text(creds.to_json(), encoding="utf-8")


def is_connected() -> bool:
    return get_credentials(refresh=True) is not None


def run_connect_flow() -> Credentials:
    secret = client_secret_path()
    if not secret.exists():
        raise FileNotFoundError(
            f"Google OAuth client secret not found at {secret}. "
            "Download a Desktop OAuth client JSON from Google Cloud Console and save it as client_secret.json "
            f"(or set CAMPAIGN_DRIVE_CLIENT_SECRET)."
        )
    flow = InstalledAppFlow.from_client_secrets_file(str(secret), scopes=list(DRIVE_SCOPES))
    creds = flow.run_local_server(port=0, open_browser=True)
    _save_credentials(creds)
    return creds


def disconnect() -> None:
    path = token_path()
    if path.exists():
        path.unlink()
