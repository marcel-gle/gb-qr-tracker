from __future__ import annotations

import os
from pathlib import Path

DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive",)
FOLDER_MIME = "application/vnd.google-apps.folder"
SKIP_PUSH_SUFFIXES = (".pyc",)
SKIP_PUSH_NAMES = {"__pycache__"}


def drive_config_dir() -> Path:
    path = Path(os.environ.get("CAMPAIGN_DRIVE_CONFIG_DIR", Path.home() / ".config" / "campaign-pipeline"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def client_secret_path() -> Path:
    env = os.environ.get("CAMPAIGN_DRIVE_CLIENT_SECRET")
    if env:
        return Path(env).expanduser()
    return drive_config_dir() / "client_secret.json"


def token_path() -> Path:
    return drive_config_dir() / "token.json"


def drive_settings_path() -> Path:
    return drive_config_dir() / "settings.json"


def drive_cache_root() -> Path:
    root = Path(os.environ.get("CAMPAIGN_DRIVE_CACHE", Path.home() / ".cache" / "campaign-pipeline" / "drive"))
    root.mkdir(parents=True, exist_ok=True)
    return root


def local_root_for_drive_folder(folder_id: str) -> Path:
    return drive_cache_root() / folder_id
