from __future__ import annotations

import json
from typing import Any

from .config import drive_settings_path


def load_drive_settings() -> dict[str, Any]:
    path = drive_settings_path()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_drive_settings(**updates: Any) -> dict[str, Any]:
    data = load_drive_settings()
    data.update({k: v for k, v in updates.items() if v is not None})
    path = drive_settings_path()
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return data
