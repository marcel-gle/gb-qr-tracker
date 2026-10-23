"""Load API keys / LLM settings (e.g. ``APOLLO_API_KEY``, ``OPENAI_API_KEY``, ``ANTHROPIC_API_KEY``, ``ANTHROPIC_MODEL``, ``ANTHROPIC_EFFORT``) from the repo ``.env``."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

REPO_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def load_project_env(path: Path = REPO_ENV_PATH) -> bool:
    """Load ``path`` into ``os.environ``; variables already set in the shell win."""
    if not path.exists():
        return False
    try:
        from dotenv import load_dotenv
    except ImportError:
        logger.warning("python-dotenv not installed; %s not loaded.", path)
        return False
    return bool(load_dotenv(path, override=False))
