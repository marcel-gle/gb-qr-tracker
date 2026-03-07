from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging for the list_processing pipeline.

    This mirrors the style used in the existing scripts while keeping the
    configuration local to this module.
    """
    logging.basicConfig(
        level=level,
        # Keep CLI output clean: no timestamps, no logger names, just the message.
        format="%(message)s",
    )

    # Silence very verbose third-party debug logs (especially OpenAI/httpx request dumps)
    # even when --verbose sets the root logger to DEBUG.
    for logger_name in (
        "openai",
        "openai._base_client",
        "httpx",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def log_step_banner(title: str) -> None:
    """
    Log a clear visual separator for major pipeline steps.
    """
    line = "=" * 80
    # Use print for human-friendly CLI output without logging prefixes.
    print(f"\n{line}\n{title}\n{line}")

