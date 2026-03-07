"""
Thin wrapper to keep the old CLI path working.

Delegates to `list_processing.main`.
"""

from __future__ import annotations

import sys

from list_processing.main import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

