#!/usr/bin/env python3
"""Example: read A1:B5 from first sheet, optionally write a test cell.

Usage (from repo root):
  python -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
  # or use credentials.json for OAuth (browser opens once)
  PYTHONPATH=src python scripts/example_sheets_rw.py

Set GOOGLE_SHEETS_SPREADSHEET_ID if not using the default Ashley sheet ID.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Allow running as script from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sheets_client import SheetsClient  # noqa: E402


def load_local_env() -> None:
    """Load simple KEY=VALUE pairs from a repo-local .env file."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


def main():
    load_local_env()
    client = SheetsClient()
    # Adjust tab name to match your spreadsheet (e.g. first sheet is often Sheet1).
    tab = os.environ.get("GOOGLE_SHEETS_TAB", "Sheet1")
    read_range = f"{tab}!A1:B5"
    rows = client.get_range(read_range)
    print("Read", read_range, "=>", rows)

    if os.environ.get("SHEETS_EXAMPLE_WRITE") == "1":
        stamp_range = f"{tab}!Z1"
        client.update_range(stamp_range, [["example_sheets_rw"]])
        print("Wrote", stamp_range)


if __name__ == "__main__":
    main()
