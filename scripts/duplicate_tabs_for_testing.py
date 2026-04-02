#!/usr/bin/env python3
"""Duplicate each non-test tab so you can run automation against copies.

Skips tabs whose title already ends with HYPERCARE_TEST_TAB_SUFFIX (default " (TEST)").

Usage (repo root):
  source .venv/bin/activate
  HYPERCARE_CLIENT=ashley DRY_RUN=1 PYTHONPATH=src python scripts/duplicate_tabs_for_testing.py
  HYPERCARE_CLIENT=ashley PYTHONPATH=src python scripts/duplicate_tabs_for_testing.py

Env:
  GOOGLE_SHEETS_SPREADSHEET_ID — overrides client file
  HYPERCARE_CLIENT — client id from config/clients.json
  HYPERCARE_CLIENTS_FILE — path to clients json (default config/clients.json)
  HYPERCARE_TEST_TAB_SUFFIX — default " (TEST)"
  DRY_RUN=1 — print actions only
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sheets_client import SheetsClient  # noqa: E402


def load_local_env() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.is_file():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k, v)


def main() -> None:
    load_local_env()
    suffix = os.environ.get("HYPERCARE_TEST_TAB_SUFFIX", " (TEST)")
    dry = os.environ.get("DRY_RUN", "").strip() in ("1", "true", "yes")

    client = SheetsClient()
    tabs = client.list_tabs()
    to_dup: list[tuple[int, str, str]] = []
    for t in tabs:
        if t.get("hidden"):
            continue
        title = (t.get("title") or "").strip()
        sid = t.get("sheetId")
        if sid is None or not title:
            continue
        if title.endswith(suffix):
            continue
        new_title = f"{title}{suffix}"
        to_dup.append((int(sid), title, new_title))

    if not to_dup:
        print("No tabs to duplicate (all skipped or already have suffix).")
        return

    print(f"Spreadsheet: {client.spreadsheet_id}")
    print(f"Suffix: {suffix!r}")
    for _, old, new in to_dup:
        print(f"  duplicate: {old!r} -> {new!r}")

    if dry:
        print("DRY_RUN=1: no API writes.")
        return

    for sheet_id, _old, new_title in to_dup:
        resp = client.duplicate_sheet(sheet_id, new_title)
        replies = resp.get("replies") or []
        if replies and "duplicateSheet" in replies[0]:
            dup = replies[0]["duplicateSheet"]
            props = dup.get("properties") or {}
            print(
                f"Created tab {props.get('title')!r} sheetId={props.get('sheetId')}"
            )
        else:
            print("Done (unexpected reply shape):", resp)


if __name__ == "__main__":
    main()
