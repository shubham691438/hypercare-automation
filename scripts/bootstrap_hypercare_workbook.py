#!/usr/bin/env python3
"""Create tabs, headers, and seed the Query registry on the hypercare sheet.

  PYTHONPATH=src python scripts/bootstrap_hypercare_workbook.py
  HYPERCARE_BOOTSTRAP_OVERWRITE=1 PYTHONPATH=src python scripts/bootstrap_hypercare_workbook.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from hypercare_bootstrap import bootstrap_hypercare_workbook  # noqa: E402
from sheets_client import SheetsClient  # noqa: E402


def load_local_env() -> None:
    env_path = _ROOT / ".env"
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
    overwrite = os.environ.get("HYPERCARE_BOOTSTRAP_OVERWRITE", "").strip() in (
        "1",
        "true",
        "yes",
    )
    client = SheetsClient()
    bootstrap_hypercare_workbook(client, overwrite=overwrite)
    print("Bootstrap done for spreadsheet:", client.spreadsheet_id)


if __name__ == "__main__":
    main()
