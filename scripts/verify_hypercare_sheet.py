#!/usr/bin/env python3
"""Read back key ranges after a run and print a short verification summary.

  PYTHONPATH=src python scripts/verify_hypercare_sheet.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from hypercare_bootstrap import (  # noqa: E402
    TAB_FUNNEL,
    TAB_JOB,
    TAB_MOJO,
    TAB_QUERY_REGISTRY,
)
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


def last_nonempty_row(rows: list[list[object]]) -> list[object]:
    for row in reversed(rows):
        if any(str(cell).strip() for cell in row):
            return row
    return []


def main() -> None:
    load_local_env()
    c = SheetsClient()
    print("spreadsheet_id:", c.spreadsheet_id)
    print()
    print("Overview last run:", c.get_range("'Overview'!A13:B15"))
    print()
    job_rows = c.get_range(f"'{TAB_JOB}'!A2:J1000")
    print(f"{TAB_JOB} latest row:", last_nonempty_row(job_rows))
    print()
    mojo_rows = c.get_range(f"'{TAB_MOJO}'!A2:H1000")
    print(f"{TAB_MOJO} latest row:", last_nonempty_row(mojo_rows))
    print()
    funnel_rows = c.get_range(f"'{TAB_FUNNEL}'!A2:G1000")
    tail_funnel = funnel_rows[-12:] if funnel_rows and len(funnel_rows) > 12 else funnel_rows
    print(f"{TAB_FUNNEL} last up to 12 rows:", tail_funnel)
    print()
    reg = c.get_range(f"'{TAB_QUERY_REGISTRY}'!A2:F12")
    print(f"{TAB_QUERY_REGISTRY} sample rows:", reg[:3] if reg else reg)
    print()
    log = c.get_range("'Run log'!A1:G2000")
    tail = log[-12:] if log and len(log) > 12 else log
    print("Run log (last up to 12 rows):", tail)


if __name__ == "__main__":
    main()
