#!/usr/bin/env python3
"""Run the daily hypercare pipeline for every active client config."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from client_config import hypercare_status, list_client_ids  # noqa: E402


def main() -> int:
    client_ids = list_client_ids()
    if not client_ids:
        print("No client configs found under config/clients.")
        return 0

    active_clients: list[str] = []
    skipped_clients: list[str] = []
    for client_id in client_ids:
        status = hypercare_status(client_id)
        if status["is_active"] or status["status"] == "missing_go_live_date":
            active_clients.append(client_id)
        else:
            skipped_clients.append(
                f"{client_id} ({status['status']}, go_live_date={status['go_live_date']})"
            )

    if skipped_clients:
        print("Skipping inactive clients:")
        for item in skipped_clients:
            print(f"  - {item}")

    if not active_clients:
        print("No active hypercare clients to run.")
        return 0

    failures = 0
    for client_id in active_clients:
        print(f"\n=== Running {client_id} ===")
        env = os.environ.copy()
        env["HYPERCARE_CLIENT"] = client_id
        result = subprocess.run(
            [sys.executable, "scripts/run_hypercare_queries.py"],
            cwd=str(ROOT),
            env=env,
            check=False,
        )
        if result.returncode != 0:
            failures += 1
            print(f"Client {client_id} failed with exit code {result.returncode}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
