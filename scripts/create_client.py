#!/usr/bin/env python3
"""Scaffold a new hypercare client config from the repo templates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLIENT_TEMPLATE = ROOT / "config" / "clients" / "example.json"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create config/clients/<id>.json and config/ats/<provider>/<id>.json"
    )
    parser.add_argument("--client-id", required=True, help="Client slug, e.g. ashleyfurniture")
    parser.add_argument("--name", required=True, help="Human-readable client name")
    parser.add_argument("--provider", required=True, help="ATS provider key, e.g. ukg")
    parser.add_argument("--spreadsheet-id", default="", help="Google spreadsheet id")
    parser.add_argument("--go-live-date", default="", help="YYYY-MM-DD")
    parser.add_argument("--hypercare-days", default=10, type=int, help="Active hypercare window")
    parser.add_argument(
        "--enabled-tabs",
        default="job_ingestion,mojo_apply,funnel_tracking",
        help="Comma-separated tracker keys",
    )
    parser.add_argument("--owner", default="", help="Optional owner email")
    args = parser.parse_args()

    client_id = args.client_id.strip()
    if not client_id:
        raise SystemExit("--client-id cannot be blank")

    provider = args.provider.strip()
    client_path = ROOT / "config" / "clients" / f"{client_id}.json"
    ats_template = ROOT / "config" / "ats" / provider / "example.json"
    ats_path = ROOT / "config" / "ats" / provider / f"{client_id}.json"

    if client_path.exists() or ats_path.exists():
        raise SystemExit(f"Refusing to overwrite existing files for client {client_id!r}")
    if not ats_template.is_file():
        raise SystemExit(f"Missing ATS template: {ats_template}")

    client_payload = _load_json(CLIENT_TEMPLATE)
    client_payload["id"] = client_id
    client_payload["name"] = args.name.strip()
    client_payload["spreadsheet_id"] = args.spreadsheet_id.strip()
    client_payload["go_live_date"] = args.go_live_date.strip()
    client_payload["hypercare_days"] = int(args.hypercare_days)
    client_payload["enabled_tabs"] = [
        item.strip()
        for item in args.enabled_tabs.split(",")
        if item.strip()
    ]
    client_payload["owner"] = args.owner.strip()
    client_payload["client_contacts"] = []
    client_payload["ats"]["provider"] = provider
    client_payload["ats"]["client_key"] = client_id
    client_payload["mojo"]["jobs_path"] = client_payload["mojo"]["jobs_path"].replace(
        "<agency>", "replace-me"
    )

    ats_payload = _load_json(ats_template)

    _write_json(client_path, client_payload)
    _write_json(ats_path, ats_payload)

    print(f"Created {client_path.relative_to(ROOT)}")
    print(f"Created {ats_path.relative_to(ROOT)}")
    print("Next: fill in client IDs, ATS URL, go_live_date, and spreadsheet_id.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
