#!/usr/bin/env python3
"""Flask backend for Hypercare Automation UI.

Runs jobs asynchronously in background threads. Returns immediately with a
run_id so the frontend can poll for status updates.

  PYTHONPATH=src:scripts:web .venv/bin/python web/backend.py
"""

from __future__ import annotations

import os
import sys
import traceback
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Lock, Thread

from flask import Flask, jsonify, request
from flask_cors import CORS

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

app = Flask(__name__)
CORS(app)

# In-memory run log keyed by run_id, plus ordered list (newest first)
_runs: dict[str, dict] = {}
_run_order: list[str] = []
_lock = Lock()
_MAX_LOG = 100


def _load_base_env() -> None:
    env_path = _ROOT / ".env"
    if not env_path.is_file():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k, v)


def _apply_overrides(data: dict) -> None:
    mapping = {
        "mojo_client_id": "MOJO_CLIENT_ID",
        "google_sheet_id": "GOOGLE_SHEETS_SPREADSHEET_ID",
        "agency_id": "MOJO_AGENCY_ID",
        "account_id": "MOJO_ACCOUNT_ID",
        "mojo_email": "MOJO_EMAIL",
        "mojo_username": "MOJO_USERNAME",
        "mojo_access_token": "MOJO_ACCESS_TOKEN",
    }
    for key, env_var in mapping.items():
        val = (data.get(key) or "").strip()
        if val:
            os.environ[env_var] = val


def _upsert_run(run_id: str, data: dict) -> None:
    with _lock:
        _runs[run_id] = data
        if run_id not in _run_order:
            _run_order.insert(0, run_id)
        if len(_run_order) > _MAX_LOG:
            old = _run_order.pop()
            _runs.pop(old, None)


def _run_pipeline_bg(run_id: str, job_date: str, report_date: str, overrides: dict) -> None:
    """Background thread target — runs the pipeline and updates the run entry."""
    try:
        _apply_overrides(overrides)
        os.environ["HYPERCARE_JOB_DATE"] = job_date
        os.environ["HYPERCARE_REPORT_DATE"] = report_date

        import importlib
        import client_config
        import mojo_jobs
        import sheets_client as sc_mod

        importlib.reload(client_config)
        importlib.reload(mojo_jobs)
        importlib.reload(sc_mod)

        from client_config import load_client_config

        client_id = (os.environ.get("HYPERCARE_CLIENT") or "").strip()
        if not client_id:
            _upsert_run(run_id, {**_runs[run_id],
                "status": "error", "message": "HYPERCARE_CLIENT not set",
                "finished_at": datetime.now(timezone.utc).isoformat()})
            return

        client_cfg = load_client_config(client_id)

        import scripts_run as runner
        importlib.reload(runner)

        result = runner.execute_run(client_id, client_cfg, job_date, report_date)
        _upsert_run(run_id, {**_runs[run_id], **result,
            "finished_at": datetime.now(timezone.utc).isoformat()})

    except Exception as e:
        _upsert_run(run_id, {**_runs[run_id],
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()})


@app.route("/api/run", methods=["POST"])
def api_run():
    data = request.get_json(force=True) or {}

    raw_date = (data.get("run_date") or "").strip()
    if raw_date:
        job_date = raw_date
        try:
            d = date.fromisoformat(raw_date)
            report_date = (d - timedelta(days=1)).isoformat()
        except ValueError:
            return jsonify({"status": "error", "message": f"Invalid date: {raw_date}"}), 400
    else:
        job_date = date.today().isoformat()
        report_date = (date.today() - timedelta(days=1)).isoformat()

    run_id = str(uuid.uuid4())[:8]
    entry = {
        "run_id": run_id,
        "status": "running",
        "message": "Pipeline started...",
        "job_date": job_date,
        "report_date": report_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    }
    _upsert_run(run_id, entry)

    t = Thread(target=_run_pipeline_bg, args=(run_id, job_date, report_date, dict(data)), daemon=True)
    t.start()

    return jsonify(entry), 202


@app.route("/api/logs", methods=["GET"])
def api_logs():
    with _lock:
        return jsonify([_runs[rid] for rid in _run_order if rid in _runs])


@app.route("/api/run/<run_id>", methods=["GET"])
def api_run_status(run_id: str):
    with _lock:
        entry = _runs.get(run_id)
    if not entry:
        return jsonify({"error": "not found"}), 404
    return jsonify(entry)


@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify({"status": "ok"})


_load_base_env()

if __name__ == "__main__":
    print("Hypercare backend starting on http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False)
