"""Load and validate per-client configuration from ``config/clients/<id>.json``.

The active client is controlled by:
  HYPERCARE_CLIENT=ashleyfurniture   (or any key matching a file in config/clients/)

GOOGLE_SHEETS_SPREADSHEET_ID in .env overrides the spreadsheet_id from the JSON file.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict

TRACKER_JOB_INGESTION = "job_ingestion"
TRACKER_MOJO_APPLY = "mojo_apply"
TRACKER_FUNNEL_TRACKING = "funnel_tracking"
VALID_TRACKERS = {
    TRACKER_JOB_INGESTION,
    TRACKER_MOJO_APPLY,
    TRACKER_FUNNEL_TRACKING,
}
DEFAULT_HYPERCARE_DAYS = 10


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _active_client_id() -> str:
    return (os.environ.get("HYPERCARE_CLIENT") or "").strip()


def _config_dir() -> Path:
    return _repo_root() / "config" / "clients"


def _require_string(
    data: Dict[str, Any],
    key: str,
    *,
    context: str,
    allow_blank: bool = False,
) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{context}.{key} must be a string")
    text = value.strip()
    if not allow_blank and not text:
        raise ValueError(f"{context}.{key} cannot be blank")
    return text


def _optional_string(
    data: Dict[str, Any],
    key: str,
    *,
    context: str,
) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{context}.{key} must be a string when provided")
    text = value.strip()
    return text or None


def _normalize_enabled_tabs(raw: Any) -> list[str]:
    if raw is None:
        return sorted(VALID_TRACKERS)
    if not isinstance(raw, list) or not raw:
        raise ValueError("enabled_tabs must be a non-empty list")

    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            raise ValueError("enabled_tabs entries must be strings")
        tracker = item.strip().lower()
        if tracker not in VALID_TRACKERS:
            allowed = ", ".join(sorted(VALID_TRACKERS))
            raise ValueError(f"Unsupported enabled_tabs value {item!r}; expected one of: {allowed}")
        if tracker not in seen:
            normalized.append(tracker)
            seen.add(tracker)
    return normalized


def _normalize_go_live_date(raw: Any) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("go_live_date must be a YYYY-MM-DD string when provided")
    text = raw.strip()
    if not text:
        return None
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("go_live_date must use YYYY-MM-DD format") from exc
    return text


def _normalize_hypercare_days(raw: Any) -> int:
    if raw is None:
        return DEFAULT_HYPERCARE_DAYS
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError("hypercare_days must be an integer")
    if raw <= 0:
        raise ValueError("hypercare_days must be >= 1")
    return raw


def _validate_client_config(data: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(data)

    cfg["id"] = _require_string(cfg, "id", context="client")
    cfg["name"] = _require_string(cfg, "name", context="client")
    cfg["spreadsheet_id"] = _require_string(
        cfg,
        "spreadsheet_id",
        context="client",
        allow_blank=True,
    )
    cfg["go_live_date"] = _normalize_go_live_date(cfg.get("go_live_date"))
    cfg["hypercare_days"] = _normalize_hypercare_days(cfg.get("hypercare_days"))
    cfg["enabled_tabs"] = _normalize_enabled_tabs(cfg.get("enabled_tabs"))
    owner = _optional_string(cfg, "owner", context="client")
    if owner is not None:
        cfg["owner"] = owner
    contacts = cfg.get("client_contacts")
    if contacts is None:
        cfg["client_contacts"] = []
    elif not isinstance(contacts, list) or any(not isinstance(item, str) for item in contacts):
        raise ValueError("client_contacts must be a list of strings when provided")
    else:
        cfg["client_contacts"] = [item.strip() for item in contacts if item.strip()]

    ats = cfg.get("ats")
    if not isinstance(ats, dict):
        raise ValueError("client.ats must be an object")
    ats_norm = dict(ats)
    ats_norm["provider"] = _require_string(ats_norm, "provider", context="client.ats")
    ats_norm["url"] = _require_string(ats_norm, "url", context="client.ats")
    cfg["ats"] = ats_norm

    db = cfg.get("db")
    if not isinstance(db, dict):
        raise ValueError("client.db must be an object")
    unified = db.get("unified")
    tao = db.get("tao")
    if not isinstance(unified, dict):
        raise ValueError("client.db.unified must be an object")
    if not isinstance(tao, dict):
        raise ValueError("client.db.tao must be an object")
    cfg["db"] = {
        "unified": {
            "customer_id": _require_string(
                unified, "customer_id", context="client.db.unified"
            )
        },
        "tao": {
            "source_id": _require_string(tao, "source_id", context="client.db.tao"),
            "client_id": _require_string(tao, "client_id", context="client.db.tao"),
        },
    }

    if VALID_TRACKERS.intersection(cfg["enabled_tabs"]):
        mojo = cfg.get("mojo")
        if not isinstance(mojo, dict):
            raise ValueError("client.mojo must be an object when hypercare trackers use Mojo")
        cfg["mojo"] = {
            "account_id": _require_string(mojo, "account_id", context="client.mojo"),
            "agency_id": _require_string(mojo, "agency_id", context="client.mojo"),
            "client_id": _require_string(mojo, "client_id", context="client.mojo"),
            "jobs_path": _require_string(mojo, "jobs_path", context="client.mojo"),
        }

    return cfg


def list_client_ids() -> list[str]:
    """Return all per-file client ids under ``config/clients``."""
    out: list[str] = []
    for path in sorted(_config_dir().glob("*.json")):
        if path.name == "example.json":
            continue
        out.append(path.stem)
    return out


def load_client_config(client_id: str | None = None) -> Dict[str, Any]:
    """Load and return the full config dict for the given client.

    If client_id is None, falls back to HYPERCARE_CLIENT env var.
    Raises FileNotFoundError if no matching config file is found.
    """
    cid = (client_id or _active_client_id()).strip()
    if not cid:
        raise ValueError(
            "No client configured. Set HYPERCARE_CLIENT=<id> in .env "
            "or pass client_id explicitly."
        )

    # Try config/clients/<id>.json first (new per-file structure)
    per_file = _repo_root() / "config" / "clients" / f"{cid}.json"
    if per_file.is_file():
        data = _validate_client_config(json.loads(per_file.read_text(encoding="utf-8")))
        # Spreadsheet ID env override
        explicit_sid = (os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID") or "").strip()
        if explicit_sid:
            data["spreadsheet_id"] = explicit_sid
        return data

    # Legacy fallback: config/clients.json flat file
    flat = _repo_root() / "config" / "clients.json"
    if flat.is_file():
        blob = json.loads(flat.read_text(encoding="utf-8"))
        for entry in blob.get("clients", []):
            if entry.get("id") == cid:
                entry = _validate_client_config(entry)
                explicit_sid = (os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID") or "").strip()
                if explicit_sid:
                    entry["spreadsheet_id"] = explicit_sid
                return entry

    raise FileNotFoundError(
        f"No client config found for {cid!r}. "
        f"Create config/clients/{cid}.json following config/clients/example.json."
    )


def resolve_spreadsheet_id() -> str:
    """Return the Google Sheets spreadsheet ID for the active client.

    Priority:
      1. GOOGLE_SHEETS_SPREADSHEET_ID env var (explicit override)
      2. spreadsheet_id from config/clients/<HYPERCARE_CLIENT>.json
    """
    explicit = (os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID") or "").strip()
    if explicit:
        return explicit

    cid = _active_client_id()
    if not cid:
        # No client configured — return the hardcoded fallback used during initial setup
        return "1PghVTvx3FR1WGgqtQTcn0RX0TctlZvb5WX5fmQAdisI"

    cfg = load_client_config(cid)
    sid = (cfg.get("spreadsheet_id") or "").strip()
    if not sid:
        raise ValueError(f"No spreadsheet_id in config for client {cid!r}")
    return sid


def enabled_tabs(client_id: str | None = None) -> list[str]:
    """Return the enabled tracker tabs for the requested client."""
    cfg = load_client_config(client_id)
    return list(cfg.get("enabled_tabs") or [])


def hypercare_status(
    client_id: str | None = None,
    *,
    today: date | None = None,
) -> Dict[str, Any]:
    """Return lifecycle metadata for the configured client."""
    cfg = load_client_config(client_id)
    ref = today or date.today()
    go_live_text = cfg.get("go_live_date")
    if not go_live_text:
        return {
            "go_live_date": None,
            "hypercare_days": int(cfg["hypercare_days"]),
            "days_since_go_live": None,
            "day_number": None,
            "is_active": True,
            "status": "missing_go_live_date",
        }

    go_live = date.fromisoformat(go_live_text)
    days_since = (ref - go_live).days
    active = 0 <= days_since < int(cfg["hypercare_days"])
    if days_since < 0:
        status = "not_started"
    elif active:
        status = "active"
    else:
        status = "completed"
    return {
        "go_live_date": go_live_text,
        "hypercare_days": int(cfg["hypercare_days"]),
        "days_since_go_live": days_since,
        "day_number": days_since + 1 if days_since >= 0 else None,
        "is_active": active,
        "status": status,
    }
