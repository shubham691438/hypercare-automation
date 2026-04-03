"""Helpers to fetch metrics from Mojo instead of querying Snowflake directly.

This uses the same endpoint as your curl:
- URL:  {MOJO_BASE_URL}{MOJO_JOBS_PATH}
- Auth: short‑lived access token + headers from env.

We DO NOT hardcode tokens. Put credentials in .env (gitignored).
"""

from __future__ import annotations

import datetime as _dt
import json
import os
from typing import Any, Dict, List, Tuple

import requests


class MojoConfigError(RuntimeError):
    pass


def _env_required(name: str) -> str:
    v = os.environ.get(name)
    if not v or not v.strip():
        raise MojoConfigError(f"Missing environment variable {name}")
    return v.strip()


def _mojo_query_date() -> str:
    """Use today for the Mojo API — it returns live open-job counts regardless of date.

    HYPERCARE_REPORT_DATE controls which DB rows to query, but the Mojo job-list
    endpoint needs today's date to return the current live count correctly.
    """
    raw = (os.environ.get("MOJO_QUERY_DATE") or "").strip()
    if raw:
        return raw
    return _dt.date.today().isoformat()


_MOJO_BASE_URL = "https://mojopro.joveo.com"


def _mojo_common_config() -> Dict[str, str]:
    base = os.environ.get("MOJO_BASE_URL", _MOJO_BASE_URL).strip() or _MOJO_BASE_URL
    access_token = _env_required("MOJO_ACCESS_TOKEN")
    account_id = _env_required("MOJO_ACCOUNT_ID")
    agency_id = _env_required("MOJO_AGENCY_ID")
    client_id = _env_required("MOJO_CLIENT_ID")
    email = _env_required("MOJO_EMAIL")
    username = os.environ.get("MOJO_USERNAME", email).strip() or email
    return {
        "base": base,
        "access_token": access_token,
        "account_id": account_id,
        "agency_id": agency_id,
        "client_id": client_id,
        "email": email,
        "username": username,
    }


def _build_headers(
    *,
    account_id: str,
    agency_id: str,
    email: str,
    username: str,
    access_token: str,
    referer: str | None = None,
) -> Dict[str, str]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": os.environ.get(
            "MOJO_USER_AGENT",
            "hypercare-automation/0.1 (+python-requests)",
        ),
        "accountId": account_id,
        "MojoAgencyId": agency_id,
        "email": email,
        "X-MOJO-USERNAME": username,
        "accessToken": access_token,
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _build_request() -> Tuple[str, Dict[str, str], Dict[str, Any]]:
    cfg = _mojo_common_config()
    path = _env_required("MOJO_JOBS_PATH")
    url = cfg["base"].rstrip("/") + "/" + path.lstrip("/")

    headers = _build_headers(
        account_id=cfg["account_id"],
        agency_id=cfg["agency_id"],
        email=cfg["email"],
        username=cfg["username"],
        access_token=cfg["access_token"],
    )

    report_date = _mojo_query_date()
    body = {
        "legacyFilters": {
            "rules": [
                {"operator": "IN", "field": "status", "data": ["A"]},
                {"operator": "IN", "field": "clientId", "data": [cfg["client_id"]]},
            ]
        },
        "pageOptions": {"page": 1, "limit": 100},
        # Sort by most recently started job so data[0] gives the latest startDate
        "sortOptions": [{"sortOrder": "DESCENDING", "field": "startDate"}],
        "startDate": report_date,
        "endDate": report_date,
        "projectionFields": [],
        "currencyParams": {"baseCurrency": "USD", "targetCurrency": None},
        "tthIdentifiers": {},
    }

    return url, headers, body


def _build_publishers_request(
    report_date: str,
    *,
    tth_identifiers: Dict[str, str] | None = None,
) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
    """Build the Mojo publishers summary request used for Sponsored Applies on Mojo."""
    cfg = _mojo_common_config()
    url = (
        cfg["base"].rstrip("/")
        + f"/fna-dashboard/v1/agencies/{cfg['agency_id']}/publishers?pageType=CLIENT_PUBLISHER"
    )
    referer = (
        cfg["base"].rstrip("/")
        + f"/{cfg['account_id']}/clients/{cfg['client_id']}/placements"
    )
    headers = _build_headers(
        account_id=cfg["account_id"],
        agency_id=cfg["agency_id"],
        email=cfg["email"],
        username=cfg["username"],
        access_token=cfg["access_token"],
        referer=referer,
    )
    body = {
        "legacyFilters": {
            "rules": [
                {"operator": "IN", "field": "clientId", "data": [cfg["client_id"]]},
            ]
        },
        "pageOptions": {"page": 1, "limit": 20},
        "sortOptions": [{"sortOrder": "DESCENDING", "field": "clicks"}],
        "startDate": report_date,
        "endDate": report_date,
        "projectionFields": [],
        "currencyParams": {"baseCurrency": "USD", "targetCurrency": None},
        "tthIdentifiers": tth_identifiers or {},
    }
    return url, headers, body


def _build_funnel_setup_request(report_date: str) -> Tuple[str, Dict[str, str]]:
    """Build the Mojo funnel-setup request used to discover active stage mappings."""
    cfg = _mojo_common_config()
    url = (
        cfg["base"].rstrip("/")
        + f"/funnel-tracking/v1/{cfg['client_id']}/setup?from={report_date}&to={report_date}"
    )
    referer = (
        cfg["base"].rstrip("/")
        + f"/{cfg['account_id']}/clients/{cfg['client_id']}/placements"
    )
    headers = _build_headers(
        account_id=cfg["account_id"],
        agency_id=cfg["agency_id"],
        email=cfg["email"],
        username=cfg["username"],
        access_token=cfg["access_token"],
        referer=referer,
    )
    return url, headers


def _extract_jobs(payload: Any) -> List[Dict[str, Any]]:
    """Try to find the jobs array without hardcoding response shape."""
    if isinstance(payload, dict):
        # Common patterns: data.jobs, jobs, items, results, etc.
        for key in ("jobs", "items", "results"):
            if isinstance(payload.get(key), list):
                return [x for x in payload[key] if isinstance(x, dict)]
        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("jobs", "items", "results"):
                if isinstance(data.get(key), list):
                    return [x for x in data[key] if isinstance(x, dict)]
        # Fallback: first list-of-dicts value
        for value in payload.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return [x for x in value if isinstance(x, dict)]
    return []


def _extract_updated_at(job: Dict[str, Any]) -> str | None:
    """Return an ISO datetime string from the job dict, if present."""
    # Prefer explicit fields
    for key in ("jobUpdatedDate", "updatedAt", "updated_at", "lastUpdated"):
        v = job.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # Try any key containing both 'update' and 'date' or 'time'
    for key, v in job.items():
        if not isinstance(v, str) or not v.strip():
            continue
        lk = key.lower()
        if "update" in lk and ("date" in lk or "time" in lk):
            return v.strip()
    return None


def _parse_iso_to_utc(dt_str: str) -> _dt.datetime | None:
    try:
        # Handle trailing Z and no timezone
        if dt_str.endswith("Z"):
            return _dt.datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(
                _dt.timezone.utc
            )
        dt = _dt.datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=_dt.timezone.utc)
        return dt.astimezone(_dt.timezone.utc)
    except Exception:
        return None


def _utc_to_ist(dt_utc: _dt.datetime) -> _dt.datetime:
    ist_offset = _dt.timedelta(hours=5, minutes=30)
    return dt_utc.astimezone(_dt.timezone.utc) + ist_offset


def fetch_open_jobs_metrics() -> Tuple[int, str]:
    """Return (open_jobs_count, latest_job_updated_ist_str).

    open_jobs_count prefers `summary.totalCount` from the API response when present,
    otherwise falls back to len(jobs).
    """
    url, headers, body = _build_request()
    timeout = float(os.environ.get("MOJO_HTTP_TIMEOUT", "20"))

    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"Mojo API {resp.status_code}: {resp.text[:400]}")

    payload = resp.json()

    open_count = 0
    if isinstance(payload, dict):
        summary = payload.get("summary") or {}
        if isinstance(summary, dict) and isinstance(summary.get("totalCount"), (int, float)):
            open_count = int(summary["totalCount"])

    jobs = _extract_jobs(payload)
    if open_count <= 0:
        open_count = len(jobs)

    # Use the most recent startDate from the first job (sorted DESC by startDate).
    # The Mojo API does not expose an updatedAt field, so startDate is the best proxy.
    latest_date_str = ""
    if jobs:
        first_job = jobs[0]
        raw = first_job.get("startDate", "")
        if raw and isinstance(raw, str):
            # startDate is YYYY-MM-DD — no time component, so no UTC→IST offset needed;
            # just represent it as a date string in IST context.
            latest_date_str = raw.strip()

    return open_count, latest_date_str


def fetch_sponsored_applies(report_date: str) -> int:
    """Return sponsored applies from Mojo publishers summary for a single report date."""
    url, headers, body = _build_publishers_request(report_date)
    timeout = float(os.environ.get("MOJO_HTTP_TIMEOUT", "20"))

    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"Mojo API {resp.status_code}: {resp.text[:400]}")

    payload = resp.json()
    if isinstance(payload, dict):
        outer_summary = payload.get("summary") or {}
        if isinstance(outer_summary, dict):
            inner_summary = outer_summary.get("summary") or {}
            if isinstance(inner_summary, dict) and isinstance(
                inner_summary.get("applies"), (int, float)
            ):
                return int(inner_summary["applies"])

    raise RuntimeError("Mojo publishers response missing summary.summary.applies")


def fetch_sponsored_stage_counts(report_date: str) -> List[Dict[str, Any]]:
    """Return active Mojo funnel stages, mappings, and counts for the given date.

    The setup endpoint returns active stage mappings with an `order` value.
    We translate that order to the publishers API's `tthN` keys, then read the
    counts from `summary.summary.tthStats`.
    """
    timeout = float(os.environ.get("MOJO_HTTP_TIMEOUT", "20"))

    setup_url, setup_headers = _build_funnel_setup_request(report_date)
    setup_resp = requests.get(setup_url, headers=setup_headers, timeout=timeout)
    if setup_resp.status_code >= 400:
        raise RuntimeError(f"Mojo funnel setup API {setup_resp.status_code}: {setup_resp.text[:400]}")

    payload = setup_resp.json()
    raw_mapping = ((payload.get("data") or {}).get("mapping") or []) if isinstance(payload, dict) else []
    if not isinstance(raw_mapping, list):
        raise RuntimeError("Mojo funnel setup response missing data.mapping")

    ordered_stages: List[Dict[str, Any]] = []
    for item in raw_mapping:
        if not isinstance(item, dict):
            continue
        if item.get("active") is False:
            continue
        label = str(item.get("label") or "").strip()
        key = str(item.get("key") or "").strip().upper()
        mapping = str(item.get("mapping") or "").strip()
        order_raw = item.get("order")
        if not label:
            continue
        if not isinstance(order_raw, (int, float)) or int(order_raw) <= 0:
            continue
        stage_key = key or label.upper().replace(" ", "_")
        ordered_stages.append(
            {
                "order": int(order_raw),
                "label": label,
                "key": stage_key,
                "mapping": mapping,
            }
        )

    ordered_stages.sort(key=lambda stage: int(stage["order"]))
    if not ordered_stages:
        raise RuntimeError("Mojo funnel setup returned no active ordered stages")

    tth_identifiers = {
        f"tth{int(stage['order'])}": str(stage["label"])
        for stage in ordered_stages
    }
    url, headers, body = _build_publishers_request(
        report_date,
        tth_identifiers=tth_identifiers,
    )
    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"Mojo publishers API {resp.status_code}: {resp.text[:400]}")

    pub_payload = resp.json()
    inner_summary = ((pub_payload.get("summary") or {}).get("summary") or {}) if isinstance(pub_payload, dict) else {}
    tth_stats = inner_summary.get("tthStats") or {}
    if not isinstance(tth_stats, dict):
        raise RuntimeError("Mojo publishers response missing summary.summary.tthStats")

    result: List[Dict[str, Any]] = []
    for stage in ordered_stages:
        order = int(stage["order"])
        raw = tth_stats.get(f"tth{order}", 0)
        if isinstance(raw, (int, float)):
            count = int(raw)
        else:
            try:
                count = int(str(raw).strip())
            except Exception:
                count = 0
        result.append(
            {
                "order": order,
                "label": stage["label"],
                "key": stage["key"],
                "mapping": stage["mapping"],
                "count": count,
            }
        )

    return result

