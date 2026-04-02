"""UKG Pro (UltiPro) external job board — open-jobs counter.

Per-client config (only the URL) lives in config/ats/ukg/<client_key>.json.
The request body is identical for all UKG clients and is hardcoded here.
No authentication headers are required — the public job board API is open.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

import requests


class UKGConfigError(RuntimeError):
    pass


# Same request body for every UKG client — only the URL differs per client.
_SEARCH_BODY: Dict[str, Any] = {
    "opportunitySearch": {
        "Top": 50,
        "Skip": 0,
        "QueryString": "",
        "OrderBy": [{"Value": "postedDateDesc", "PropertyName": "PostedDate", "Ascending": False}],
        "Filters": [
            {"t": "TermsSearchFilterDto", "fieldName": 4, "extra": None, "values": []},
            {"t": "TermsSearchFilterDto", "fieldName": 5, "extra": None, "values": []},
            {"t": "TermsSearchFilterDto", "fieldName": 6, "extra": None, "values": []},
            {"t": "TermsSearchFilterDto", "fieldName": 37, "extra": None, "values": []},
        ],
    },
    "matchCriteria": {
        "PreferredJobs": [],
        "Educations": [],
        "LicenseAndCertifications": [],
        "Skills": [],
        "hasNoLicenses": False,
        "SkippedSkills": [],
    },
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_url(client_key: str) -> str:
    path = _repo_root() / "config" / "ats" / "ukg" / f"{client_key}.json"
    if not path.is_file():
        raise UKGConfigError(
            f"UKG config not found: {path}. "
            f"Create it following config/ats/ukg/example.json."
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    url = data.get("url", "").strip()
    if not url:
        raise UKGConfigError(f"Missing 'url' in {path}")
    return url


def fetch_open_jobs(client_key: str) -> int:
    """Return count of open jobs from the UKG public job board."""
    url = _load_url(client_key)
    timeout = float(os.environ.get("UKG_HTTP_TIMEOUT", "20"))

    resp = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json=_SEARCH_BODY,
        timeout=timeout,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"UKG {client_key} API {resp.status_code}: {resp.text[:400]}")

    payload = resp.json()
    total = payload.get("totalCount") if isinstance(payload, dict) else None
    if isinstance(total, (int, float)):
        return int(total)

    opportunities = payload.get("opportunities") if isinstance(payload, dict) else None
    if isinstance(opportunities, list):
        return len(opportunities)

    raise RuntimeError(f"UKG {client_key} response missing totalCount/opportunities")
