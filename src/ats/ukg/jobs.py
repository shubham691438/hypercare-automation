"""UKG Pro (UltiPro) — open-jobs counter.

The job-board URL lives in config/clients/<id>.json → ats.url and is
loaded into the ATS_URL env var by set_client_runtime_defaults() before
this module is called.

No authentication is required — the UKG public job board API is open.
The request body is identical for all UKG clients and is hardcoded here.
"""

from __future__ import annotations

import os
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


def _get_url() -> str:
    url = os.environ.get("ATS_URL", "").strip()
    if not url:
        raise UKGConfigError(
            "ATS_URL is not set. Add 'url' under the 'ats' key in config/clients/<id>.json."
        )
    return url


def fetch_open_jobs() -> int:
    """Return count of open jobs from the UKG public job board."""
    url = _get_url()
    timeout = float(os.environ.get("UKG_HTTP_TIMEOUT", "20"))

    resp = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json=_SEARCH_BODY,
        timeout=timeout,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"UKG API {resp.status_code}: {resp.text[:400]}")

    payload = resp.json()
    total = payload.get("totalCount") if isinstance(payload, dict) else None
    if isinstance(total, (int, float)):
        return int(total)

    opportunities = payload.get("opportunities") if isinstance(payload, dict) else None
    if isinstance(opportunities, list):
        return len(opportunities)

    raise RuntimeError("UKG response missing totalCount/opportunities")


