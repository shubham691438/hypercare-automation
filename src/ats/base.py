"""ATS dispatcher — routes calls to the correct provider implementation.

ATS config lives entirely in config/clients/<id>.json → ats:
  - provider: which ATS (e.g. "ukg")
  - url:      job-board endpoint URL

set_client_runtime_defaults() in run_hypercare_queries.py loads these into
ATS_PROVIDER and ATS_URL env vars before any of these functions are called.

To add a new ATS provider:
  1. Create  src/ats/<provider>/__init__.py  (empty)
  2. Create  src/ats/<provider>/jobs.py  with fetch_open_jobs() -> int
     and optionally fetch_job_details(), fetch_candidate_details(),
     fetch_application_details()
  3. Add 'url' to config/clients/<id>.json → ats.url
  4. Add the routing branch in _dispatch() below
"""

from __future__ import annotations

import os


class WebsiteJobsConfigError(RuntimeError):
    pass


def fetch_website_open_jobs(provider: str | None = None) -> int:
    """Return count of open jobs from the ATS website.

    provider defaults to the ATS_PROVIDER env var (set from client config).
    Raises WebsiteJobsConfigError when config is missing.
    """
    provider = (provider or os.environ.get("ATS_PROVIDER") or "").strip().lower()
    if not provider:
        raise WebsiteJobsConfigError(
            "ATS_PROVIDER is not set. Add 'provider' under 'ats' in config/clients/<id>.json."
        )
    return _dispatch(provider)


def _dispatch(provider: str) -> int:
    if provider == "ukg":
        from ats.ukg.jobs import UKGConfigError, fetch_open_jobs
        try:
            return fetch_open_jobs()
        except UKGConfigError as e:
            raise WebsiteJobsConfigError(str(e)) from e

    raise WebsiteJobsConfigError(
        f"No open-jobs implementation for provider={provider!r}. "
        f"Add one under src/ats/{provider}/jobs.py."
    )
