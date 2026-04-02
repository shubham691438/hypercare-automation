"""ATS website open-jobs dispatcher.

Reads ATS_PROVIDER and ATS_CLIENT from env (or accepts them directly), then
delegates to the provider-specific implementation in src/ats/<provider>/jobs.py.

To add a new ATS provider:
  1. Create  src/ats/<provider>/__init__.py
  2. Create  src/ats/<provider>/jobs.py  with a  fetch_open_jobs(client_key: str) -> int
  3. Create  config/ats/<provider>/<client_key>.json  with non-secret request config
  4. Add the routing branch below in _dispatch()
  5. Document new env vars in env.example
"""

from __future__ import annotations

import os


class WebsiteJobsConfigError(RuntimeError):
    pass


def fetch_website_open_jobs(
    provider: str | None = None,
    client_key: str | None = None,
) -> int:
    """Return count of open jobs from the ATS website.

    Parameters default to ATS_PROVIDER and ATS_CLIENT env vars.
    Raises WebsiteJobsConfigError when config is missing so the caller can skip
    the API call and leave the sheet cell for manual entry.
    """
    provider = (provider or os.environ.get("ATS_PROVIDER") or "").strip().lower()
    client_key = (client_key or os.environ.get("ATS_CLIENT") or "").strip().lower()

    if not provider or not client_key:
        raise WebsiteJobsConfigError(
            "ATS_PROVIDER and ATS_CLIENT must be set (or passed explicitly)"
        )

    return _dispatch(provider, client_key)


def _dispatch(provider: str, client_key: str) -> int:
    if provider == "ukg":
        from ats.ukg.jobs import UKGConfigError, fetch_open_jobs

        try:
            return fetch_open_jobs(client_key)
        except UKGConfigError as e:
            raise WebsiteJobsConfigError(str(e)) from e  # missing config/ats/ukg/<client>.json

    raise WebsiteJobsConfigError(
        f"No website open-jobs implementation for provider={provider!r}. "
        f"Add one under src/ats/{provider}/jobs.py."
    )
