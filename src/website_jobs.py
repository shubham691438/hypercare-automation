"""Backward-compatible re-export.  New code should import from ats.base directly."""

from ats.base import WebsiteJobsConfigError, fetch_website_open_jobs

__all__ = ["WebsiteJobsConfigError", "fetch_website_open_jobs"]
