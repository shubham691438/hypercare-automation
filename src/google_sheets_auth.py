"""Credential helpers for Google Sheets API.

Order of resolution:
1. Service account: set GOOGLE_APPLICATION_CREDENTIALS to a JSON key file path.
2. OAuth desktop: place OAuth client JSON at OAUTH_CREDENTIALS_PATH (default
   credentials.json in cwd); first run opens a browser; token saved for reuse.

GCP setup (one-time):
- Enable "Google Sheets API" for your project.
- Service account: IAM → Service accounts → create → Keys → JSON. Share the
  spreadsheet with the `client_email` from that JSON as Editor.
- OAuth: APIs & Services → Credentials → Create OAuth client ID → Desktop app.
  Download JSON as credentials.json.
"""

from __future__ import annotations

import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.exceptions import DefaultCredentialsError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _service_account_credentials():
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path or not Path(path).is_file():
        return None
    return service_account.Credentials.from_service_account_file(path, scopes=SCOPES)


def _oauth_user_credentials():
    cred_path = os.environ.get("OAUTH_CREDENTIALS_PATH", "credentials.json")
    token_path = os.environ.get("OAUTH_TOKEN_PATH", "token.json")
    p = Path(cred_path)
    if not p.is_file():
        return None

    creds = None
    if Path(token_path).is_file():
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        Path(token_path).write_text(creds.to_json(), encoding="utf-8")
        return creds

    flow = InstalledAppFlow.from_client_secrets_file(str(p), SCOPES)
    creds = flow.run_local_server(port=0)
    Path(token_path).write_text(creds.to_json(), encoding="utf-8")
    return creds


def get_sheets_credentials():
    sa = _service_account_credentials()
    if sa is not None:
        return sa
    oauth = _oauth_user_credentials()
    if oauth is not None:
        return oauth
    raise DefaultCredentialsError(
        "No Google Sheets credentials. Either set GOOGLE_APPLICATION_CREDENTIALS "
        "to a service-account JSON file, or add OAuth credentials.json and set "
        "OAUTH_CREDENTIALS_PATH if not using the default name."
    )
