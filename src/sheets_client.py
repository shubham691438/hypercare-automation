"""Thin wrapper around Google Sheets API v4 for read/update/append."""

from __future__ import annotations

import os
from typing import Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from client_config import resolve_spreadsheet_id
from google_sheets_auth import get_sheets_credentials


def default_spreadsheet_id() -> str:
    return resolve_spreadsheet_id()


class SheetsClient:
    def __init__(self, spreadsheet_id: str | None = None):
        self.spreadsheet_id = spreadsheet_id or default_spreadsheet_id()
        creds = get_sheets_credentials()
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self._spreadsheets = self._service.spreadsheets()
        self._values = self._spreadsheets.values()

    def get_spreadsheet(self, fields: str | None = None) -> dict[str, Any]:
        """Fetch spreadsheet metadata. Default fields include sheet ids and titles."""
        f = fields or "properties.title,sheets(properties(sheetId,title,index,hidden))"
        try:
            return self._spreadsheets.get(
                spreadsheetId=self.spreadsheet_id,
                fields=f,
            ).execute()
        except HttpError as e:
            raise RuntimeError(f"Sheets API get spreadsheet failed: {e}") from e

    def list_tabs(self) -> list[dict[str, Any]]:
        """Return sheet properties: sheetId, title, index, hidden."""
        meta = self.get_spreadsheet()
        out: list[dict[str, Any]] = []
        for s in meta.get("sheets", []):
            props = s.get("properties") or {}
            out.append(
                {
                    "sheetId": props.get("sheetId"),
                    "title": props.get("title"),
                    "index": props.get("index"),
                    "hidden": props.get("hidden", False),
                }
            )
        return out

    def batch_update(self, requests: list[dict[str, Any]]) -> dict[str, Any]:
        try:
            return (
                self._spreadsheets.batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": requests},
                ).execute()
            )
        except HttpError as e:
            raise RuntimeError(f"Sheets API batchUpdate failed: {e}") from e

    def duplicate_sheet(self, source_sheet_id: int, new_title: str) -> dict[str, Any]:
        """Clone a tab; returns batchUpdate response (includes new sheet id in replies)."""
        return self.batch_update(
            [
                {
                    "duplicateSheet": {
                        "sourceSheetId": source_sheet_id,
                        "newSheetName": new_title,
                    }
                }
            ]
        )

    def get_range(self, a1_range: str) -> list[list[Any]]:
        """Read a range in A1 notation, e.g. 'Sheet1!A1:D10'."""
        try:
            result = (
                self._values.get(
                    spreadsheetId=self.spreadsheet_id,
                    range=a1_range,
                ).execute()
            )
        except HttpError as e:
            raise RuntimeError(f"Sheets API get failed: {e}") from e
        return result.get("values", [])

    def update_range(
        self,
        a1_range: str,
        values: list[list[Any]],
        value_input_option: str = "USER_ENTERED",
    ) -> dict[str, Any]:
        """Overwrite cells for the given range. values must match range shape."""
        body = {"values": values}
        try:
            return (
                self._values.update(
                    spreadsheetId=self.spreadsheet_id,
                    range=a1_range,
                    valueInputOption=value_input_option,
                    body=body,
                ).execute()
            )
        except HttpError as e:
            raise RuntimeError(f"Sheets API update failed: {e}") from e

    def append_rows(
        self,
        a1_range: str,
        values: list[list[Any]],
        value_input_option: str = "USER_ENTERED",
        insert_data_option: str = "INSERT_ROWS",
    ) -> dict[str, Any]:
        """Append rows after the table described by a1_range (e.g. 'Sheet1!A1')."""
        body = {"values": values}
        try:
            return (
                self._values.append(
                    spreadsheetId=self.spreadsheet_id,
                    range=a1_range,
                    valueInputOption=value_input_option,
                    insertDataOption=insert_data_option,
                    body=body,
                ).execute()
            )
        except HttpError as e:
            raise RuntimeError(f"Sheets API append failed: {e}") from e

    def get_sheet_conditional_formats(self, sheet_id: int) -> list[dict[str, Any]]:
        """Return the list of conditionalFormats for the given sheetId (may be empty)."""
        try:
            result = self._spreadsheets.get(
                spreadsheetId=self.spreadsheet_id,
                fields="sheets(properties(sheetId),conditionalFormats)",
            ).execute()
        except HttpError as e:
            raise RuntimeError(f"Sheets API get failed: {e}") from e
        for sheet in result.get("sheets", []):
            if sheet.get("properties", {}).get("sheetId") == sheet_id:
                return sheet.get("conditionalFormats", [])
        return []

    def get_sheet_tables(self, sheet_id: int) -> list[dict[str, Any]]:
        """Return the list of native Google Sheets tables for the given sheetId."""
        try:
            result = self._spreadsheets.get(
                spreadsheetId=self.spreadsheet_id,
                fields="sheets(properties(sheetId),tables)",
            ).execute()
        except HttpError as e:
            raise RuntimeError(f"Sheets API get failed: {e}") from e
        for sheet in result.get("sheets", []):
            if sheet.get("properties", {}).get("sheetId") == sheet_id:
                return sheet.get("tables", [])
        return []

    def batch_get(self, ranges: list[str]) -> list[dict[str, Any]]:
        """Read multiple ranges in one call."""
        try:
            result = (
                self._values.batchGet(
                    spreadsheetId=self.spreadsheet_id,
                    ranges=ranges,
                ).execute()
            )
        except HttpError as e:
            raise RuntimeError(f"Sheets API batchGet failed: {e}") from e
        return result.get("valueRanges", [])
