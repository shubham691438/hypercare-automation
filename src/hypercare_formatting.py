"""Apply consistent column widths, freezes, header styles, and conditional formatting."""

from __future__ import annotations

from typing import Any

from hypercare_bootstrap import (
    TAB_FUNNEL,
    TAB_JOB,
    TAB_MOJO,
    TAB_OVERVIEW,
    TAB_QUERY_REGISTRY,
    TAB_RUN_LOG,
    TAB_WEBSITE,
    TABLE_MAX_ROWS,
)
from sheets_client import SheetsClient

_DATA_START_ROW = 1
_DATA_END_ROW = TABLE_MAX_ROWS

# Traffic-light thresholds (% of ATS count)
# Green : ≤10%  — expected sync lag
# Yellow: >10% and ≤25% — worth investigating
# Red   : >25%  — something is wrong
_THRESH_YELLOW = 0.10
_THRESH_RED = 0.25

# 0-based header row index to freeze below (freeze rows 1..header_row inclusive -> frozenRowCount = header_row + 1)
_SHEET_STYLE: dict[str, dict[str, Any]] = {
    TAB_OVERVIEW: {
        "frozen_row_count": 0,
        "header_rows": [],  # (start_row_idx, end_row_idx_exclusive) 0-based
        "column_pixels": [320, 520],
    },
    TAB_QUERY_REGISTRY: {
        "frozen_row_count": 1,
        "header_rows": [(0, 1)],
        "column_pixels": [140, 72, 200, 520, 160, 88],
    },
    TAB_JOB: {
        "frozen_row_count": 1,
        "header_rows": [],
        # A:Date B:ATS C:Unified D:Tao E:Mojo F:ΔATS−Mojo G:ΔUnified−Tao H:OPEN ts I:CLOSED ts J:Null map
        "column_pixels": [110, 170, 180, 150, 150, 140, 160, 250, 250, 210],
    },
    TAB_MOJO: {
        "frozen_row_count": 1,
        "header_rows": [],
        # A:Date  B:Sponsored Applies on Mojo  C:Sponsored Applies in Tao
        # D:Delta Mojo vs CRM (%)  E:Total Applies in CRM  F:CRM Creation Failed
        # G:ATS Rejected  H:ATS Rejected out of Total Applies in CRM (%)
        # I:Cum B  J:Cum C  K:Cum D  L:Cum E  M:Cum F  N:Cum G  O:Cum H
        "column_pixels": [110, 230, 220, 210, 190, 190, 170, 280, 230, 220, 210, 190, 190, 170, 280],
    },
    TAB_FUNNEL: {
        "frozen_row_count": 1,
        "header_rows": [],
        # A:Date  B:Mojo Stage  C:CRM Stage Mapping  D:CRM Count-All
        # E:CRM Count-Spons  F:Mojo Count-Spons  G:Delta
        "column_pixels": [110, 160, 500, 130, 160, 160, 200],
        "wrap_columns": [2],  # C: CRM Stage Mapping — contains comma-separated stage names
    },
    TAB_WEBSITE: {
        "frozen_row_count": 1,
        "header_rows": [(0, 1)],
        "column_pixels": [120, 480],
    },
    TAB_RUN_LOG: {
        "frozen_row_count": 1,
        "header_rows": [(0, 1)],
        "column_pixels": [160, 160, 72, 88, 420],
    },
}


def _sheet_id_map(client: SheetsClient) -> dict[str, int]:
    return {t["title"]: int(t["sheetId"]) for t in client.list_tabs()}


def apply_hypercare_formatting(client: SheetsClient) -> None:
    titles = _sheet_id_map(client)
    requests: list[dict[str, Any]] = []

    for title, cfg in _SHEET_STYLE.items():
        if title not in titles:
            continue
        sid = titles[title]

        fr = int(cfg["frozen_row_count"])
        if fr > 0:
            requests.append(
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sid,
                            "gridProperties": {"frozenRowCount": fr},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                }
            )

        pixels: list[int] = cfg["column_pixels"]
        for i, px in enumerate(pixels):
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sid,
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1,
                        },
                        "properties": {"pixelSize": px},
                        "fields": "pixelSize",
                    }
                }
            )

        for col_idx in cfg.get("wrap_columns", []):
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sid,
                            "startRowIndex": 1,
                            "endRowIndex": _DATA_END_ROW,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {"wrapStrategy": "WRAP"}
                        },
                        "fields": "userEnteredFormat.wrapStrategy",
                    }
                }
            )

        header_rows: list[tuple[int, int]] = cfg["header_rows"]
        for start, end in header_rows:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sid,
                            "startRowIndex": start,
                            "endRowIndex": end,
                            "startColumnIndex": 0,
                            "endColumnIndex": 40,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"bold": True},
                                "backgroundColor": {
                                    "red": 0.85,
                                    "green": 0.90,
                                    "blue": 0.98,
                                },
                                "wrapStrategy": "WRAP",
                                "verticalAlignment": "MIDDLE",
                            }
                        },
                        "fields": "userEnteredFormat(textFormat,backgroundColor,wrapStrategy,verticalAlignment)",
                    }
                }
            )

    if requests:
        client.batch_update(requests)

    if TAB_JOB in titles:
        _remove_obsolete_job_conditional_formats(client, titles[TAB_JOB])
        _apply_percent_threshold_formatting_range(
            client,
            titles[TAB_JOB],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=5,
        )  # F2:F...
        _apply_unified_tao_delta_pct_formatting_range(
            client,
            titles[TAB_JOB],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=6,
        )  # G2:G... red if >0 else green
        _apply_zero_good_nonzero_bad_formatting_range(
            client,
            titles[TAB_JOB],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=9,
        )  # J2:J...
        _apply_today_freshness_formatting_range(
            client,
            titles[TAB_JOB],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=7,
        )  # H2:H...
        _apply_today_freshness_formatting_range(
            client,
            titles[TAB_JOB],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=8,
        )  # I2:I...
    if TAB_MOJO in titles:
        _apply_percent_threshold_formatting_range(
            client,
            titles[TAB_MOJO],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=3,
        )  # D2:D...
        _apply_percent_threshold_formatting_range(
            client,
            titles[TAB_MOJO],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=7,
        )  # H2:H...
        _apply_percent_threshold_formatting_range(
            client,
            titles[TAB_MOJO],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=10,
        )  # K2:K... Cum. Delta Mojo vs CRM (%)
        _apply_percent_threshold_formatting_range(
            client,
            titles[TAB_MOJO],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=14,
        )  # O2:O... Cum. ATS Rejected out of Total (%)
    if TAB_FUNNEL in titles:
        _apply_percent_threshold_formatting_range(
            client,
            titles[TAB_FUNNEL],
            start_row=_DATA_START_ROW,
            end_row=_DATA_END_ROW,
            col=6,
        )  # G2:G...


# ---------------------------------------------------------------------------
# Delta ATS − Mojo  traffic-light conditional formatting
# ---------------------------------------------------------------------------

def _apply_percent_threshold_formatting_range(
    client: SheetsClient,
    sid: int,
    *,
    start_row: int,
    end_row: int,
    col: int,
) -> None:
    """Apply green/yellow/red percentage thresholds to one column across many rows."""
    existing = client.get_sheet_conditional_formats(sid)
    delete_reqs: list[dict[str, Any]] = []
    for i, rule in reversed(list(enumerate(existing))):
        for row in range(start_row, end_row):
            if _rule_overlaps_cell(rule, row=row, col=col):
                delete_reqs.append(
                    {"deleteConditionalFormatRule": {"sheetId": sid, "index": i}}
                )
                break
    if delete_reqs:
        client.batch_update(delete_reqs)

    rng: dict[str, Any] = {
        "sheetId": sid,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": col,
        "endColumnIndex": col + 1,
    }
    col_letter = chr(65 + col)
    row_num = start_row + 1
    cell_ref = f"${col_letter}{row_num}"

    def _bg(r: float, g: float, b: float) -> dict[str, float]:
        return {"red": r, "green": g, "blue": b}

    rules = [
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}<={_THRESH_YELLOW})"}],
                },
                "format": {"backgroundColor": _bg(0.714, 0.843, 0.659)},
            },
        },
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}>{_THRESH_YELLOW},{cell_ref}<={_THRESH_RED})"}],
                },
                "format": {"backgroundColor": _bg(1.0, 0.898, 0.600)},
            },
        },
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}>{_THRESH_RED})"}],
                },
                "format": {"backgroundColor": _bg(0.918, 0.600, 0.600)},
            },
        },
    ]
    add_reqs = [
        {"addConditionalFormatRule": {"rule": rule, "index": 0}}
        for rule in rules
    ]
    client.batch_update(add_reqs)

    client.batch_update([
        {
            "repeatCell": {
                "range": {
                    "sheetId": sid,
                    "startRowIndex": start_row,
                    "endRowIndex": end_row,
                    "startColumnIndex": col,
                    "endColumnIndex": col + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "PERCENT", "pattern": "0.00%"},
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    ])


def _apply_unified_tao_delta_pct_formatting_range(
    client: SheetsClient,
    sid: int,
    *,
    start_row: int,
    end_row: int,
    col: int,
) -> None:
    """``(Unified−Tao)/Unified`` as percent: red if > 0, green if <= 0."""
    existing = client.get_sheet_conditional_formats(sid)
    delete_reqs: list[dict[str, Any]] = []
    for i, rule in reversed(list(enumerate(existing))):
        for row in range(start_row, end_row):
            if _rule_overlaps_cell(rule, row=row, col=col):
                delete_reqs.append(
                    {"deleteConditionalFormatRule": {"sheetId": sid, "index": i}}
                )
                break
    if delete_reqs:
        client.batch_update(delete_reqs)

    rng: dict[str, Any] = {
        "sheetId": sid,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": col,
        "endColumnIndex": col + 1,
    }
    col_letter = chr(65 + col)
    cell_ref = f"${col_letter}{start_row + 1}"

    def _bg(r: float, g: float, b: float) -> dict[str, float]:
        return {"red": r, "green": g, "blue": b}

    rules = [
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [
                        {"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}<=0)"},
                    ],
                },
                "format": {"backgroundColor": _bg(0.714, 0.843, 0.659)},
            },
        },
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [
                        {"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}>0)"},
                    ],
                },
                "format": {"backgroundColor": _bg(0.918, 0.600, 0.600)},
            },
        },
    ]
    add_reqs = [
        {"addConditionalFormatRule": {"rule": rule, "index": 0}}
        for rule in rules
    ]
    client.batch_update(add_reqs)

    client.batch_update([
        {
            "repeatCell": {
                "range": {
                    "sheetId": sid,
                    "startRowIndex": start_row,
                    "endRowIndex": end_row,
                    "startColumnIndex": col,
                    "endColumnIndex": col + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "PERCENT", "pattern": "0.00%"},
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    ])


# ---------------------------------------------------------------------------
# Generic: green = 0, red = any non-zero
# ---------------------------------------------------------------------------

def _rule_overlaps_cell(rule: dict[str, Any], *, row: int, col: int) -> bool:
    for r in rule.get("ranges", []):
        r_start = r.get("startRowIndex", 0)
        r_end = r.get("endRowIndex", r_start + 1)
        c_start = r.get("startColumnIndex", 0)
        c_end = r.get("endColumnIndex", c_start + 1)
        if r_start <= row < r_end and c_start <= col < c_end:
            return True
    return False


def _remove_obsolete_job_conditional_formats(client: SheetsClient, sid: int) -> None:
    """Remove leftover rules from the old Job Ingestion layout (old H7/I7/J7 cells)."""
    obsolete_cells = [(6, 7), (6, 8), (6, 9)]  # H7, I7, J7
    existing = client.get_sheet_conditional_formats(sid)
    delete_reqs: list[dict[str, Any]] = []
    for i, rule in reversed(list(enumerate(existing))):
        if any(_rule_overlaps_cell(rule, row=row, col=col) for row, col in obsolete_cells):
            delete_reqs.append(
                {"deleteConditionalFormatRule": {"sheetId": sid, "index": i}}
            )
    if delete_reqs:
        client.batch_update(delete_reqs)


def _apply_today_freshness_formatting(
    client: SheetsClient, sid: int, *, row: int, col: int
) -> None:
    """Green when timestamp is today; red when it is before today."""
    existing_rules = client.get_sheet_conditional_formats(sid)
    delete_reqs: list[dict[str, Any]] = []
    for i, rule in reversed(list(enumerate(existing_rules))):
        if _rule_overlaps_cell(rule, row=row, col=col):
            delete_reqs.append(
                {"deleteConditionalFormatRule": {"sheetId": sid, "index": i}}
            )
    if delete_reqs:
        client.batch_update(delete_reqs)

    rng: dict[str, Any] = {
        "sheetId": sid,
        "startRowIndex": row,
        "endRowIndex": row + 1,
        "startColumnIndex": col,
        "endColumnIndex": col + 1,
    }
    col_letter = chr(65 + col)
    cell_ref = f"${col_letter}${row + 1}"

    def _bg(r: float, g: float, b: float) -> dict[str, float]:
        return {"red": r, "green": g, "blue": b}

    rules = [
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND({cell_ref}<>\"\",INT({cell_ref})>=TODAY())"}],
                },
                "format": {"backgroundColor": _bg(0.714, 0.843, 0.659)},
            },
        },
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND({cell_ref}<>\"\",INT({cell_ref})<TODAY())"}],
                },
                "format": {"backgroundColor": _bg(0.918, 0.600, 0.600)},
            },
        },
    ]

    add_reqs = [
        {"addConditionalFormatRule": {"rule": rule, "index": 0}}
        for rule in rules
    ]
    client.batch_update(add_reqs)


def _apply_today_freshness_formatting_range(
    client: SheetsClient,
    sid: int,
    *,
    start_row: int,
    end_row: int,
    col: int,
) -> None:
    """Green when timestamp is today; red when it is before today."""
    existing_rules = client.get_sheet_conditional_formats(sid)
    delete_reqs: list[dict[str, Any]] = []
    for i, rule in reversed(list(enumerate(existing_rules))):
        for row in range(start_row, end_row):
            if _rule_overlaps_cell(rule, row=row, col=col):
                delete_reqs.append(
                    {"deleteConditionalFormatRule": {"sheetId": sid, "index": i}}
                )
                break
    if delete_reqs:
        client.batch_update(delete_reqs)

    rng: dict[str, Any] = {
        "sheetId": sid,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": col,
        "endColumnIndex": col + 1,
    }
    col_letter = chr(65 + col)
    cell_ref = f"${col_letter}{start_row + 1}"

    def _bg(r: float, g: float, b: float) -> dict[str, float]:
        return {"red": r, "green": g, "blue": b}

    rules = [
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND({cell_ref}<>\"\",INT({cell_ref})>=TODAY())"}],
                },
                "format": {"backgroundColor": _bg(0.714, 0.843, 0.659)},
            },
        },
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND({cell_ref}<>\"\",INT({cell_ref})<TODAY())"}],
                },
                "format": {"backgroundColor": _bg(0.918, 0.600, 0.600)},
            },
        },
    ]
    add_reqs = [
        {"addConditionalFormatRule": {"rule": rule, "index": 0}}
        for rule in rules
    ]
    client.batch_update(add_reqs)

def _apply_zero_good_nonzero_bad_formatting(
    client: SheetsClient, sid: int, *, row: int, col: int
) -> None:
    """Idempotent: green when cell = 0, red when cell > 0. row/col are 0-based."""
    existing = client.get_sheet_conditional_formats(sid)
    delete_reqs: list[dict[str, Any]] = []
    for i, rule in reversed(list(enumerate(existing))):
        for r in rule.get("ranges", []):
            r_start = r.get("startRowIndex", 0)
            r_end = r.get("endRowIndex", r_start + 1)
            c_start = r.get("startColumnIndex", 0)
            c_end = r.get("endColumnIndex", c_start + 1)
            if r_start <= row < r_end and c_start <= col < c_end:
                delete_reqs.append(
                    {"deleteConditionalFormatRule": {"sheetId": sid, "index": i}}
                )
                break
    if delete_reqs:
        client.batch_update(delete_reqs)

    rng: dict[str, Any] = {
        "sheetId": sid,
        "startRowIndex": row,
        "endRowIndex": row + 1,
        "startColumnIndex": col,
        "endColumnIndex": col + 1,
    }

    # A1 address of this cell (for formula references)
    col_letter = chr(65 + col)  # works for cols A-Z
    cell_ref = f"${col_letter}${row + 1}"

    def _bg(r: float, g: float, b: float) -> dict[str, float]:
        return {"red": r, "green": g, "blue": b}

    rules = [
        # Green: exactly 0
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}=0)"}],
                },
                "format": {"backgroundColor": _bg(0.714, 0.843, 0.659)},  # #b6d7a8
            },
        },
        # Red: any non-zero value
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}>0)"}],
                },
                "format": {"backgroundColor": _bg(0.918, 0.600, 0.600)},  # #ea9999
            },
        },
    ]

    # Insert in forward order so Red (last) ends at index 0 (highest priority)
    add_reqs = [
        {"addConditionalFormatRule": {"rule": rule, "index": 0}}
        for rule in rules
    ]
    client.batch_update(add_reqs)


def _apply_zero_good_nonzero_bad_formatting_range(
    client: SheetsClient,
    sid: int,
    *,
    start_row: int,
    end_row: int,
    col: int,
) -> None:
    """Idempotent: green when cell = 0, red when cell > 0 over a row range."""
    existing = client.get_sheet_conditional_formats(sid)
    delete_reqs: list[dict[str, Any]] = []
    for i, rule in reversed(list(enumerate(existing))):
        for row in range(start_row, end_row):
            if _rule_overlaps_cell(rule, row=row, col=col):
                delete_reqs.append(
                    {"deleteConditionalFormatRule": {"sheetId": sid, "index": i}}
                )
                break
    if delete_reqs:
        client.batch_update(delete_reqs)

    rng: dict[str, Any] = {
        "sheetId": sid,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": col,
        "endColumnIndex": col + 1,
    }
    col_letter = chr(65 + col)
    cell_ref = f"${col_letter}{start_row + 1}"

    def _bg(r: float, g: float, b: float) -> dict[str, float]:
        return {"red": r, "green": g, "blue": b}

    rules = [
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}=0)"}],
                },
                "format": {"backgroundColor": _bg(0.714, 0.843, 0.659)},
            },
        },
        {
            "ranges": [rng],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": f"=AND(ISNUMBER({cell_ref}),{cell_ref}>0)"}],
                },
                "format": {"backgroundColor": _bg(0.918, 0.600, 0.600)},
            },
        },
    ]
    add_reqs = [
        {"addConditionalFormatRule": {"rule": rule, "index": 0}}
        for rule in rules
    ]
    client.batch_update(add_reqs)
