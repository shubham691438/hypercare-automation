"""Create tabs and baseline layout on the hypercare spreadsheet."""

from __future__ import annotations

import os
from typing import Any

from client_config import hypercare_status, load_client_config
from sheets_client import SheetsClient

TAB_QUERY_REGISTRY = "Query registry"
TAB_OVERVIEW = "Overview"
TAB_JOB = "Job Ingestion"
TAB_MOJO = "Mojo Apply"
TAB_FUNNEL = "Funnel Tracking"
TAB_WEBSITE = "Website"
TAB_RUN_LOG = "Run log"
TAB_JOB_KEY = "job_ingestion"
TAB_MOJO_KEY = "mojo_apply"
TAB_FUNNEL_KEY = "funnel_tracking"
TABLE_MAX_ROWS = 1000
JOB_INGESTION_HEADERS = [
    "Date",
    "Open Jobs on ATS",
    "Open Jobs in Unified DB",
    "Open Jobs in Tao DB",
    "Open Jobs on Mojo",
    "Delta ATS − Mojo (%)",
    "Last OPEN job updated (IST)",
    "Last CLOSED job updated (IST)",
    "Null Mojo↔Tao mappings",
]
MOJO_APPLY_HEADERS = [
    "Date",
    "Sponsored Applies on Mojo",
    "Sponsored Applies in Tao",
    "Delta Mojo vs CRM (%)",
    "Total Applies in CRM",
    "CRM Creation Failed",
    "ATS Rejected",
    "ATS Rejected out of Total Applies in CRM (%)",
]
FUNNEL_HEADERS = [
    "Date",
    "Mojo Stage",
    "CRM Stage Mapping",
    "CRM Count - All",
    "CRM Count - Sponsored",
    "Mojo Count - Sponsored",
    "Delta CRM Sponsored vs Mojo (%)",
]

ORDERED_TABS = [
    TAB_OVERVIEW,
    TAB_QUERY_REGISTRY,
    TAB_JOB,
    TAB_MOJO,
    TAB_FUNNEL,
    TAB_WEBSITE,
    TAB_RUN_LOG,
]


def _tab_title_set(client: SheetsClient) -> dict[str, int]:
    return {t["title"]: int(t["sheetId"]) for t in client.list_tabs()}


def _active_client() -> dict[str, Any]:
    client_id = (os.environ.get("HYPERCARE_CLIENT") or "").strip()
    if not client_id:
        raise ValueError("Set HYPERCARE_CLIENT before bootstrapping the workbook.")
    return load_client_config(client_id)


def _ordered_tabs_for_client() -> list[str]:
    cfg = _active_client()
    trackers = set(cfg.get("enabled_tabs") or [])
    tabs = [TAB_OVERVIEW, TAB_QUERY_REGISTRY]
    if TAB_JOB_KEY in trackers:
        tabs.extend([TAB_JOB, TAB_WEBSITE])
    if TAB_MOJO_KEY in trackers:
        tabs.append(TAB_MOJO)
    if TAB_FUNNEL_KEY in trackers:
        tabs.append(TAB_FUNNEL)
    tabs.append(TAB_RUN_LOG)
    return tabs


def ensure_job_ingestion_native_table(client: SheetsClient) -> None:
    """Create or update the native Google Sheets table for Job Ingestion."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_JOB)
    if sid is None:
        return

    table = {
        "name": "JobIngestion",
        "range": {
            "sheetId": sid,
            "startRowIndex": 0,
            "endRowIndex": TABLE_MAX_ROWS,
            "startColumnIndex": 0,
            "endColumnIndex": 9,
        },
        "columnProperties": [
            {"columnIndex": 0, "columnName": "Date", "columnType": "DATE"},
            {"columnIndex": 1, "columnName": "Open Jobs on ATS", "columnType": "DOUBLE"},
            {"columnIndex": 2, "columnName": "Open Jobs in Unified DB", "columnType": "DOUBLE"},
            {"columnIndex": 3, "columnName": "Open Jobs in Tao DB", "columnType": "DOUBLE"},
            {"columnIndex": 4, "columnName": "Open Jobs on Mojo", "columnType": "DOUBLE"},
            {"columnIndex": 5, "columnName": "Delta ATS − Mojo (%)", "columnType": "PERCENT"},
            {"columnIndex": 6, "columnName": "Last OPEN job updated (IST)", "columnType": "DATE_TIME"},
            {"columnIndex": 7, "columnName": "Last CLOSED job updated (IST)", "columnType": "DATE_TIME"},
            {"columnIndex": 8, "columnName": "Null Mojo↔Tao mappings", "columnType": "DOUBLE"},
        ],
    }

    existing = client.get_sheet_tables(sid)
    if existing:
        table["tableId"] = existing[0]["tableId"]
        client.batch_update(
            [
                {
                    "updateTable": {
                        "table": table,
                        "fields": "name,range,columnProperties",
                    }
                }
            ]
        )
    else:
        client.batch_update([{"addTable": {"table": table}}])


def ensure_mojo_apply_native_table(client: SheetsClient) -> None:
    """Create or update the native Google Sheets table for Mojo Apply."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_MOJO)
    if sid is None:
        return

    table = {
        "name": "MojoApply",
        "range": {
            "sheetId": sid,
            "startRowIndex": 0,
            "endRowIndex": TABLE_MAX_ROWS,
            "startColumnIndex": 0,
            "endColumnIndex": 8,
        },
        "columnProperties": [
            {
                "columnIndex": i,
                "columnName": name,
                **({"columnType": "PERCENT"} if i in (3, 7) else {}),
            }
            for i, name in enumerate(MOJO_APPLY_HEADERS)
        ],
    }

    existing = client.get_sheet_tables(sid)
    if existing:
        table["tableId"] = existing[0]["tableId"]
        client.batch_update(
            [
                {
                    "updateTable": {
                        "table": table,
                        "fields": "name,range,columnProperties",
                    }
                }
            ]
        )
    else:
        client.batch_update([{"addTable": {"table": table}}])


def ensure_funnel_native_table(client: SheetsClient) -> None:
    """Create or update the native Google Sheets table for Funnel Tracking."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_FUNNEL)
    if sid is None:
        return

    table = {
        "name": "FunnelTracking",
        "range": {
            "sheetId": sid,
            "startRowIndex": 0,
            "endRowIndex": TABLE_MAX_ROWS,
            "startColumnIndex": 0,
            "endColumnIndex": 7,
        },
        "columnProperties": [
            {
                "columnIndex": i,
                "columnName": name,
                **({"columnType": "DATE"} if i == 0 else {}),
                **({"columnType": "DOUBLE"} if i in (3, 4, 5) else {}),
                **({"columnType": "PERCENT"} if i == 6 else {}),
            }
            for i, name in enumerate(FUNNEL_HEADERS)
        ],
    }

    existing = client.get_sheet_tables(sid)
    if existing:
        table["tableId"] = existing[0]["tableId"]
        client.batch_update(
            [
                {
                    "updateTable": {
                        "table": table,
                        "fields": "name,range,columnProperties",
                    }
                }
            ]
        )
    else:
        client.batch_update([{"addTable": {"table": table}}])


def clear_job_ingestion_manual_header_format(client: SheetsClient) -> None:
    """Let the native table own the Job Ingestion header styling."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_JOB)
    if sid is None:
        return
    client.batch_update(
        [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sid,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 9,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)",
                }
            }
        ]
    )


def clear_job_ingestion_extra_area_format(client: SheetsClient) -> None:
    """Remove stale formatting to the right of the Job Ingestion table."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_JOB)
    if sid is None:
        return
    client.batch_update(
        [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sid,
                        "startRowIndex": 0,
                        "endRowIndex": TABLE_MAX_ROWS,
                        "startColumnIndex": 9,
                        "endColumnIndex": 26,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment,numberFormat,borders)",
                }
            }
        ]
    )


def clear_mojo_apply_manual_header_format(client: SheetsClient) -> None:
    """Let the native table own the Mojo Apply header styling."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_MOJO)
    if sid is None:
        return
    client.batch_update(
        [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sid,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 8,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)",
                }
            }
        ]
    )


def clear_mojo_apply_extra_area_format(client: SheetsClient) -> None:
    """Remove stale formatting to the right of the Mojo Apply table."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_MOJO)
    if sid is None:
        return
    client.batch_update(
        [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sid,
                        "startRowIndex": 0,
                        "endRowIndex": TABLE_MAX_ROWS,
                        "startColumnIndex": 8,
                        "endColumnIndex": 26,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment,numberFormat,borders)",
                }
            }
        ]
    )


def clear_funnel_manual_header_format(client: SheetsClient) -> None:
    """Let the native table own the Funnel Tracking header styling."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_FUNNEL)
    if sid is None:
        return
    client.batch_update(
        [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sid,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 7,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)",
                }
            }
        ]
    )


def clear_funnel_extra_area_format(client: SheetsClient) -> None:
    """Remove stale formatting to the right of the Funnel Tracking table."""
    titles = _tab_title_set(client)
    sid = titles.get(TAB_FUNNEL)
    if sid is None:
        return
    client.batch_update(
        [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sid,
                        "startRowIndex": 0,
                        "endRowIndex": TABLE_MAX_ROWS,
                        "startColumnIndex": 7,
                        "endColumnIndex": 26,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment,numberFormat,borders)",
                }
            }
        ]
    )


def ensure_workbook_structure(client: SheetsClient) -> None:
    """Rename default sheet, add missing tabs, reorder left-to-right."""
    desired_tabs = _ordered_tabs_for_client()
    titles = _tab_title_set(client)
    requests: list[dict[str, Any]] = []

    if "Sheet1" in titles and TAB_OVERVIEW not in titles:
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": titles["Sheet1"],
                        "title": TAB_OVERVIEW,
                    },
                    "fields": "title",
                }
            }
        )
        titles[TAB_OVERVIEW] = titles.pop("Sheet1")

    for name in desired_tabs:
        if name not in titles:
            requests.append({"addSheet": {"properties": {"title": name}}})

    if requests:
        client.batch_update(requests)
        titles = _tab_title_set(client)

    reorder: list[dict[str, Any]] = []
    for idx, title in enumerate(desired_tabs):
        if title in titles:
            reorder.append(
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": titles[title],
                            "index": idx,
                        },
                        "fields": "index",
                    }
                }
            )
    if reorder:
        client.batch_update(reorder)


def seed_overview(client: SheetsClient) -> None:
    """Refresh Overview on every bootstrap so instructions stay in sync."""
    cfg = _active_client()
    lifecycle = hypercare_status(cfg["id"])
    trackers = ", ".join(cfg.get("enabled_tabs") or []) or "(none)"
    client.update_range(
        f"'{TAB_OVERVIEW}'!A1:C20",
        [
            ["Hypercare — automation workbook", "", ""],
            ["Client", cfg.get("name", ""), cfg.get("id", "")],
            ["Go-live date", lifecycle.get("go_live_date") or "(set in client config)", ""],
            ["Hypercare days", str(lifecycle.get("hypercare_days") or ""), ""],
            ["Enabled trackers", trackers, ""],
            ["Current lifecycle status", str(lifecycle.get("status") or ""), ""],
            [
                "Step 1 — database",
                "Add to .env: UNIFIED_DB_HOST, UNIFIED_DB_PORT, UNIFIED_DB_NAME, UNIFIED_DB_USER, UNIFIED_DB_PASSWORD",
                "",
            ],
            [
                "",
                "and TAO_DB_HOST, TAO_DB_PORT, TAO_DB_NAME, TAO_DB_USER, TAO_DB_PASSWORD",
                "",
            ],
            [
                "Step 2 — report date (optional)",
                "HYPERCARE_REPORT_DATE=YYYY-MM-DD (default: yesterday UTC). SQL uses __REPORT_DATE__ in Query registry.",
                "",
            ],
            [
                "Step 2b — job date (optional)",
                "HYPERCARE_JOB_DATE=YYYY-MM-DD (default: today). Job Ingestion uses this date.",
                "",
            ],
            [
                "Step 3 — run",
                "PYTHONPATH=src python scripts/run_hypercare_queries.py",
                "",
            ],
            [
                "Step 4 — verify",
                "PYTHONPATH=src python scripts/verify_hypercare_sheet.py",
                "",
            ],
            ["Last run (UTC)", "(filled automatically)", ""],
            ["Last run status", "", ""],
            ["Last run lifecycle", "", ""],
            ["", "", ""],
            [
                "Where results land",
                "Job Ingestion appends one row/day · Mojo Apply appends one row/day · Funnel Tracking appends stage rows/day · Run log appends each run step",
                "",
            ],
            [
                "Registry",
                f"All SQL lives in tab “{TAB_QUERY_REGISTRY}” (columns A–F).",
                "",
            ],
            [
                "Lifecycle rule",
                "Clients stay active only within go_live_date + hypercare_days. Historical rows remain after hypercare ends.",
                "",
            ],
            ["", "", ""],
        ],
    )


def seed_registry_and_reporting_tabs(client: SheetsClient) -> None:
    """Write Query registry rows and reporting tab headers."""
    cfg = _active_client()
    db_cfg = cfg["db"]
    unified_customer_id = db_cfg["unified"]["customer_id"]
    tao_source_id = db_cfg["tao"]["source_id"]
    tao_client_id = db_cfg["tao"]["client_id"]

    titles = _tab_title_set(client)
    enabled = set(cfg.get("enabled_tabs") or [])
    client.update_range(
        f"'{TAB_QUERY_REGISTRY}'!A1:F1",
        [
            [
                "query_id",
                "db",
                "description",
                "sql",
                "output_sheet",
                "output_cell",
            ]
        ],
    )

    # Seeded rows mirror Ashley hypercare SQL; __REPORT_DATE__ substituted at run time.
    registry_rows: list[list[str]] = []
    if TAB_JOB_KEY in enabled:
        registry_rows.extend(
            [
                [
                    "ji_unified_open",
                    "unified",
                    "Count OPEN jobs (unified job table)",
                    f"SELECT count(*)::text AS v FROM job WHERE customer_id = '{unified_customer_id}' AND status = 'OPEN'",
                    TAB_JOB,
                    "C2",
                ],
                [
                    "ji_tao_open",
                    "tao",
                    "Count OPEN jobs (tao jobs table)",
                    f"SELECT count(*)::text AS v FROM jobs WHERE source_id = '{tao_source_id}' AND status = 'OPEN'",
                    TAB_JOB,
                    "D2",
                ],
                [
                    "ji_unified_open_latest",
                    "unified",
                    "Max updated_at OPEN job (IST text)",
                    f"SELECT to_char(max(updated_at AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS.MS') AS v FROM job WHERE customer_id = '{unified_customer_id}' AND status = 'OPEN'",
                    TAB_JOB,
                    "G2",
                ],
                [
                    "ji_unified_closed_latest",
                    "unified",
                    "Max updated_at CLOSED job (IST text)",
                    f"SELECT to_char(max(updated_at AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS.MS') AS v FROM job WHERE customer_id = '{unified_customer_id}' AND status = 'CLOSED'",
                    TAB_JOB,
                    "H2",
                ],
                [
                    "ji_mojo_tao_null_mapping",
                    "tao",
                    "Jobs with broken Mojo↔Tao mapping (mojo_job_id OR tao_job_id is NULL)",
                    f"SELECT count(*)::text AS v FROM mojo_job_tao_job_mapping mjtjm WHERE mjtjm.tao_client_id = '{tao_client_id}' AND (mjtjm.mojo_job_id IS NULL OR mjtjm.tao_job_id IS NULL)",
                    TAB_JOB,
                    "I2",
                ],
            ]
        )
    if TAB_MOJO_KEY in enabled:
        registry_rows.extend(
            [
                [
                    "mojo_sponsored_unified",
                    "unified",
                    "Sponsored APPLY (Snowflake path — not in this RDS; placeholder)",
                    "SELECT 'N/A — no view_hot_store_tracking_event in unified RDS (use Snowflake / original sheet source)'::text AS v",
                    TAB_MOJO,
                    "B2",
                ],
                [
                    "mojo_sponsored_tao",
                    "tao",
                    "Sponsored APPLY_FINISH count (CRM) for report date",
                    f"SELECT count(*)::text AS v FROM candidate_submissions_v2 cs WHERE cs.client_id = '{tao_client_id}' AND cs.event = 'APPLY_FINISH' AND date(cs.created_at) = DATE '__REPORT_DATE__' AND cs.source_metadata ->> 'originSourceL1' = 'SPONSORED'",
                    TAB_MOJO,
                    "C2",
                ],
                [
                    "mojo_total_tao",
                    "tao",
                    "Total APPLY_FINISH (sponsored + non-sponsored) for report date",
                    f"SELECT count(*)::text AS v FROM candidate_submissions_v2 cs WHERE cs.client_id = '{tao_client_id}' AND cs.event = 'APPLY_FINISH' AND date(cs.created_at) = DATE '__REPORT_DATE__'",
                    TAB_MOJO,
                    "E2",
                ],
                [
                    "mojo_crm_creation_failed",
                    "tao",
                    "CRM creation failed: APPLY_FINISH rows missing candidate_id or application_id",
                    f"SELECT count(*)::text AS v FROM candidate_submissions_v2 WHERE client_id = '{tao_client_id}' AND job_id IS NOT NULL AND (candidate_id IS NULL OR application_id IS NULL) AND date(created_at) = DATE '__REPORT_DATE__' AND event = 'APPLY_FINISH'",
                    TAB_MOJO,
                    "F2",
                ],
                [
                    "mojo_ats_rejected",
                    "tao",
                    "ATS rejected: APPLY_FINISH rows with no matching WTA audit submission",
                    f"SELECT count(*)::text AS v FROM candidate_submissions_v2 t LEFT JOIN candidate_submissions_wta_audit cswa ON cswa.submission_id = t.source_metadata ->> 'submissionId' WHERE t.client_id = '{tao_client_id}' AND cswa.submission_id IS NULL AND t.event = 'APPLY_FINISH' AND date(t.created_at) = DATE '__REPORT_DATE__'",
                    TAB_MOJO,
                    "G2",
                ],
            ]
        )
    if not registry_rows:
        registry_rows = [["", "", "", "", "", ""]]
    last_data_row = 1 + len(registry_rows)
    client.update_range(f"'{TAB_QUERY_REGISTRY}'!A2:F{last_data_row}", registry_rows)
    # Clear any stale rows that may exist beyond the current registry (e.g. after removing a query)
    for extra_row in range(last_data_row + 1, last_data_row + 10):
        client.update_range(f"'{TAB_QUERY_REGISTRY}'!A{extra_row}:F{extra_row}", [["", "", "", "", "", ""]])

    # Single header row; rows 2+ are append-only history.
    if TAB_JOB in titles:
        client.update_range(
            f"'{TAB_JOB}'!A1:I1",
            [JOB_INGESTION_HEADERS],
        )
        client.update_range(
            f"'{TAB_JOB}'!A2:I{TABLE_MAX_ROWS}",
            [["" for _ in range(len(JOB_INGESTION_HEADERS))] for _ in range(TABLE_MAX_ROWS - 1)],
        )
        client.update_range(
            f"'{TAB_JOB}'!J1:L2",
            [["", "", ""], ["", "", ""]],
        )

    if TAB_MOJO in titles:
        client.update_range(f"'{TAB_MOJO}'!A1:H1", [MOJO_APPLY_HEADERS])
        client.update_range(
            f"'{TAB_MOJO}'!A2:H{TABLE_MAX_ROWS}",
            [["" for _ in range(len(MOJO_APPLY_HEADERS))] for _ in range(TABLE_MAX_ROWS - 1)],
        )

    if TAB_FUNNEL in titles:
        client.update_range(f"'{TAB_FUNNEL}'!A1:G1", [FUNNEL_HEADERS])
        client.update_range(
            f"'{TAB_FUNNEL}'!A2:G{TABLE_MAX_ROWS}",
            [["" for _ in range(len(FUNNEL_HEADERS))] for _ in range(TABLE_MAX_ROWS - 1)],
        )

    if TAB_WEBSITE in titles:
        client.update_range(
            f"'{TAB_WEBSITE}'!A1:B3",
            [
                ["Date", "Checks done"],
                ["", ""],
                ["", ""],
            ],
        )

    client.update_range(
        f"'{TAB_RUN_LOG}'!A1:G1",
        [["run_at_utc", "client_id", "run_scope", "query_id", "db", "status", "detail"]],
    )


def bootstrap_hypercare_workbook(client: SheetsClient, *, overwrite: bool = False) -> None:
    from hypercare_formatting import apply_hypercare_formatting

    ensure_workbook_structure(client)
    seed_overview(client)
    reg = client.get_range(f"'{TAB_QUERY_REGISTRY}'!A2")
    has_data = bool(reg and reg[0] and str(reg[0][0]).strip())
    if not has_data or overwrite:
        seed_registry_and_reporting_tabs(client)
    apply_hypercare_formatting(client)
    ensure_job_ingestion_native_table(client)
    clear_job_ingestion_manual_header_format(client)
    clear_job_ingestion_extra_area_format(client)
    ensure_mojo_apply_native_table(client)
    clear_mojo_apply_manual_header_format(client)
    clear_mojo_apply_extra_area_format(client)
    ensure_funnel_native_table(client)
    clear_funnel_manual_header_format(client)
    clear_funnel_extra_area_format(client)
    titles = _tab_title_set(client)
    # Native table creation/update can reset visible header cells to "Column 1..N".
    # Re-apply the intended labels after the table operation.
    if TAB_JOB in titles:
        client.update_range(f"'{TAB_JOB}'!A1:I1", [JOB_INGESTION_HEADERS])
    if TAB_MOJO in titles:
        client.update_range(f"'{TAB_MOJO}'!A1:H1", [MOJO_APPLY_HEADERS])
    if TAB_FUNNEL in titles:
        client.update_range(f"'{TAB_FUNNEL}'!A1:G1", [FUNNEL_HEADERS])
