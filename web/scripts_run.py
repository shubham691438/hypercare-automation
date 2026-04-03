"""Thin wrapper that runs the hypercare pipeline and returns a result dict.

Called by backend.py — not meant to be run standalone.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT / "scripts"))


def execute_run(
    client_id: str,
    client_cfg: dict[str, Any],
    job_date: str,
    report_date: str,
) -> dict[str, Any]:
    """Run the full hypercare pipeline and return a summary dict."""
    import importlib

    # Force-reload the runner so it picks up fresh env
    import run_hypercare_queries as runner_mod

    importlib.reload(runner_mod)

    from client_config import (
        TRACKER_FUNNEL_TRACKING,
        TRACKER_JOB_INGESTION,
        TRACKER_MOJO_APPLY,
        enabled_tabs,
        hypercare_status,
    )
    from sheets_client import SheetsClient
    from hypercare_bootstrap import TAB_FUNNEL, TAB_JOB, TAB_MOJO, TAB_QUERY_REGISTRY
    from db_readonly import (
        WriteQueryNotAllowed,
        assert_read_only_sql,
        fetch_all,
        readonly_connection,
        unified_config_from_env,
        tao_config_from_env,
    )
    from ats.base import WebsiteJobsConfigError, fetch_website_open_jobs
    from mojo_jobs import (
        MojoConfigError,
        fetch_open_jobs_metrics,
        fetch_sponsored_stage_counts,
        fetch_sponsored_applies,
    )

    runner_mod.set_client_runtime_defaults(client_cfg)
    active_trackers = set(enabled_tabs(client_id))
    lifecycle = hypercare_status(client_id)

    sheets = SheetsClient()
    errors: list[str] = []
    ok_count = 0
    err_count = 0

    try:
        unified_cfg = unified_config_from_env()
        tao_cfg = tao_config_from_env()
    except OSError as e:
        return {"status": "error", "message": f"DB config missing: {e}"}

    # --- Run registry queries ---
    reg = sheets.get_range(f"'{TAB_QUERY_REGISTRY}'!A2:F500")
    registry_results: dict[str, str] = {}
    website_open = None
    website_error = ""
    sponsored_applies = None
    sponsored_applies_error = ""
    sponsored_stage_counts = []
    funnel_stage_error = ""
    open_count = None
    mojo_open_error = ""
    crm_all_counts: dict[str, int] = {}
    crm_sponsored_counts: dict[str, int] = {}
    funnel_crm_error = ""

    if TRACKER_JOB_INGESTION in active_trackers:
        try:
            website_open = fetch_website_open_jobs()
        except WebsiteJobsConfigError as e:
            website_error = f"config: {e}"
        except Exception as e:
            err_count += 1
            website_error = str(e)
            errors.append(f"ATS: {e}")

    with readonly_connection(unified_cfg) as unified_conn, \
         readonly_connection(tao_cfg) as tao_conn:

        for row in reg:
            row = runner_mod.pad_registry_row(row)
            if not str(row[0]).strip():
                break
            qid = str(row[0]).strip()
            db = str(row[1]).strip().lower()
            sql = str(row[3]).strip() if len(row) > 3 else ""
            out_sheet = str(row[4]).strip() if len(row) > 4 else ""
            if not sql or not out_sheet:
                continue

            tracker_key = runner_mod.tracker_for_sheet(out_sheet)
            if tracker_key and tracker_key not in active_trackers:
                continue

            date_for_query = job_date if out_sheet == TAB_JOB else report_date
            sql_filled = sql.replace("__REPORT_DATE__", date_for_query)

            try:
                assert_read_only_sql(sql_filled)
            except WriteQueryNotAllowed:
                continue

            conn = unified_conn if db == "unified" else tao_conn if db == "tao" else None
            if conn is None:
                continue

            try:
                raw_rows = fetch_all(conn, sql_filled)
                values = runner_mod.normalize_rows(raw_rows)
                registry_results[qid] = runner_mod.first_scalar(values)
                ok_count += 1
            except Exception as e:
                err_count += 1
                errors.append(f"Query {qid}: {e}")

        if TRACKER_FUNNEL_TRACKING in active_trackers:
            tao_client_id = client_cfg["db"]["tao"]["client_id"]
            try:
                crm_all_counts = runner_mod.fetch_funnel_crm_counts(
                    tao_conn, report_date,
                    tao_client_id=tao_client_id, sponsored_only=False,
                )
                crm_sponsored_counts = runner_mod.fetch_funnel_crm_counts(
                    tao_conn, report_date,
                    tao_client_id=tao_client_id, sponsored_only=True,
                )
            except Exception as e:
                err_count += 1
                funnel_crm_error = str(e)
                errors.append(f"Funnel CRM: {e}")

    # --- Mojo API calls ---
    if TRACKER_MOJO_APPLY in active_trackers:
        try:
            sponsored_applies = fetch_sponsored_applies(report_date)
        except Exception as e:
            err_count += 1
            sponsored_applies_error = str(e)
            errors.append(f"Mojo applies: {e}")

    if TRACKER_FUNNEL_TRACKING in active_trackers:
        try:
            sponsored_stage_counts = fetch_sponsored_stage_counts(report_date)
        except Exception as e:
            err_count += 1
            funnel_stage_error = str(e)
            errors.append(f"Mojo funnel: {e}")

    if TRACKER_JOB_INGESTION in active_trackers:
        try:
            open_count, _ = fetch_open_jobs_metrics()
        except Exception as e:
            err_count += 1
            mojo_open_error = str(e)
            errors.append(f"Mojo jobs: {e}")

    # --- Write rows ---
    if TRACKER_JOB_INGESTION in active_trackers:
        job_row = [
            job_date,
            runner_mod.format_metric_value(
                website_open if not website_error else f"ERROR: {website_error}"
            ),
            registry_results.get("ji_unified_open", ""),
            registry_results.get("ji_tao_open", ""),
            runner_mod.format_metric_value(
                open_count if not mojo_open_error else f"ERROR: {mojo_open_error}"
            ),
            runner_mod.percent_delta(open_count, website_open),
            runner_mod.percent_delta(
                registry_results.get("ji_tao_open", ""),
                registry_results.get("ji_unified_open", ""),
            ),
            registry_results.get("ji_unified_open_latest", ""),
            registry_results.get("ji_unified_closed_latest", ""),
            registry_results.get("ji_mojo_tao_null_mapping", ""),
        ]
        target, _ = runner_mod.find_or_next_row(
            sheets, TAB_JOB, date_val=job_date, end_col="J"
        )
        col_end = runner_mod.idx_to_col(len(job_row))
        sheets.update_range(
            f"'{TAB_JOB}'!A{target}:{col_end}{target}", [job_row]
        )

    if TRACKER_MOJO_APPLY in active_trackers:
        mojo_api_val = sponsored_applies
        if sponsored_applies_error:
            mojo_api_val = f"ERROR: {sponsored_applies_error}"
        mojo_row = [
            report_date,
            runner_mod.format_metric_value(mojo_api_val),
            registry_results.get("mojo_sponsored_tao", ""),
            runner_mod.percent_delta(
                registry_results.get("mojo_sponsored_tao", ""), sponsored_applies
            ),
            registry_results.get("mojo_total_tao", ""),
            registry_results.get("mojo_crm_creation_failed", ""),
            registry_results.get("mojo_ats_rejected", ""),
            runner_mod.ratio(
                registry_results.get("mojo_ats_rejected", ""),
                registry_results.get("mojo_total_tao", ""),
            ),
        ]
        # Cumulative
        existing_rows = sheets.get_range(f"'{TAB_MOJO}'!A2:H1000")
        _SUM_COLS = [1, 2, 4, 5, 6]
        cum_sums = {c: 0.0 for c in _SUM_COLS}
        rdate_parsed = runner_mod._parse_date_loose(report_date)
        for erow in existing_rows:
            if not erow or not str(erow[0]).strip():
                continue
            rd = runner_mod._parse_date_loose(str(erow[0]).strip())
            if rd is None:
                continue
            if runner_mod._dates_equal(str(erow[0]), report_date):
                continue
            if rdate_parsed and rd > rdate_parsed:
                continue
            for c in _SUM_COLS:
                if c < len(erow):
                    v = runner_mod.to_number(erow[c])
                    if v is not None:
                        cum_sums[c] += v
        for c in _SUM_COLS:
            v = runner_mod.to_number(mojo_row[c])
            if v is not None:
                cum_sums[c] += v

        mojo_row.extend([
            runner_mod.format_metric_value(int(cum_sums[1])),
            runner_mod.format_metric_value(int(cum_sums[2])),
            runner_mod.percent_delta(cum_sums[2], cum_sums[1]),
            runner_mod.format_metric_value(int(cum_sums[4])),
            runner_mod.format_metric_value(int(cum_sums[5])),
            runner_mod.format_metric_value(int(cum_sums[6])),
            runner_mod.ratio(cum_sums[6], cum_sums[4]),
        ])
        target, _ = runner_mod.find_or_next_row(
            sheets, TAB_MOJO, date_val=report_date, end_col="O"
        )
        col_end = runner_mod.idx_to_col(len(mojo_row))
        sheets.update_range(
            f"'{TAB_MOJO}'!A{target}:{col_end}{target}", [mojo_row]
        )

    if TRACKER_FUNNEL_TRACKING in active_trackers:
        if funnel_crm_error:
            funnel_rows = [[report_date, "ERROR", funnel_crm_error,
                            "", "", "", "", "", "", "", ""]]
        elif funnel_stage_error:
            funnel_rows = [[report_date, "ERROR", funnel_stage_error,
                            "", "", "", "", "", "", "", ""]]
        else:
            existing_funnel = sheets.get_range(f"'{TAB_FUNNEL}'!A2:F1000")
            rdate_parsed = runner_mod._parse_date_loose(report_date)
            cum_stage_sums: dict[str, tuple[float, float, float]] = {}
            for erow in existing_funnel:
                if not erow or not str(erow[0]).strip():
                    continue
                rd = runner_mod._parse_date_loose(str(erow[0]).strip())
                if rd is None:
                    continue
                if runner_mod._dates_equal(str(erow[0]), report_date):
                    continue
                if rdate_parsed and rd > rdate_parsed:
                    continue
                sk = str(erow[1]).strip() if len(erow) > 1 else ""
                if not sk:
                    continue
                d_all = runner_mod.to_number(erow[3]) if len(erow) > 3 else None
                d_sp = runner_mod.to_number(erow[4]) if len(erow) > 4 else None
                d_mj = runner_mod.to_number(erow[5]) if len(erow) > 5 else None
                prev = cum_stage_sums.get(sk, (0.0, 0.0, 0.0))
                cum_stage_sums[sk] = (
                    prev[0] + (d_all or 0.0),
                    prev[1] + (d_sp or 0.0),
                    prev[2] + (d_mj or 0.0),
                )
            funnel_rows = runner_mod.build_funnel_table_rows(
                report_date, sponsored_stage_counts,
                crm_all_counts, crm_sponsored_counts,
                cum_stage_sums=cum_stage_sums,
            )
        if funnel_rows:
            funnel_start, existing_len = runner_mod.find_funnel_block_start(
                sheets, TAB_FUNNEL, date_val=report_date
            )
            if existing_len == 0 and funnel_start > 2:
                sheets.update_range(
                    f"'{TAB_FUNNEL}'!A{funnel_start - 1}:K{funnel_start - 1}",
                    [[""] * 11],
                )
            end_row = funnel_start + len(funnel_rows) - 1
            sheets.update_range(
                f"'{TAB_FUNNEL}'!A{funnel_start}:K{end_row}", funnel_rows
            )
            if existing_len > len(funnel_rows):
                blank = [[""] * 11] * (existing_len - len(funnel_rows))
                cs = funnel_start + len(funnel_rows)
                sheets.update_range(
                    f"'{TAB_FUNNEL}'!A{cs}:K{cs + len(blank) - 1}", blank
                )

    status = "ok" if err_count == 0 else "partial"
    return {
        "status": status,
        "message": f"ok={ok_count} errors={err_count}",
        "ok_count": ok_count,
        "error_count": err_count,
        "errors": errors,
        "spreadsheet_id": sheets.spreadsheet_id,
    }
