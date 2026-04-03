#!/usr/bin/env python3
"""Run SELECT queries from the Query registry and write results to target cells.

Read-only DB sessions only. Set UNIFIED_* and TAO_* in .env.

  PYTHONPATH=src python scripts/run_hypercare_queries.py

Env:
  HYPERCARE_REPORT_DATE=YYYY-MM-DD  (default: yesterday UTC)
  GOOGLE_SHEETS_SPREADSHEET_ID / HYPERCARE_CLIENT
"""

from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from db_readonly import (  # noqa: E402
    WriteQueryNotAllowed,
    assert_read_only_sql,
    fetch_all,
    readonly_connection,
    tao_config_from_env,
    unified_config_from_env,
)
from hypercare_bootstrap import TAB_FUNNEL, TAB_JOB, TAB_MOJO, TAB_QUERY_REGISTRY  # noqa: E402
from ats.base import WebsiteJobsConfigError, fetch_website_open_jobs  # noqa: E402
from client_config import (  # noqa: E402
    TRACKER_FUNNEL_TRACKING,
    TRACKER_JOB_INGESTION,
    TRACKER_MOJO_APPLY,
    enabled_tabs,
    hypercare_status,
    load_client_config,
)
from mojo_jobs import (  # noqa: E402
    MojoConfigError,
    fetch_open_jobs_metrics,
    fetch_sponsored_stage_counts,
    fetch_sponsored_applies,
)
from sheets_client import SheetsClient  # noqa: E402

CELL_RE = re.compile(r"^([A-Za-z]+)(\d+)$")
REGISTRY_WIDTH = 6
OVERVIEW_LAST_RUN_ROW = 13
OVERVIEW_LAST_STATUS_ROW = 14
OVERVIEW_LAST_LIFECYCLE_ROW = 15
RUN_LOG_RANGE = "'Run log'!A1"

JOB_QUERY_IDS = {
    "ji_unified_open",
    "ji_tao_open",
    "ji_unified_open_latest",
    "ji_unified_closed_latest",
    "ji_mojo_tao_null_mapping",
}
MOJO_QUERY_IDS = {
    "mojo_sponsored_tao",
    "mojo_total_tao",
    "mojo_crm_creation_failed",
    "mojo_ats_rejected",
}


def load_local_env() -> None:
    env_path = _ROOT / ".env"
    if not env_path.is_file():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k, v)


def job_date_string() -> str:
    """Today's date for Job Ingestion queries — live job counts / latest status."""
    raw = (os.environ.get("HYPERCARE_JOB_DATE") or "").strip()
    if raw:
        return raw
    return date.today().isoformat()


def report_date_string() -> str:
    """Yesterday's date for Mojo Apply / Funnel queries — prior-day apply stats."""
    raw = (os.environ.get("HYPERCARE_REPORT_DATE") or "").strip()
    if raw:
        return raw
    return (date.today() - timedelta(days=1)).isoformat()


def _parse_date_loose(val: str) -> date | None:
    """Try common date formats; return a date object or None."""
    for fmt in ("%Y-%m-%d", "%-m/%-d/%Y", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(val.strip(), fmt).date()
        except ValueError:
            pass
    return None


def _dates_equal(a: str, b: str) -> bool:
    """True if both strings represent the same calendar date."""
    da, db = _parse_date_loose(a), _parse_date_loose(b)
    if da is not None and db is not None:
        return da == db
    return a.strip().lower() == b.strip().lower()


def find_or_next_row(
    sheets: SheetsClient,
    tab: str,
    *,
    date_val: str,
    end_col: str,
    max_scan: int = 1000,
) -> tuple[int, bool]:
    """Return (row_number, is_existing) for writing a dated row.

    Scans column A (rows 2..max_scan+1) for an entry whose date matches
    *date_val* (tolerant of M/D/YYYY vs YYYY-MM-DD differences).  If found,
    returns that row number with is_existing=True so the caller can overwrite
    in place.  Otherwise returns the row after the last data row.
    Returns (2, False) when the tab is completely empty.
    """
    rows = sheets.get_range(f"'{tab}'!A2:{end_col}{max_scan + 1}")
    last_data_row = 1
    for i, row in enumerate(rows, start=2):
        cell = str(row[0]).strip() if row else ""
        if cell and _dates_equal(cell, date_val):
            return i, True
        if any(str(c).strip() for c in row):
            last_data_row = i
    return last_data_row + 1, False


def find_funnel_block_start(
    sheets: SheetsClient,
    tab: str,
    *,
    date_val: str,
    max_scan: int = 1000,
) -> tuple[int, int]:
    """Return (block_start_row, block_length) for a funnel date block.

    For an **existing** date: returns (first_matching_row, number_of_matching_rows).
    For a **new** date with existing data above: returns (last_data_row + 2, 0) so
    that the caller can write a blank separator at last_data_row + 1 then stage rows
    starting at last_data_row + 2.
    For a completely empty tab: returns (2, 0) — no separator needed.

    Blank separator rows (empty column A) are skipped when tracking last_data_row,
    so they never push the separator offset.
    """
    rows = sheets.get_range(f"'{tab}'!A2:A{max_scan + 1}")
    last_data_row = 1
    block_start = None
    block_len = 0
    for i, row in enumerate(rows, start=2):
        cell = str(row[0]).strip() if row else ""
        if cell and _dates_equal(cell, date_val):
            if block_start is None:
                block_start = i
            block_len += 1
        if cell:  # only non-blank rows count as "data"
            last_data_row = i
    if block_start is not None:
        return block_start, block_len
    # New date: reserve a blank separator row if there is existing data above
    if last_data_row > 1:
        return last_data_row + 2, 0  # blank at last_data_row+1, data at last_data_row+2
    return 2, 0  # empty tab — start directly at row 2, no separator


def parse_a1(cell: str) -> tuple[str, int]:
    m = CELL_RE.match(cell.strip())
    if not m:
        raise ValueError(f"Invalid output_cell A1: {cell!r}")
    return m.group(1).upper(), int(m.group(2))


def col_to_idx(col: str) -> int:
    n = 0
    for ch in col.upper():
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Invalid column: {col!r}")
        n = n * 26 + (ord(ch) - 64)
    return n


def idx_to_col(idx: int) -> str:
    if idx < 1:
        raise ValueError("Column index must be >= 1")
    parts: list[str] = []
    n = idx
    while n:
        n, rem = divmod(n - 1, 26)
        parts.append(chr(65 + rem))
    return "".join(reversed(parts))


def write_block(
    sheets: SheetsClient,
    tab: str,
    start_cell: str,
    values: list[list[str]],
) -> None:
    if not values:
        return
    col0, row0 = parse_a1(start_cell)
    nrows = len(values)
    ncols = len(values[0])
    end_row = row0 + nrows - 1
    end_col = idx_to_col(col_to_idx(col0) + ncols - 1)
    a1 = f"'{tab}'!{col0}{row0}:{end_col}{end_row}"
    sheets.update_range(a1, values)


def normalize_rows(raw: list[tuple[object, ...]]) -> list[list[str]]:
    out: list[list[str]] = []
    for tup in raw:
        out.append(["" if v is None else str(v) for v in tup])
    return out


def pad_registry_row(row: list[str | int | float | None]) -> list[str]:
    cells = [str(c) if c is not None else "" for c in row]
    while len(cells) < REGISTRY_WIDTH:
        cells.append("")
    return cells[:REGISTRY_WIDTH]


def build_error_detail(step: str, exc: Exception, **context: object) -> str:
    parts = [f"step={step}"]
    for key, value in context.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    parts.append(f"exception_type={type(exc).__name__}")
    parts.append(f"message={str(exc).strip()}")
    return " | ".join(parts)


def append_run_log(
    sheets: SheetsClient,
    *,
    client_id: str,
    run_scope: str,
    query_id: str,
    db: str,
    status: str,
    detail: str,
    at_utc: str | None = None,
) -> None:
    sheets.append_rows(
        RUN_LOG_RANGE,
        [
            [
                at_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                client_id,
                run_scope,
                query_id,
                db,
                status,
                detail,
            ]
        ],
    )


def update_overview_status(
    sheets: SheetsClient,
    *,
    last_run_utc: str,
    status_text: str,
    lifecycle_text: str,
) -> None:
    sheets.update_range(
        f"'Overview'!B{OVERVIEW_LAST_RUN_ROW}:B{OVERVIEW_LAST_LIFECYCLE_ROW}",
        [[last_run_utc], [status_text], [lifecycle_text]],
    )


def tracker_for_sheet(sheet_name: str) -> str | None:
    if sheet_name == TAB_JOB:
        return TRACKER_JOB_INGESTION
    if sheet_name == TAB_MOJO:
        return TRACKER_MOJO_APPLY
    if sheet_name == TAB_FUNNEL:
        return TRACKER_FUNNEL_TRACKING
    return None


def set_client_runtime_defaults(client_cfg: dict[str, Any]) -> None:
    ats_cfg = client_cfg.get("ats") or {}
    mojo_cfg = client_cfg.get("mojo") or {}
    os.environ.setdefault("ATS_PROVIDER", str(ats_cfg.get("provider") or ""))
    os.environ.setdefault("ATS_URL", str(ats_cfg.get("url") or ""))
    os.environ.setdefault("MOJO_ACCOUNT_ID", str(mojo_cfg.get("account_id") or ""))
    os.environ.setdefault("MOJO_AGENCY_ID", str(mojo_cfg.get("agency_id") or ""))
    os.environ.setdefault("MOJO_CLIENT_ID", str(mojo_cfg.get("client_id") or ""))
    os.environ.setdefault("MOJO_JOBS_PATH", str(mojo_cfg.get("jobs_path") or ""))


def first_scalar(rows: list[list[str]]) -> str:
    if not rows or not rows[0]:
        return ""
    return str(rows[0][0]).strip()


def to_number(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _format_ratio_for_percent_cell(ratio: float) -> str:
    """Stable decimal string for PERCENT-formatted cells (no scientific notation)."""
    if not ratio:
        return "0"
    text = f"{ratio:.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


def format_metric_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value)


def percent_delta(numerator: object, denominator: object) -> str:
    num = to_number(numerator)
    den = to_number(denominator)
    if num is None or den is None or den == 0:
        return ""
    return str(abs(num - den) / den)


def percent_unified_minus_tao_over_unified(
    open_jobs_unified_db: object,
    open_jobs_tao_db: object,
) -> str:
    """Value for Job Ingestion column *Delta Unified − Tao (%)*.

    Formula (same as header labels in ``JOB_INGESTION_HEADERS``)::

        ((Open Jobs in Unified DB − Open Jobs in Tao DB) / Open Jobs in Unified DB) × 100

    In code, ``open_jobs_unified_db`` / ``open_jobs_tao_db`` are the query scalars
    ``ji_unified_open`` / ``ji_tao_open`` (same numbers written to columns C and D).

    We write only the **ratio** ``(unified − tao) / unified``; conditional formatting
    and the sheet's ``PERCENT`` (``0.00%``) format handle the ×100 display, like Excel.

    Counts are rounded to whole jobs before the ratio so tiny float noise (or drivers
    returning ``564.0`` vs ``563.999…``) does not explode the percentage. If column G
    still shows millions of percent, clear any manual formula there (e.g. ``=C2/(C2-D2)``
    inverts the ratio when the difference is near zero).
    """
    u = to_number(open_jobs_unified_db)
    t = to_number(open_jobs_tao_db)
    if u is None or t is None:
        return ""
    u_i = int(round(u))
    t_i = int(round(t))
    if u_i <= 0:
        return ""
    ratio_unified_tao = (u_i - t_i) / u_i
    return _format_ratio_for_percent_cell(ratio_unified_tao)


def ratio(value: object, total: object) -> str:
    num = to_number(value)
    den = to_number(total)
    if num is None or den is None or den == 0:
        return ""
    return str(num / den)


def fetch_funnel_crm_counts(
    conn: object,
    report_date: str,
    *,
    tao_client_id: str,
    sponsored_only: bool,
) -> dict[str, int]:
    sponsored_clause = (
        "AND csv2.source_metadata ->> 'originSourceL1' = 'SPONSORED'"
        if sponsored_only
        else ""
    )
    sql = f"""
SELECT
  aps.crm_stage,
  COUNT(DISTINCT csv2.application_id)::int AS application_count
FROM candidate_application_stages cas
JOIN candidate_submissions_v2 csv2
  ON csv2.application_id::uuid = cas.application_id
JOIN application_stages aps
  ON cas.stage_id = aps.id
WHERE cas.is_deleted = false
  AND csv2.client_id = '{tao_client_id}'
  {sponsored_clause}
  AND DATE(cas.start_date) = DATE '__REPORT_DATE__'
GROUP BY aps.crm_stage
ORDER BY application_count DESC, aps.crm_stage
""".strip().replace("__REPORT_DATE__", report_date)
    raw_rows = fetch_all(conn, sql)
    counts: dict[str, int] = {}
    for crm_stage, application_count in raw_rows:
        key = "" if crm_stage is None else str(crm_stage).strip()
        if not key:
            continue
        counts[key] = int(application_count or 0)
    return counts


def split_mapped_crm_stages(mapping: str) -> list[str]:
    return [part.strip() for part in mapping.split(",") if part.strip()]


def build_funnel_table_rows(
    report_date: str,
    mojo_stages: list[dict[str, object]],
    crm_all_counts: dict[str, int],
    crm_sponsored_counts: dict[str, int],
) -> list[list[str]]:
    rows: list[list[str]] = []
    for stage in mojo_stages:
        mapping = str(stage.get("mapping") or "").strip()
        mapped_crm_stages = split_mapped_crm_stages(mapping)
        crm_all_total = sum(crm_all_counts.get(name, 0) for name in mapped_crm_stages)
        crm_sponsored_total = sum(
            crm_sponsored_counts.get(name, 0) for name in mapped_crm_stages
        )
        mojo_count = int(stage.get("count") or 0)
        if crm_sponsored_total == 0:
            delta_pct = 0.0 if mojo_count == 0 else 1.0
        else:
            delta_pct = abs(crm_sponsored_total - mojo_count) / crm_sponsored_total
        rows.append(
            [
                report_date,
                str(stage.get("key") or ""),
                mapping,
                str(crm_all_total),
                str(crm_sponsored_total),
                str(mojo_count),
                str(delta_pct),
            ]
        )
    return rows


def main() -> None:
    load_local_env()
    jdate = job_date_string()  # today  — Job Ingestion tab
    rdate = report_date_string()  # yesterday — Mojo Apply + Funnel tabs
    client_id = (os.environ.get("HYPERCARE_CLIENT") or "").strip()
    if not client_id:
        raise SystemExit("Set HYPERCARE_CLIENT in .env before running hypercare.")

    client_cfg = load_client_config(client_id)
    set_client_runtime_defaults(client_cfg)
    active_trackers = set(enabled_tabs(client_id))
    lifecycle = hypercare_status(client_id)
    lifecycle_text = (
        f"status={lifecycle['status']} "
        f"go_live_date={lifecycle['go_live_date'] or 'missing'} "
        f"day_number={lifecycle['day_number'] if lifecycle['day_number'] is not None else 'n/a'} "
        f"hypercare_days={lifecycle['hypercare_days']}"
    )

    sheets = SheetsClient()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not lifecycle["is_active"] and lifecycle["status"] != "missing_go_live_date":
        message = (
            f"Skipped: hypercare window inactive for client {client_id} "
            f"({lifecycle_text})"
        )
        update_overview_status(
            sheets,
            last_run_utc=now_utc,
            status_text=message,
            lifecycle_text=lifecycle_text,
        )
        append_run_log(
            sheets,
            client_id=client_id,
            run_scope="lifecycle",
            query_id="hypercare_window",
            db="system",
            status="skipped",
            detail=message,
            at_utc=now_utc,
        )
        print(message)
        return

    try:
        unified_cfg = unified_config_from_env()
        tao_cfg = tao_config_from_env()
    except OSError as e:
        sheets_err = SheetsClient()
        update_overview_status(
            sheets_err,
            last_run_utc="(not run — fix .env)",
            status_text=f"Missing DB config: {e}",
            lifecycle_text=lifecycle_text,
        )
        print(f"Configure database environment variables first: {e}", file=sys.stderr)
        raise SystemExit(2) from e

    reg = sheets.get_range(f"'{TAB_QUERY_REGISTRY}'!A2:F500")
    if not reg:
        print("No rows in Query registry.")
        return

    ok_count = err_count = rej_count = 0
    crm_all_counts: dict[str, int] = {}
    crm_sponsored_counts: dict[str, int] = {}
    funnel_crm_error = ""
    registry_results: dict[str, str] = {}
    website_open: int | None = None
    website_error = ""
    sponsored_applies: int | None = None
    sponsored_applies_error = ""
    sponsored_stage_counts: list[dict[str, object]] = []
    funnel_stage_error = ""
    open_count: int | None = None
    mojo_open_error = ""

    if TRACKER_JOB_INGESTION in active_trackers:
        try:
            website_open = fetch_website_open_jobs()
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_JOB_INGESTION,
                query_id="website_open_jobs",
                db="ats",
                status="ok",
                detail=f"website_open={website_open}",
            )
        except WebsiteJobsConfigError as e:
            website_error = f"config: {e}"
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_JOB_INGESTION,
                query_id="website_open_jobs",
                db="ats",
                status="skipped",
                detail=website_error,
            )
        except Exception as e:
            err_count += 1
            website_error = build_error_detail(
                "website_open_jobs",
                e,
                provider=os.environ.get("ATS_PROVIDER", ""),
            )
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_JOB_INGESTION,
                query_id="website_open_jobs",
                db="ats",
                status="error",
                detail=website_error,
            )

    with readonly_connection(unified_cfg) as unified_conn, readonly_connection(
        tao_cfg
    ) as tao_conn:
        for row in reg:
            row = pad_registry_row(row)
            if not str(row[0]).strip():
                break
            qid = str(row[0]).strip()
            db = str(row[1]).strip().lower()
            desc = str(row[2]).strip() if len(row) > 2 else ""
            sql = str(row[3]).strip() if len(row) > 3 else ""
            out_sheet = str(row[4]).strip() if len(row) > 4 else ""
            out_cell = str(row[5]).strip() if len(row) > 5 else ""

            if not sql or not out_sheet or not out_cell:
                continue

            tracker_key = tracker_for_sheet(out_sheet)
            if tracker_key and tracker_key not in active_trackers:
                append_run_log(
                    sheets,
                    client_id=client_id,
                    run_scope=tracker_key,
                    query_id=qid,
                    db=db,
                    status="skipped",
                    detail=f"tracker disabled; {desc}",
                )
                continue

            date_for_query = jdate if out_sheet == TAB_JOB else rdate
            sql_filled = sql.replace("__REPORT_DATE__", date_for_query)
            log_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            try:
                assert_read_only_sql(sql_filled)
            except WriteQueryNotAllowed as e:
                rej_count += 1
                append_run_log(
                    sheets,
                    client_id=client_id,
                    run_scope=tracker_key or "query_registry",
                    query_id=qid,
                    db=db,
                    status="rejected",
                    detail=str(e),
                    at_utc=log_ts,
                )
                continue

            conn = unified_conn if db == "unified" else tao_conn if db == "tao" else None
            if conn is None:
                err_count += 1
                append_run_log(
                    sheets,
                    client_id=client_id,
                    run_scope=tracker_key or "query_registry",
                    query_id=qid,
                    db=db,
                    status="bad_db",
                    detail="db must be unified or tao",
                    at_utc=log_ts,
                )
                continue

            try:
                raw_rows = fetch_all(conn, sql_filled)
                values = normalize_rows(raw_rows)
                registry_results[qid] = first_scalar(values)
                ok_count += 1
                append_run_log(
                    sheets,
                    client_id=client_id,
                    run_scope=tracker_key or "query_registry",
                    query_id=qid,
                    db=db,
                    status="ok",
                    detail=f"{len(values)} row(s); {desc}",
                    at_utc=log_ts,
                )
            except Exception as e:
                err_count += 1
                detail = build_error_detail(
                    "query_registry",
                    e,
                    query_id=qid,
                    db=db,
                    output_sheet=out_sheet,
                    output_cell=out_cell,
                    report_date=date_for_query,
                )
                append_run_log(
                    sheets,
                    client_id=client_id,
                    run_scope=tracker_key or "query_registry",
                    query_id=qid,
                    db=db,
                    status="error",
                    detail=detail,
                    at_utc=log_ts,
                )

        if TRACKER_FUNNEL_TRACKING in active_trackers:
            tao_client_id = client_cfg["db"]["tao"]["client_id"]
            try:
                crm_all_counts = fetch_funnel_crm_counts(
                    tao_conn,
                    rdate,
                    tao_client_id=tao_client_id,
                    sponsored_only=False,
                )
                crm_sponsored_counts = fetch_funnel_crm_counts(
                    tao_conn,
                    rdate,
                    tao_client_id=tao_client_id,
                    sponsored_only=True,
                )
                append_run_log(
                    sheets,
                    client_id=client_id,
                    run_scope=TRACKER_FUNNEL_TRACKING,
                    query_id="funnel_crm_counts",
                    db="tao",
                    status="ok",
                    detail=f"crm_all={len(crm_all_counts)} crm_sponsored={len(crm_sponsored_counts)}",
                )
            except Exception as e:
                err_count += 1
                funnel_crm_error = build_error_detail(
                    "funnel_crm_counts",
                    e,
                    report_date=rdate,
                    target=f"{TAB_FUNNEL}!D:E",
                )
                append_run_log(
                    sheets,
                    client_id=client_id,
                    run_scope=TRACKER_FUNNEL_TRACKING,
                    query_id="funnel_crm_counts",
                    db="tao",
                    status="error",
                    detail=funnel_crm_error,
                )

    finished = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if TRACKER_MOJO_APPLY in active_trackers:
        try:
            sponsored_applies = fetch_sponsored_applies(rdate)
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_MOJO_APPLY,
                query_id="mojo_sponsored_applies",
                db="mojo",
                status="ok",
                detail=f"applies={sponsored_applies}",
                at_utc=finished,
            )
        except (MojoConfigError, Exception) as e:
            err_count += 1
            sponsored_applies_error = build_error_detail(
                "mojo_sponsored_applies",
                e,
                report_date=rdate,
            )
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_MOJO_APPLY,
                query_id="mojo_sponsored_applies",
                db="mojo",
                status="error",
                detail=sponsored_applies_error,
                at_utc=finished,
            )

    if TRACKER_FUNNEL_TRACKING in active_trackers:
        try:
            sponsored_stage_counts = fetch_sponsored_stage_counts(rdate)
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_FUNNEL_TRACKING,
                query_id="mojo_funnel_sponsored",
                db="mojo",
                status="ok",
                detail=f"{len(sponsored_stage_counts)} stage(s)",
                at_utc=finished,
            )
        except (MojoConfigError, Exception) as e:
            err_count += 1
            funnel_stage_error = build_error_detail(
                "mojo_funnel_sponsored",
                e,
                report_date=rdate,
            )
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_FUNNEL_TRACKING,
                query_id="mojo_funnel_sponsored",
                db="mojo",
                status="error",
                detail=funnel_stage_error,
                at_utc=finished,
            )

    if TRACKER_JOB_INGESTION in active_trackers:
        try:
            open_count, _latest_ist = fetch_open_jobs_metrics()
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_JOB_INGESTION,
                query_id="mojo_open_jobs",
                db="mojo",
                status="ok",
                detail=f"open_jobs={open_count}",
                at_utc=finished,
            )
        except (MojoConfigError, Exception) as e:
            err_count += 1
            mojo_open_error = build_error_detail(
                "mojo_open_jobs",
                e,
                report_date=jdate,
            )
            append_run_log(
                sheets,
                client_id=client_id,
                run_scope=TRACKER_JOB_INGESTION,
                query_id="mojo_open_jobs",
                db="mojo",
                status="error",
                detail=mojo_open_error,
                at_utc=finished,
            )

    if TRACKER_JOB_INGESTION in active_trackers:
        job_row = [
            jdate,
            format_metric_value(website_open if website_error == "" else f"ERROR: {website_error}"),
            registry_results.get("ji_unified_open", ""),
            registry_results.get("ji_tao_open", ""),
            format_metric_value(open_count if mojo_open_error == "" else f"ERROR: {mojo_open_error}"),
            percent_delta(open_count, website_open),
            # Delta Unified − Tao (%): abs(Tao − Unified) / Unified — same as ATS−Mojo logic
            percent_delta(
                registry_results.get("ji_tao_open", ""),
                registry_results.get("ji_unified_open", ""),
            ),
            registry_results.get("ji_unified_open_latest", ""),
            registry_results.get("ji_unified_closed_latest", ""),
            registry_results.get("ji_mojo_tao_null_mapping", ""),
        ]
        job_target, _job_exists = find_or_next_row(sheets, TAB_JOB, date_val=jdate, end_col="J")
        col_end = idx_to_col(len(job_row))
        sheets.update_range(f"'{TAB_JOB}'!A{job_target}:{col_end}{job_target}", [job_row])

    if TRACKER_MOJO_APPLY in active_trackers:
        mojo_api_value: object = sponsored_applies
        if sponsored_applies_error:
            mojo_api_value = f"ERROR: {sponsored_applies_error}"
        mojo_row = [
            rdate,
            format_metric_value(mojo_api_value),
            registry_results.get("mojo_sponsored_tao", ""),
            percent_delta(registry_results.get("mojo_sponsored_tao", ""), sponsored_applies),
            registry_results.get("mojo_total_tao", ""),
            registry_results.get("mojo_crm_creation_failed", ""),
            registry_results.get("mojo_ats_rejected", ""),
            ratio(
                registry_results.get("mojo_ats_rejected", ""),
                registry_results.get("mojo_total_tao", ""),
            ),
        ]

        # --- Cumulative columns (I–O): sum of daily values from hypercare start to current date ---
        # Mojo Apply dates start at go_live_date - 1 (report_date convention).
        # Read ALL existing rows including the one we're about to write/update,
        # then sum everything up to and including rdate.
        existing_rows = sheets.get_range(f"'{TAB_MOJO}'!A2:O{1000}")
        # Columns to sum: B(1) C(2) E(4) F(5) G(6) — indices within each row
        _SUM_COLS = [1, 2, 4, 5, 6]
        cum_sums = {c: 0.0 for c in _SUM_COLS}
        rdate_parsed = _parse_date_loose(rdate)
        for erow in existing_rows:
            if not erow or not str(erow[0]).strip():
                continue
            row_date = _parse_date_loose(str(erow[0]).strip())
            if row_date is None:
                continue
            # Skip the current date row — we'll add today's fresh values below
            if _dates_equal(str(erow[0]), rdate):
                continue
            # Only include rows up to (but not including) current date
            if rdate_parsed is not None and row_date > rdate_parsed:
                continue
            for c in _SUM_COLS:
                if c < len(erow):
                    v = to_number(erow[c])
                    if v is not None:
                        cum_sums[c] += v
        # Add current day's values from the row we just built
        for c in _SUM_COLS:
            v = to_number(mojo_row[c])
            if v is not None:
                cum_sums[c] += v

        cum_sponsored_mojo = cum_sums[1]
        cum_sponsored_tao = cum_sums[2]
        cum_total_crm = cum_sums[4]
        cum_crm_failed = cum_sums[5]
        cum_ats_rejected = cum_sums[6]
        # Recompute percentages from cumulative sums (not sum of daily %)
        cum_delta_pct = percent_delta(cum_sponsored_tao, cum_sponsored_mojo)
        cum_rejected_pct = ratio(cum_ats_rejected, cum_total_crm)

        mojo_row.extend([
            format_metric_value(int(cum_sponsored_mojo)),
            format_metric_value(int(cum_sponsored_tao)),
            cum_delta_pct,
            format_metric_value(int(cum_total_crm)),
            format_metric_value(int(cum_crm_failed)),
            format_metric_value(int(cum_ats_rejected)),
            cum_rejected_pct,
        ])

        mojo_target, _mojo_exists = find_or_next_row(sheets, TAB_MOJO, date_val=rdate, end_col="O")
        col_end_m = idx_to_col(len(mojo_row))
        sheets.update_range(f"'{TAB_MOJO}'!A{mojo_target}:{col_end_m}{mojo_target}", [mojo_row])

    if TRACKER_FUNNEL_TRACKING in active_trackers:
        if funnel_crm_error:
            funnel_rows = [[rdate, "ERROR", funnel_crm_error, "", "", "", ""]]
        elif funnel_stage_error:
            funnel_rows = [[rdate, "ERROR", funnel_stage_error, "", "", "", ""]]
        else:
            funnel_rows = build_funnel_table_rows(
                rdate,
                sponsored_stage_counts,
                crm_all_counts,
                crm_sponsored_counts,
            )
        if funnel_rows:
            funnel_start, existing_len = find_funnel_block_start(sheets, TAB_FUNNEL, date_val=rdate)
            # Write blank separator row for new date blocks (funnel_start > 2 means
            # there is existing data above; the blank goes at funnel_start - 1)
            if existing_len == 0 and funnel_start > 2:
                sheets.update_range(
                    f"'{TAB_FUNNEL}'!A{funnel_start - 1}:G{funnel_start - 1}",
                    [[""] * 7],
                )
            funnel_end_row = funnel_start + len(funnel_rows) - 1
            sheets.update_range(
                f"'{TAB_FUNNEL}'!A{funnel_start}:G{funnel_end_row}",
                funnel_rows,
            )
            # If the re-run block is shorter than the stored one, clear leftover rows
            if existing_len > len(funnel_rows):
                blank = [[""] * 7] * (existing_len - len(funnel_rows))
                clear_start = funnel_start + len(funnel_rows)
                sheets.update_range(
                    f"'{TAB_FUNNEL}'!A{clear_start}:G{clear_start + len(blank) - 1}",
                    blank,
                )

    finished = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    update_overview_status(
        sheets,
        last_run_utc=finished,
        status_text=f"ok={ok_count}  error={err_count}  rejected={rej_count}  (job date {jdate} / apply date {rdate})",
        lifecycle_text=lifecycle_text,
    )

    print(
        f"Done. ok={ok_count} error={err_count} rejected={rej_count}. "
        f"See Run log and Overview → Last run."
    )


if __name__ == "__main__":
    main()
