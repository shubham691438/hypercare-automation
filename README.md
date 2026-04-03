# Hypercare Automation

Python automation that populates a Google Sheets hypercare workbook from:
- PostgreSQL (`Unified` and `Tao`)
- Mojo APIs
- ATS career-site APIs

The current implementation supports multi-client configuration, UKG ATS ingestion, Mojo open-job/applies/funnel APIs, and native Google Sheets tables for the reporting tabs.

## Project structure

```text
Hypercare-Automation/
├── .env
├── env.example
├── config/
│   ├── clients/
│   │   ├── example.json
│   │   └── ashleyfurniture.json
│   └── ats/
│       └── ukg/
│           ├── example.json
│           └── ashleyfurniture.json
├── src/
│   ├── ats/
│   │   ├── base.py
│   │   └── ukg/jobs.py
│   ├── client_config.py
│   ├── db_readonly.py
│   ├── google_sheets_auth.py
│   ├── hypercare_bootstrap.py
│   ├── hypercare_formatting.py
│   ├── mojo_jobs.py
│   ├── sheets_client.py
│   └── website_jobs.py
└── scripts/
    ├── create_client.py
    ├── bootstrap_hypercare_workbook.py
    ├── duplicate_tabs_for_testing.py
    ├── run_all_active_clients.py
    ├── run_hypercare_queries.py
    └── verify_hypercare_sheet.py
```

## Configuration

There are three config tiers. Keep them separate.

### 1. `.env`

Secrets and local overrides only:
- DB credentials
- Google Sheets credential path
- Mojo token and identity headers
- optional date overrides

See `env.example` for the supported variables.

### 2. `config/clients/<id>.json`

Per-client non-secret settings:
- spreadsheet ID
- go-live date + hypercare window
- enabled trackers (`job_ingestion`, `mojo_apply`, `funnel_tracking`)
- ATS provider + client key
- Mojo account/agency/client IDs
- DB customer/source/client IDs

Recommended shape:

```json
{
  "id": "clientname",
  "name": "Human-readable client name",
  "spreadsheet_id": "GOOGLE_SHEET_ID_HERE",
  "go_live_date": "2026-04-01",
  "hypercare_days": 10,
  "enabled_tabs": ["job_ingestion", "mojo_apply", "funnel_tracking"],
  "owner": "owner@example.com",
  "client_contacts": ["stakeholder@example.com"],
  "ats": { "provider": "ukg", "client_key": "clientname" },
  "mojo": { "account_id": "...", "agency_id": "...", "client_id": "...", "jobs_path": "..." },
  "db": { "unified": { "customer_id": "..." }, "tao": { "source_id": "...", "client_id": "..." } }
}
```

### 3. `config/ats/<provider>/<client>.json`

Per-client ATS request config. For UKG this is URL-only.

```json
{
  "url": "https://recruiting2.ultipro.com/.../LoadSearchResults"
}
```

## Setup

### Python environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Google Sheets

1. Enable the Google Sheets API for your GCP project.
2. Create a service account or OAuth flow supported by `src/google_sheets_auth.py`.
3. Share the target spreadsheet with the service-account email as `Editor`.

### Local config

```bash
cp env.example .env
```

Then fill in:
- `HYPERCARE_CLIENT`
- `GOOGLE_APPLICATION_CREDENTIALS`
- DB credentials
- `MOJO_ACCESS_TOKEN`
- `MOJO_EMAIL`
- `MOJO_USERNAME`

## Running

### Create a new client config

```bash
PYTHONPATH=src .venv/bin/python scripts/create_client.py \
  --client-id myclient \
  --name "My Client" \
  --provider ukg \
  --go-live-date 2026-04-01
```

Then fill in the generated JSON files with the real spreadsheet/DB/Mojo/ATS values.

### Bootstrap

Creates only the tabs enabled for the active client, seeds headers, seeds the Query registry, creates native Sheets tables, and clears stale formatting.

```bash
PYTHONPATH=src .venv/bin/python scripts/bootstrap_hypercare_workbook.py
HYPERCARE_BOOTSTRAP_OVERWRITE=1 PYTHONPATH=src .venv/bin/python scripts/bootstrap_hypercare_workbook.py
```

### Daily run

Executes:
- Query-registry SQL
- ATS website job count
- Mojo applies/open-jobs/funnel APIs
- append-only historical row writes

```bash
PYTHONPATH=src .venv/bin/python scripts/run_hypercare_queries.py
```

Running the same day multiple times is safe — the second run updates the existing row in place instead of appending a duplicate.

### Backdated / simulated run

Override the date context via environment variables (useful for testing or catching up after a gap):

```bash
HYPERCARE_JOB_DATE=2026-03-02 HYPERCARE_REPORT_DATE=2026-03-01 \
  PYTHONPATH=src .venv/bin/python scripts/run_hypercare_queries.py
```

`HYPERCARE_JOB_DATE` controls the `Job Ingestion` date column.
`HYPERCARE_REPORT_DATE` controls `Mojo Apply` and `Funnel Tracking` dates (defaults to one day before `HYPERCARE_JOB_DATE`).
Re-running with the same date pair updates the existing rows rather than duplicating them.

### Run all active clients

Loops through `config/clients/*.json` and runs only clients whose hypercare window is still active.

```bash
PYTHONPATH=src .venv/bin/python scripts/run_all_active_clients.py
```

### Verify

```bash
PYTHONPATH=src .venv/bin/python scripts/verify_hypercare_sheet.py
```

## Date and timezone rules

| Area | Date used | Override |
|------|-----------|----------|
| `Job Ingestion` | today | `HYPERCARE_JOB_DATE` |
| `Mojo Apply` | yesterday | `HYPERCARE_REPORT_DATE` |
| `Funnel Tracking` | yesterday | `HYPERCARE_REPORT_DATE` |

Timestamp fields written from SQL are converted to IST.

## Hypercare lifecycle

- Use **one Google Sheet per client**.
- A client stays active for `hypercare_days` starting from `go_live_date`.
- The runner appends new daily rows while the client is active.
- Historical rows remain in the sheet after hypercare ends; the runner simply stops appending.
- If `go_live_date` is missing, the runner treats the client as runnable but marks the lifecycle as `missing_go_live_date` in `Overview`.

## Append-only write model

Each reporting tab (`Job Ingestion`, `Mojo Apply`, `Funnel Tracking`) uses a **date-keyed, idempotent append model**:

- The first run for a given date appends a new row at the bottom.
- Re-running on the same date overwrites that row in place — no duplicates.
- Funnel Tracking appends one row per Mojo stage; the full stage block is replaced when the same date is re-run.

### How row placement works

The runner uses `find_or_next_row()` (for single-row tabs) and `find_funnel_block_start()` (for Funnel Tracking) to locate the correct target row by scanning column A and comparing dates. Both helpers use `_dates_equal()`, which parses date strings before comparing, so ISO format (`2026-04-03`) and Google Sheets display format (`4/3/2026`) are treated as equal.

> **Do not use `sheets.append_rows(..., "'Tab'!A1")`** for native-table tabs. The Google Sheets `values.append` API respects the table's declared row range (e.g. `A1:I1000`) and inserts new rows _after row 1000_, making them invisible. Always use `find_or_next_row` + `sheets.update_range`.

### Bootstrap behaviour

Bootstrap (`scripts/bootstrap_hypercare_workbook.py`) only writes header rows to the reporting tabs. It does **not** pre-populate blank rows, so existing historical data is preserved across bootstrap re-runs. Use `HYPERCARE_BOOTSTRAP_OVERWRITE=1` only when you want to fully reinitialise the Query registry.

## Current sheet layout

### `Job Ingestion`

Native Sheets table: `JobIngestion`

One new row is appended per run-day.

| Column | Meaning | Source |
|------|---------|--------|
| `A` | Date | today |
| `B` | Open Jobs on ATS | ATS API |
| `C` | Open Jobs in Unified DB | Unified DB |
| `D` | Open Jobs in Tao DB | Tao DB |
| `E` | Open Jobs on Mojo | Mojo jobs API |
| `F` | Delta ATS − Mojo (%) | computed in runner |
| `G` | Last OPEN job updated (IST) | Unified DB |
| `H` | Last CLOSED job updated (IST) | Unified DB |
| `I` | Null Mojo↔Tao mappings | Tao DB |

### `Mojo Apply`

Native Sheets table: `MojoApply`

One new row is appended per run-day.

| Column | Meaning | Source |
|------|---------|--------|
| `A` | Date | yesterday |
| `B` | Sponsored Applies on Mojo | Mojo publishers API |
| `C` | Sponsored Applies in Tao | Tao DB |
| `D` | Delta Mojo vs CRM (%) | computed in runner |
| `E` | Total Applies in CRM | Tao DB |
| `F` | CRM Creation Failed | Tao DB |
| `G` | ATS Rejected | Tao DB |
| `H` | ATS Rejected out of Total Applies in CRM (%) | computed in runner |

### `Funnel Tracking`

Native Sheets table: `FunnelTracking`

One new block of rows is appended per run-day, with one row per Mojo stage.

| Column | Meaning | Source |
|--------|---------|--------|
| `A` | Date | yesterday |
| `B` | Mojo Stage | Mojo funnel setup API |
| `C` | CRM Stage Mapping | Mojo funnel setup API |
| `D` | CRM Count - All | Tao DB |
| `E` | CRM Count - Sponsored | Tao DB |
| `F` | Mojo Count - Sponsored | Mojo publishers API |
| `G` | Delta CRM Sponsored vs Mojo (%) | computed in runner |

The funnel stage mapping is derived from the Mojo funnel setup API:
- setup `order=1` maps to `tth1`
- setup `order=2` maps to `tth2`
- and so on

Then the publishers API `summary.summary.tthStats` provides the stage counts.

## Color rules

### Shared percent thresholds

Used for:
- `Job Ingestion!F2:F`
- `Mojo Apply!D2:D`
- `Mojo Apply!H2:H`
- `Funnel Tracking!G2:G`

Thresholds:
- green: `<= 10%`
- yellow: `> 10%` and `<= 25%`
- red: `> 25%`

### `Job Ingestion`

- `G:G` and `H:H`
  - green: timestamp is today
  - red: timestamp is before today
- `I:I`
  - green: `0`
  - red: `> 0`

## Mojo API notes

`src/mojo_jobs.py` currently handles:
- open-job count
- latest Mojo job date proxy
- sponsored applies
- funnel stage setup and sponsored funnel counts

Required `.env` values:
- `MOJO_BASE_URL`
- `MOJO_ACCESS_TOKEN`
- `MOJO_EMAIL`
- `MOJO_USERNAME`

### Token type — CRITICAL

There are **two different Cognito JWT types** issued by Joveo. You must always use the **`mojopro` token**, not the `TALENT_ENGAGE` token.

| Token type | `productId` in JWT | Source | Works for |
|------------|-------------------|--------|-----------|
| **mojopro** ✅ | `mojopro` | `mojopro.joveo.com` | All Mojo APIs |
| TALENT_ENGAGE ❌ | `TALENT_ENGAGE` | Other Joveo products | Jobs/publishers only — **funnel-tracking returns 401** |

The `funnel-tracking` API strictly checks `productId: mojopro`. The jobs endpoint is more lenient and may accept either type, which is why job counts succeed but `Funnel Tracking` fails in the same run when the wrong token type is used.

**Verify token type before pasting:**
```bash
python3 -c "
import base64, json, sys
p = sys.argv[1].split('.')[1]; p += '=' * (-len(p) % 4)
print(json.loads(base64.urlsafe_b64decode(p))['productId'])
" YOUR_TOKEN
# must print: mojopro
```

### How to get a fresh `mojopro` token

1. Open [https://mojopro.joveo.com](https://mojopro.joveo.com) and log in.
2. Open DevTools → **Network** tab → filter by Fetch/XHR.
3. Navigate to Funnel Tracking (or any Mojo page that makes API calls).
4. Click any request to `mojopro.joveo.com/...` → **Request Headers**.
5. Copy the `accessToken` header value.
6. Paste into `.env` as `MOJO_ACCESS_TOKEN=<value>`.

The token is short-lived (~2 hours). When Mojo-backed cells fail with `401` or `Invalid value for: header accessToken`, check **both** expiry and token type. Then rerun the script.

Mojo-backed areas currently include:
- `Job Ingestion!E` (Open Jobs on Mojo — all historical rows)
- `Mojo Apply!B` (Sponsored Applies on Mojo — all historical rows)
- `Funnel Tracking!B/F` (stage names + sponsored counts — all rows)

## ATS API notes

ATS requests should stay minimal.

For UKG:
- `config/ats/ukg/<client>.json` contains only the URL
- the request body is hardcoded in `src/ats/ukg/jobs.py`
- no cookies, XSRF tokens, `origin`, or `referer` are required

The runner loads `ATS_PROVIDER` and `ATS_CLIENT` from `config/clients/<id>.json`.

## Query registry rules

The Query registry remains the source of truth for SQL-backed sheet outputs.

- only `SELECT` / `WITH` are allowed
- use `__REPORT_DATE__` in SQL for date substitution
- keep output cell mappings in sync with the reporting-tab layouts

Direct Python writes are still used for:
- Mojo API results
- ATS API results
- formulas
- funnel stage comparison rows

## Adding a new client

1. Copy `config/clients/example.json` to `config/clients/<id>.json`
2. Fill in spreadsheet, ATS, Mojo, and DB IDs
3. Copy the provider ATS example file if needed
4. Set `HYPERCARE_CLIENT=<id>` in `.env`
5. Bootstrap the workbook
6. Run the daily script

## Adding a new ATS provider

1. Create `src/ats/<provider>/jobs.py`
2. Implement `fetch_open_jobs(...)`
3. Add a config template under `config/ats/<provider>/example.json`
4. Add provider routing in `src/ats/base.py`
5. Document only truly required new env vars in `env.example`

## Safety

- DB access is read-only via `db_readonly.py`
- `assert_read_only_sql()` blocks non-read queries
- secrets belong only in `.env`
- client/ATS JSON files must stay non-secret
