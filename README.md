# Hypercare Automation

Python automation that populates a per-client Google Sheets hypercare workbook from:
- PostgreSQL (`Unified` and `Tao` databases)
- Mojo APIs (open jobs, sponsored applies, funnel tracking)
- ATS career-site APIs (e.g. UKG)

Each client gets its own spreadsheet. The daily runner appends one row of data per day; re-running on the same day updates that row in place.

---

## Quick Start (after cloning)

Five files need to exist before you can run anything. None of them are in the repo
(they contain secrets or client-specific IDs).

### Step 1 — Python environment

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Step 2 — Google Sheets service account

1. In GCP Console → IAM → Service Accounts, create (or pick) a service account.
2. Keys tab → **Add Key → JSON** → download the file into the project folder.
3. Create a new Google Sheet and **share it with the service-account email** as **Editor**.

### Step 3 — Create `.env`

```bash
cp env.example .env
```

Fill in these **10 variables** — that's everything `.env` needs:

| Variable | What to put |
|---|---|
| `HYPERCARE_CLIENT` | Client ID — must match `config/clients/<id>.json` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Filename of the GCP JSON key (drop it in the project root) |
| `UNIFIED_DB_HOST` | Unified DB host |
| `UNIFIED_DB_USER` | Unified DB read-only user |
| `UNIFIED_DB_PASSWORD` | Unified DB password |
| `TAO_DB_HOST` | Tao DB host |
| `TAO_DB_USER` | Tao DB user |
| `TAO_DB_PASSWORD` | Tao DB password |
| `MOJO_ACCESS_TOKEN` | Short-lived token from `mojopro.joveo.com` DevTools (~2 h) |
| `MOJO_EMAIL` | Your Joveo email — also used as the Mojo API username |

Everything else is either a built-in default or loaded automatically from `config/clients/<id>.json`:
- DB port/name → defaults (`5432`, `unified_datastore`, `tao_db`)
- Mojo base URL → hardcoded `https://mojopro.joveo.com`
- Mojo account/agency/client IDs, jobs path → from client JSON
- Spreadsheet ID → from client JSON

### Step 4 — Create `config/clients/<id>.json`

```bash
cp config/clients/example.json config/clients/myclient.json
```

Open `config/clients/myclient.json` and replace every placeholder:

| Field | What to put |
|---|---|
| `id` | Same as the filename without `.json` (e.g. `myclient`) |
| `spreadsheet_id` | ID from the Google Sheet URL (`/spreadsheets/d/<ID>/edit`) |
| `go_live_date` | Client go-live date in `YYYY-MM-DD` |
| `ats.provider` | ATS provider key, e.g. `ukg` |
| `ats.url` | ATS job-board endpoint URL (from network tab / page source) |
| `mojo.account_id` / `agency_id` | From Mojo URL: `mojopro.joveo.com/<agency_id>/...` |
| `mojo.client_id` | UUID from Mojo URL: `.../clients/<uuid>/...` |
| `mojo.jobs_path` | `/fna-dashboard/v1/agencies/<agency_id>/jobs?page-type=CLIENT_JOBS` |
| `db.unified.customer_id` | `customer_id` used in Unified DB queries |
| `db.tao.source_id` / `client_id` | IDs used in Tao DB queries |

### Step 5 — Bootstrap the spreadsheet

Creates tabs, headers, the Query registry, and native Sheets tables.

```bash
PYTHONPATH=src .venv/bin/python scripts/bootstrap_hypercare_workbook.py
```

### Step 6 — Run

```bash
PYTHONPATH=src .venv/bin/python scripts/run_hypercare_queries.py
```

Open the Google Sheet — you should see data in row 2 of each enabled tab.

---

## Project structure

```text
Hypercare-Automation/
├── env.example                   ← template; copy to .env
├── config/
│   └── clients/
│       └── example.json          ← template; copy to <id>.json (gitignored)
├── src/
│   ├── ats/
│   │   ├── base.py
│   │   └── ukg/
│   │       └── jobs.py           ← open-job count
│   ├── client_config.py
│   ├── db_readonly.py
│   ├── google_sheets_auth.py
│   ├── hypercare_bootstrap.py
│   ├── hypercare_formatting.py
│   ├── mojo_jobs.py
│   ├── sheets_client.py
│   └── website_jobs.py
└── scripts/
    ├── run_hypercare_queries.py      ← daily runner (single run, today)
    ├── run_for_dates.py              ← run for a specific date or last N days
    ├── run_all_active_clients.py     ← loop all active clients
    ├── bootstrap_hypercare_workbook.py
    ├── verify_hypercare_sheet.py
    ├── create_client.py
    └── duplicate_tabs_for_testing.py
```

Files that are **gitignored** (you must create them locally):

| File | Why not in repo |
|---|---|
| `.env` | Contains secrets |
| `config/clients/<id>.json` | Contains spreadsheet IDs / client IDs |
| `*.json` service-account key | Contains GCP private key |

---

## Config tiers

Three tiers, strictly separated — never put secrets in JSON, never put client IDs in `.env`.

| Tier | File | Contents |
|------|------|----------|
| **Secrets** | `.env` | 10 vars: DB creds, GCP key filename, Mojo token + email |
| **Per-client** | `config/clients/<id>.json` | Sheet ID, go-live date, enabled trackers, DB/Mojo/ATS config (including URL) |

### `config/clients/<id>.json` reference

```json
{
  "id": "myclient",
  "name": "My Client Full Name",
  "spreadsheet_id": "GOOGLE_SHEET_ID",
  "go_live_date": "2026-04-01",
  "hypercare_days": 10,
  "enabled_tabs": ["job_ingestion", "mojo_apply", "funnel_tracking"],
  "owner": "owner@joveo.com",
  "client_contacts": ["stakeholder@client.com"],
  "ats": { "provider": "ukg", "url": "https://recruiting2.ultipro.com/..." },
  "mojo": {
    "account_id": "...", "agency_id": "...",
    "client_id": "...", "jobs_path": "..."
  },
  "db": {
    "unified": { "customer_id": "..." },
    "tao": { "source_id": "...", "client_id": "..." }
  }
}
```

---

## Running

### Daily run

```bash
PYTHONPATH=src .venv/bin/python scripts/run_hypercare_queries.py
```

Running the same day twice is safe — the second run updates the existing row in place.

### Backdated / simulated run

```bash
HYPERCARE_JOB_DATE=2026-03-31 HYPERCARE_REPORT_DATE=2026-03-30 \
  PYTHONPATH=src .venv/bin/python scripts/run_hypercare_queries.py
```

`HYPERCARE_JOB_DATE` sets the date for `Job Ingestion`.
`HYPERCARE_REPORT_DATE` sets the date for `Mojo Apply` and `Funnel Tracking` (defaults to one day before `HYPERCARE_JOB_DATE`).

### Run for a specific date or last N days

Use `scripts/run_for_dates.py` for backfills, replays, or rolling windows.
Dates are always run in **chronological order** (oldest → newest) so rows land in the correct order in the sheet.
Re-running a date that already has data updates the existing row in-place — no duplicates.

```bash
# Single date
PYTHONPATH=src .venv/bin/python scripts/run_for_dates.py --date 2026-04-01

# Last 5 days (today inclusive), oldest first
PYTHONPATH=src .venv/bin/python scripts/run_for_dates.py --last 5

# Explicit date range
PYTHONPATH=src .venv/bin/python scripts/run_for_dates.py --from 2026-03-28 --to 2026-04-03

# Preview what would run without executing
PYTHONPATH=src .venv/bin/python scripts/run_for_dates.py --last 7 --dry-run
```

| Flag | Description |
|---|---|
| `--date YYYY-MM-DD` | Run for one specific job date |
| `--last N` | Last N calendar days ending today (inclusive) |
| `--from YYYY-MM-DD` | Start of a date range (use with `--to`) |
| `--to YYYY-MM-DD` | End of a date range, defaults to today |
| `--dry-run` | Print dates that would run; don't execute |

> **Date mapping**: `job_date` is the date you pass; `report_date` is always `job_date − 1 day`.
> `Job Ingestion` uses `job_date`; `Mojo Apply` and `Funnel Tracking` use `report_date`.

### Run all active clients

```bash
PYTHONPATH=src .venv/bin/python scripts/run_all_active_clients.py
```

Iterates `config/clients/*.json` and runs only clients whose hypercare window is active.

### Bootstrap (re-run when sheet structure changes)

```bash
PYTHONPATH=src .venv/bin/python scripts/bootstrap_hypercare_workbook.py
# Force full re-seed of Query registry:
HYPERCARE_BOOTSTRAP_OVERWRITE=1 PYTHONPATH=src .venv/bin/python scripts/bootstrap_hypercare_workbook.py
```

### Verify

```bash
PYTHONPATH=src .venv/bin/python scripts/verify_hypercare_sheet.py
```

### Scaffold a new client

```bash
PYTHONPATH=src .venv/bin/python scripts/create_client.py \
  --client-id myclient \
  --name "My Client" \
  --provider ukg \
  --go-live-date 2026-04-01
# Then fill in the generated JSON files with real IDs
```

---

## Hypercare lifecycle

- Each client has `go_live_date` and `hypercare_days` (default 10) in its config.
- The runner appends rows only while the client is within its hypercare window.
- Historical rows are never deleted; the runner just stops appending after the window ends.
- If `go_live_date` is missing the run proceeds but `Overview` shows `missing_go_live_date`.

---

## Append-only write model

Each reporting tab uses a **date-keyed, idempotent** model:

- First run for a date → appends a new row (or stage block for Funnel Tracking).
- Re-run on the same date → updates that row/block in place. No duplicates.
- Funnel Tracking adds a blank separator row between each day's stage block.

> **Never use `sheets.append_rows(..., "'Tab'!A1")`** on native-table tabs.
> The Sheets `values.append` API inserts rows *after the table's declared end row* (e.g. row 1000),
> making them invisible. Always use `find_or_next_row` + `sheets.update_range`.

---

## Sheet layout

### `Job Ingestion`

One row per day. Date = today.

| Col | Meaning | Source |
|-----|---------|--------|
| A | Date | today |
| B | Open Jobs on ATS | ATS API |
| C | Open Jobs in Unified DB | Unified DB |
| D | Open Jobs in Tao DB | Tao DB |
| E | Open Jobs on Mojo | Mojo jobs API |
| F | Delta ATS − Mojo (%) | computed |
| G | Delta Unified − Tao (`(C−D)/C`) (%) | computed |
| H | Last OPEN job updated (IST) | Unified DB |
| I | Last CLOSED job updated (IST) | Unified DB |
| J | Null Mojo↔Tao mappings | Tao DB |

### `Mojo Apply`

One row per day. Date = yesterday (`HYPERCARE_REPORT_DATE`). Columns **A–H** are **daily** values; **I–O** are **cumulative** from the first Mojo Apply row in the sheet through the current report date (sums of **B, C, E, F, G**; **K** and **O** are **recomputed** from those cumulative totals, not a sum of daily percentages).

| Col | Meaning | Source |
|-----|---------|--------|
| A | Date | yesterday |
| B | Sponsored Applies on Mojo | Mojo publishers API |
| C | Sponsored Applies in Tao | Tao DB |
| D | Delta Mojo vs CRM (%) | computed (`abs(C−B)/B` as a ratio for `PERCENT` display) |
| E | Total Applies in CRM | Tao DB |
| F | CRM Creation Failed | Tao DB |
| G | ATS Rejected | Tao DB |
| H | ATS Rejected out of Total Applies in CRM (%) | computed (`G/E` as a ratio) |
| I | Cum. Sponsored Applies on Mojo | computed (running sum of **B**) |
| J | Cum. Sponsored Applies in Tao | computed (running sum of **C**) |
| K | Cum. Delta Mojo vs CRM (%) | computed from cumulative **J** vs **I** |
| L | Cum. Total Applies in CRM | computed (running sum of **E**) |
| M | Cum. CRM Creation Failed | computed (running sum of **F**) |
| N | Cum. ATS Rejected | computed (running sum of **G**) |
| O | Cum. ATS Rejected out of Total Applies in CRM (%) | computed from cumulative **N** vs **L** |

### `Funnel Tracking`

One block of rows per day (one row per Mojo stage), separated by a blank row. Date = yesterday (`HYPERCARE_REPORT_DATE`). Columns **A–G** are **per-stage values for that report day**; **H–K** are **cumulative** for the same stage key across all **prior** funnel rows in the sheet (through dates **before** the current report date), plus the current day — sums of **D, E, F** by `B` (stage key); **K** is **recomputed** from cumulative **I** vs **J**, not a sum of daily **G**.

| Col | Meaning | Source |
|-----|---------|--------|
| A | Date | yesterday |
| B | Mojo Stage | Mojo funnel setup API |
| C | CRM Stage Mapping | Mojo funnel setup API |
| D | CRM Count - All | Tao DB |
| E | CRM Count - Sponsored | Tao DB |
| F | Mojo Count - Sponsored | Mojo publishers API |
| G | Delta CRM Sponsored vs Mojo (%) | computed (same rule as daily `abs(E−F)/E` when `E>0`, stored as ratio for `PERCENT`) |
| H | Cum. CRM Count - All | computed (running sum of **D** per stage **B**) |
| I | Cum. CRM Count - Sponsored | computed (running sum of **E** per stage **B**) |
| J | Cum. Mojo Count - Sponsored | computed (running sum of **F** per stage **B**) |
| K | Cum. Delta CRM Sponsored vs Mojo (%) | computed from cumulative **I** vs **J** |

Stage mapping: setup `order=N` → `tthN` in the publishers API `tthStats`.

---

## Color rules

| Column(s) | Green | Yellow | Red |
|-----------|-------|--------|-----|
| `Job Ingestion!F`, `Mojo Apply!D/H/K/O`, `Funnel Tracking!G/K` | ≤ 10% | 10–25% | > 25% |
| `Job Ingestion!G` (Unified − Tao %) | ≤ 0 | — | > 0 |
| `Job Ingestion!H` and `I` (timestamps) | today | — | before today |
| `Job Ingestion!J` (null mappings) | 0 | — | ≥ 1 |

---

## Mojo token

The `MOJO_ACCESS_TOKEN` is a Cognito JWT that expires in ~2 hours.

**Critical:** must be a `mojopro` token, **not** a `TALENT_ENGAGE` token.

| Type | `productId` | Source | Works for |
|------|-------------|--------|-----------|
| **mojopro** ✅ | `mojopro` | `mojopro.joveo.com` | All Mojo APIs |
| TALENT_ENGAGE ❌ | `TALENT_ENGAGE` | Other Joveo products | Jobs only — funnel returns 401 |

**Verify before pasting:**
```bash
python3 -c "
import base64, json, sys
p = sys.argv[1].split('.')[1]; p += '=' * (-len(p) % 4)
print(json.loads(base64.urlsafe_b64decode(p))['productId'])
" YOUR_TOKEN
# must print: mojopro
```

**How to get a fresh token:**
1. Open [https://mojopro.joveo.com](https://mojopro.joveo.com) and log in.
2. DevTools → **Network** → filter XHR → click any API call.
3. Copy the `accessToken` request header value.
4. Paste into `.env` as `MOJO_ACCESS_TOKEN=<value>`.

---

## ATS API notes

ATS config lives entirely in `config/clients/<id>.json → ats`. No separate per-ATS config files.

For UKG: the request body is hardcoded in `src/ats/ukg/jobs.py`. The URL comes from `ats.url` in
the client config. No cookies or authentication headers are required.

Available ATS scripts (UKG):
- `src/ats/ukg/jobs.py` — `fetch_open_jobs()`

---

## Adding a new client

1. `cp config/clients/example.json config/clients/<id>.json` — fill in all fields,
   including `ats.url` for the ATS job-board endpoint
2. Set `HYPERCARE_CLIENT=<id>` in `.env`
3. Bootstrap → run

Or use the scaffold script:
```bash
PYTHONPATH=src .venv/bin/python scripts/create_client.py --client-id <id> --name "..." --provider ukg --go-live-date YYYY-MM-DD
```

---

## Adding a new ATS provider

1. `src/ats/<provider>/__init__.py` (empty)
2. `src/ats/<provider>/jobs.py` — implement `fetch_open_jobs() -> int` (reads `ATS_URL` env var)
3. Register in `src/ats/base.py → _dispatch()`
4. Add `ats.url` to `config/clients/<id>.json` for each client using that provider

---

## Safety

- All DB connections go through `db_readonly.py` with `default_transaction_read_only=on`.
- `assert_read_only_sql()` rejects any non-`SELECT`/`WITH` statement before execution.
- Secrets belong only in `.env` — never in JSON config files.
