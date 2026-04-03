#!/usr/bin/env python3
"""Run the hypercare pipeline for a specific date or the last N days.

Usage
-----
# Single date (job_date = given date, report_date = day before)
  python scripts/run_for_dates.py --date 2026-04-01

# Last N days in chronological order (oldest → newest)
  python scripts/run_for_dates.py --last 5

# Explicit date range (inclusive, oldest first)
  python scripts/run_for_dates.py --from 2026-03-28 --to 2026-04-03

Flags
-----
  --date DATE        Single job date (YYYY-MM-DD)
  --last N           Last N days ending today (inclusive), oldest first
  --from DATE        Start of date range (inclusive)
  --to DATE          End of date range (inclusive), defaults to today
  --dry-run          Print dates that would run; don't execute

Notes
-----
* job_date   = the date passed / iterated
* report_date = job_date - 1 day  (used by Mojo Apply & Funnel Tracking)
* Each date is run in order: oldest → newest so rows appear top→bottom.
* Re-running a date that already has data updates the row in-place (idempotent).
* Requires .env and config/clients/<id>.json to be set up (see README).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}'. Use YYYY-MM-DD.")


def build_date_list(args: argparse.Namespace) -> list[date]:
    today = date.today()

    if args.date:
        return [args.date]

    if args.last:
        if args.last < 1:
            sys.exit("--last must be >= 1")
        start = today - timedelta(days=args.last - 1)
        end = today
    else:
        start = args.from_date
        end = args.to_date or today

    if start > end:
        sys.exit(f"Start date {start} is after end date {end}.")

    days: list[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def run_for_date(job_date: date, *, dry_run: bool = False) -> bool:
    """Run the pipeline for one job_date. Returns True on success."""
    report_date = job_date - timedelta(days=1)
    env_extra = {
        "HYPERCARE_JOB_DATE": job_date.isoformat(),
        "HYPERCARE_REPORT_DATE": report_date.isoformat(),
    }

    label = f"job_date={job_date}  report_date={report_date}"
    if dry_run:
        print(f"[dry-run] would run: {label}")
        return True

    print(f"\n{'─'*60}")
    print(f"  Running: {label}")
    print(f"{'─'*60}")

    import os
    env = {**os.environ, **env_extra}
    result = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "run_hypercare_queries.py")],
        env=env,
        cwd=str(_ROOT),
    )
    success = result.returncode == 0
    status = "✓ ok" if success else "✗ failed"
    print(f"  {status}  ({label})")
    return success


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run hypercare pipeline for a date or date range.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        type=_date,
        help="Single job date to run.",
    )
    group.add_argument(
        "--last",
        metavar="N",
        type=int,
        help="Run for the last N days (today inclusive), oldest first.",
    )
    group.add_argument(
        "--from",
        dest="from_date",
        metavar="YYYY-MM-DD",
        type=_date,
        help="Start of date range (inclusive).",
    )

    parser.add_argument(
        "--to",
        dest="to_date",
        metavar="YYYY-MM-DD",
        type=_date,
        default=None,
        help="End of date range (inclusive). Defaults to today. Use with --from.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print dates that would run without executing.",
    )

    args = parser.parse_args()

    days = build_date_list(args)
    total = len(days)
    print(f"\nScheduled {total} run(s)  [{days[0]} → {days[-1]}]")

    failures: list[date] = []
    for i, d in enumerate(days, 1):
        print(f"\n[{i}/{total}]", end="")
        ok = run_for_date(d, dry_run=args.dry_run)
        if not ok:
            failures.append(d)

    print(f"\n{'═'*60}")
    passed = total - len(failures)
    if args.dry_run:
        print(f"  Dry run complete — {total} date(s) listed.")
    elif failures:
        print(f"  Done: {passed}/{total} succeeded.")
        print(f"  Failed dates: {', '.join(str(d) for d in failures)}")
        sys.exit(1)
    else:
        print(f"  Done: {passed}/{total} succeeded — all clear.")


if __name__ == "__main__":
    main()
