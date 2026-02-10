#!/usr/bin/env python3
"""
Dreamwidth full-history batch exporter.

Additions:
- Records failed months to a persistent CSV file (failed_months.csv)
- One row per failure, appended safely
- No change to pause, back-off, or stop behaviour
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import date, datetime
from pathlib import Path
from subprocess import run

LOG = logging.getLogger("dw_batch")


def configure_logging(level: str) -> None:
    numeric = getattr(logging, level.upper(), None)
    if not isinstance(numeric, int):
        raise ValueError(f"Invalid log level: {level}")
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def month_range(start_year: int, start_month: int, end_year: int, end_month: int):
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def polite_pause(min_s: int, max_s: int, reason: str):
    delay = random.randint(min_s, max_s)
    LOG.info("Pausing %ds (%s)", delay, reason)
    time.sleep(delay)


def record_failure(
    path: Path,
    year: int,
    month: int,
    exit_code: int | None,
):
    """
    Append a failure record to failed_months.csv
    """
    timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    line = f"{year:04d},{month:02d},{timestamp},{exit_code}\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def main() -> int:
    ap = argparse.ArgumentParser(description="Batch Dreamwidth month-by-month export")
    ap.add_argument("--start-year", type=int, default=2006)
    ap.add_argument("--start-month", type=int, default=12)
    ap.add_argument("--end-year", type=int, default=date.today().year)
    ap.add_argument("--end-month", type=int, default=date.today().month)

    ap.add_argument("--cookie-file", required=True, help="cookies.txt for dreamwidth.org")
    ap.add_argument("--journal", default="", help="Journal/community name (optional)")
    ap.add_argument("--outdir", default="dw_exports")
    ap.add_argument("--log-level", default="INFO")

    ap.add_argument("--normal-pause-min", type=int, default=5)
    ap.add_argument("--normal-pause-max", type=int, default=10)
    ap.add_argument("--grumpy-pause-min", type=int, default=30)
    ap.add_argument("--grumpy-pause-max", type=int, default=60)

    ap.add_argument(
        "--failure-log",
        default="failed_months.csv",
        help="CSV file to record failed months",
    )

    args = ap.parse_args()
    configure_logging(args.log_level)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    failure_log = Path(args.failure_log)
    failures = 0

    for year, month in month_range(
        args.start_year,
        args.start_month,
        args.end_year,
        args.end_month,
    ):
        name = args.journal or "default"
        outfile = outdir / f"dw_export_{name}_{year:04d}-{month:02d}.xml"

        if outfile.exists():
            LOG.info("Skipping %04d-%02d (already exists)", year, month)
            continue

        LOG.info("Exporting %04d-%02d", year, month)

        cmd = [
            sys.executable,
            "dw-downloader.py",
            "--year", str(year),
            "--month", str(month),
            "--cookie-file", args.cookie_file,
            "--outdir", args.outdir,
        ]

        if args.journal:
            cmd.extend(["--journal", args.journal])

        result = run(cmd, capture_output=True, text=True)

        if result.returncode == 0 and outfile.exists():
            LOG.info("Success: %04d-%02d", year, month)
            failures = 0
            polite_pause(
                args.normal_pause_min,
                args.normal_pause_max,
                "normal",
            )
            continue

        # Failure path
        failures += 1
        LOG.warning(
            "Failure exporting %04d-%02d (exit=%s)",
            year, month, result.returncode
        )

        record_failure(
            failure_log,
            year,
            month,
            result.returncode,
        )

        if result.stdout:
            LOG.warning("stdout:\n%s", result.stdout.strip())
        if result.stderr:
            LOG.warning("stderr:\n%s", result.stderr.strip())

        polite_pause(
            args.grumpy_pause_min,
            args.grumpy_pause_max,
            f"grumpy (consecutive failures: {failures})",
        )

        if failures >= 5:
            LOG.error("Too many consecutive failures — stopping batch run")
            return 3

    LOG.info("Batch export complete")
    LOG.info("Failure log written to: %s", failure_log)
    return 0


if __name__ == "__main__":
    sys.exit(main())

