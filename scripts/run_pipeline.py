"""Run the whole pipeline locally, one day at a time, exactly as the Airflow DAG does.

    python -m scripts.run_pipeline --days 7            # 7 days from the source start date
    python -m scripts.run_pipeline --days 7 --fresh    # wipe landing zone + warehouse first

Per day:   generate files  ->  load to raw  ->  dbt build (models + snapshot + tests)
At the end: build the HTML quality report.

Running day by day (instead of loading everything at once) is deliberate. It exercises the
incremental model, the SCD2 snapshot and the late status updates the way a real daily schedule does.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from ingestion.config import (
    DBT_BIN,
    DBT_PROJECT_DIR,
    DUCKDB_PATH,
    LANDING_DIR,
    REPO_ROOT,
    SOURCE_START_DATE,
)
from ingestion.load_raw import load_batch
from ingestion.source_generator import write_day


def run_dbt(*args: str) -> None:
    cmd = [DBT_BIN, *args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
    env = {**os.environ, "ECOM_DUCKDB_PATH": str(DUCKDB_PATH)}
    print("   $", " ".join(cmd))
    result = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout[-6000:])
        print(result.stderr[-2000:], file=sys.stderr)
        raise SystemExit(f"dbt {args[0]} failed (exit code {result.returncode})")
    summary = [line for line in result.stdout.splitlines() if "Done. PASS=" in line]
    print("  ", summary[-1].split("  ", 1)[-1] if summary else "ok")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start", default=SOURCE_START_DATE, help="first day (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--dirty-rate", type=float, default=0.02)
    parser.add_argument("--fresh", action="store_true", help="delete landing files and warehouse first")
    parser.add_argument("--no-report", action="store_true")
    args = parser.parse_args()

    if args.fresh:
        shutil.rmtree(LANDING_DIR, ignore_errors=True)
        for suffix in ("", ".wal"):
            Path(str(DUCKDB_PATH) + suffix).unlink(missing_ok=True)
        shutil.rmtree(DBT_PROJECT_DIR / "target", ignore_errors=True)

    first = date.fromisoformat(args.start)
    began = time.time()
    for offset in range(args.days):
        day = first + timedelta(days=offset)
        print(f"== {day} ==")
        counts = write_day(day, LANDING_DIR, dirty_rate=args.dirty_rate)
        print("   generated", counts)
        print("   loaded   ", load_batch(day))
        run_dbt("build")

    if not args.no_report:
        from scripts.build_quality_report import build_report

        print("report:", build_report())
    print(f"finished {args.days} day(s) in {time.time() - began:.0f}s")


if __name__ == "__main__":
    main()
