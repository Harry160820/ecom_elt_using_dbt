"""
### ecom_elt_daily

Daily ELT for the e-commerce platform.

    generate_source_data -> wait_for_landing_files -> load_raw_table (x5, mapped) -> dbt_build -> quality_report

* **generate_source_data**: simulates the upstream system dropping files (in real life this task does not exist).
* **wait_for_landing_files**: sensor that waits for the `_SUCCESS` marker, in `reschedule` mode so it frees its worker slot between checks.
* **load_raw_table**: dynamic task mapping, one task instance per table, so one failed table can be retried alone.
* **dbt_build**: models + snapshot + tests in dependency order. A failing test fails the DAG run.
* **quality_report**: rebuilds the HTML report and applies the freshness check for this run's date.

Backfill: `catchup=True` and `max_active_runs=1` make Airflow replay every missed day in order,
which the SCD2 snapshot and incremental model require.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import PokeReturnValue, dag, get_current_context, task

from ingestion.config import (
    DBT_BIN,
    DBT_PROJECT_DIR,
    DUCKDB_PATH,
    LANDING_DIR,
    REPO_ROOT,
    SOURCE_START_DATE,
    TABLES,
)
from ingestion.load_raw import load_batch
from ingestion.source_generator import partition_dir, write_day

log = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    """Failure hook. In production this would page someone (Slack, PagerDuty, email)."""
    ti = context.get("ti") or context.get("task_instance")
    log.error("ALERT: task %s failed (try %s)", getattr(ti, "task_id", "?"), getattr(ti, "try_number", "?"))


def _batch_date() -> date:
    return date.fromisoformat(get_current_context()["ds"])


@dag(
    dag_id="ecom_elt_daily",
    description="Landing files -> DuckDB raw -> dbt models, snapshot and tests",
    schedule="@daily",
    start_date=datetime.fromisoformat(SOURCE_START_DATE),
    catchup=True,
    max_active_runs=1,  # DuckDB has a single writer, and the SCD2 history must be built in order
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=20),
        "on_failure_callback": notify_failure,
    },
    tags=["ecom", "elt", "dbt"],
    doc_md=__doc__,
)
def ecom_elt_daily():
    @task
    def generate_source_data() -> dict:
        return write_day(_batch_date(), LANDING_DIR)

    @task.sensor(poke_interval=30, timeout=60 * 60, mode="reschedule")
    def wait_for_landing_files() -> PokeReturnValue:
        marker = partition_dir(_batch_date(), LANDING_DIR) / "_SUCCESS"
        log.info("checking for %s", marker)
        return PokeReturnValue(is_done=marker.exists())

    @task(max_active_tis_per_dagrun=1)  # one writer at a time into the DuckDB file
    def load_raw_table(table: str) -> int:
        return load_batch(_batch_date(), tables=[table])[table]

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {REPO_ROOT} && {DBT_BIN} build "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        ),
        env={"ECOM_DUCKDB_PATH": str(DUCKDB_PATH)},
        append_env=True,
    )

    @task
    def quality_report() -> str:
        from scripts.build_quality_report import build_report

        return str(build_report(expected_date=_batch_date().isoformat()))

    loaded = load_raw_table.expand(table=TABLES)
    generate_source_data() >> wait_for_landing_files() >> loaded >> dbt_build >> quality_report()


ecom_elt_daily()
