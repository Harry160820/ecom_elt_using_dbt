"""DAG integrity tests: the cheapest, highest-value tests for an Airflow project.
They catch import errors, cycles and bad wiring before anything is deployed.
Skipped automatically when Airflow is not installed (the plain-dbt CI job does not need it)."""

import pytest

pytest.importorskip("airflow.sdk")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="orchestration/dags")


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dag_and_task_wiring(dagbag):
    dag = dagbag.dags.get("ecom_elt_daily")
    assert dag is not None
    ids = {t.task_id for t in dag.tasks}
    assert ids == {
        "generate_source_data",
        "wait_for_landing_files",
        "load_raw_table",
        "dbt_build",
        "quality_report",
    }
    assert dag.get_task("dbt_build").upstream_task_ids == {"load_raw_table"}


def test_production_settings(dagbag):
    dag = dagbag.dags.get("ecom_elt_daily")
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] >= 1
