"""Tests for the raw loader: idempotency, fail-loud behaviour and audit trail."""

from datetime import date

import duckdb
import pytest

from ingestion.load_raw import SchemaMismatchError, load_batch
from ingestion.source_generator import START_DATE, partition_dir, write_day

DAY = START_DATE


@pytest.fixture()
def landing(tmp_path):
    write_day(DAY, tmp_path / "landing")
    return tmp_path / "landing"

def count(db, table):
    con = duckdb.connect(str(db))
    try:
        return con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    finally:
        con.close()


def test_load_returns_row_counts_and_adds_lineage_columns(landing, tmp_path):
    db = tmp_path / "w.duckdb"
    counts = load_batch(DAY, landing_dir=landing, db_path=db)
    assert counts["customers"] == 200
    con = duckdb.connect(str(db))
    row = con.execute("SELECT _batch_date, _source_file FROM raw.customers LIMIT 1").fetchone()
    con.close()
    assert row[0] == DAY
    assert row[1].endswith("customers.csv")

def test_reloading_the_same_day_does_not_duplicate(landing, tmp_path):
    db = tmp_path / "w.duckdb"
    load_batch(DAY, landing_dir=landing, db_path=db)
    first = count(db, "orders")
    load_batch(DAY, landing_dir=landing, db_path=db)
    assert count(db, "orders") == first

def test_audit_table_has_one_row_per_table_per_day(landing, tmp_path):
    db = tmp_path / "w.duckdb"
    load_batch(DAY, landing_dir=landing, db_path=db)
    load_batch(DAY, landing_dir=landing, db_path=db)
    con = duckdb.connect(str(db))
    n = con.execute("SELECT count(*) FROM raw._load_audit").fetchone()[0]
    con.close()
    assert n == 5

def test_missing_file_fails_loudly(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_batch(DAY, landing_dir=tmp_path / "empty", db_path=tmp_path / "w.duckdb")


def test_changed_header_fails_loudly_and_loads_nothing(landing, tmp_path):
    path = partition_dir(DAY, landing) / "orders.csv"
    path.write_text(path.read_text().replace("order_ts", "ordered_at", 1))
    db = tmp_path / "w.duckdb"
    with pytest.raises(SchemaMismatchError):
        load_batch(DAY, tables=["orders"], landing_dir=landing, db_path=db)


def test_two_days_accumulate(landing, tmp_path):
    write_day(date(2024, 1, 2), landing)
    db = tmp_path / "w.duckdb"
    load_batch(DAY, landing_dir=landing, db_path=db)
    after_one = count(db, "customers")
    load_batch(date(2024, 1, 2), landing_dir=landing, db_path=db)
    assert count(db, "customers") > after_one
