"""Load one day's landing files into the DuckDB `raw` schema.

Design rules:

1. Idempotent: re-loading a day first deletes that day's rows (keyed on `_batch_date`) and
   inserts again, inside one transaction. Airflow retries and backfills can never duplicate data.
2. Schema is checked before loading: a changed header fails loudly instead of silently
   shifting columns.
3. Raw is append-only history plus lineage columns (`_batch_date`, `_loaded_at`, `_source_file`).
   No business logic here. Cleaning belongs in dbt staging.
4. Every load writes a row to `raw._load_audit`, which feeds the pipeline-health mart.

Usage:
    python -m ingestion.load_raw --date 2024-01-01
"""

from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path

import duckdb

from ingestion.config import DUCKDB_PATH, LANDING_DIR, TABLES
from ingestion.source_generator import TABLE_COLUMNS, partition_dir

RAW_SCHEMA = "raw"

COLUMN_TYPES: dict[str, dict[str, str]] = {
    "customers": {
        "customer_id": "VARCHAR",
        "full_name": "VARCHAR",
        "email": "VARCHAR",
        "city": "VARCHAR",
        "segment": "VARCHAR",
        "signup_date": "DATE",
        "updated_at": "TIMESTAMP",
    },
    "products": {
        "product_id": "VARCHAR",
        "product_name": "VARCHAR",
        "category": "VARCHAR",
        "unit_price": "DECIMAL(12,2)",
        "updated_at": "TIMESTAMP",
    },
    "orders": {
        "order_id": "VARCHAR",
        "customer_id": "VARCHAR",
        "order_ts": "TIMESTAMP",
        "status": "VARCHAR",
        "updated_at": "TIMESTAMP",
    },
    "order_items": {
        "order_id": "VARCHAR",
        "line_no": "INTEGER",
        "product_id": "VARCHAR",
        "quantity": "INTEGER",
        "unit_price": "DECIMAL(12,2)",
    },
    "payments": {
        "payment_id": "VARCHAR",
        "order_id": "VARCHAR",
        "method": "VARCHAR",
        "amount": "DECIMAL(12,2)",
        "status": "VARCHAR",
        "paid_at": "TIMESTAMP",
    },
}

# Guard against the two dictionaries drifting apart.
assert {t: list(c) for t, c in COLUMN_TYPES.items()} == TABLE_COLUMNS

class SchemaMismatchError(Exception):
    """The file header does not match the contract we agreed with the source."""


def _check_header(path: Path, table: str) -> None:
    with path.open(newline="") as fh:
        header = next(csv.reader(fh), [])
    expected = TABLE_COLUMNS[table]
    if header != expected:
        raise SchemaMismatchError(f"{path.name}: expected columns {expected}, got {header}")


def _ensure_tables(con: duckdb.DuckDBPyConnection, table: str) -> None:
    cols = ", ".join(f"{name} {typ}" for name, typ in COLUMN_TYPES[table].items())
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{table} "
        f"({cols}, _batch_date DATE, _loaded_at TIMESTAMP, _source_file VARCHAR)"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}._load_audit "
        "(batch_date DATE, table_name VARCHAR, row_count BIGINT, loaded_at TIMESTAMP, source_file VARCHAR)"
    )


def load_table(
    con: duckdb.DuckDBPyConnection, table: str, batch_date: date, landing_dir: Path = LANDING_DIR
) -> int:
    path = partition_dir(batch_date, landing_dir) / f"{table}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing landing file: {path}")
    _check_header(path, table)

    columns = COLUMN_TYPES[table]
    column_names = ", ".join(columns)
    column_spec = ", ".join(f"'{name}': '{typ}'" for name, typ in columns.items())

    _ensure_tables(con, table)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {RAW_SCHEMA}.{table} WHERE _batch_date = ?", [batch_date])
        con.execute(
            f"INSERT INTO {RAW_SCHEMA}.{table} "
            f"SELECT {column_names}, ?::DATE, now()::TIMESTAMP, ? "
            f"FROM read_csv(?, header = true, columns = {{{column_spec}}})",
            [batch_date, str(path), str(path)],
        )
        rows = con.execute(
            f"SELECT count(*) FROM {RAW_SCHEMA}.{table} WHERE _batch_date = ?", [batch_date]
        ).fetchone()[0]
        con.execute(
            f"DELETE FROM {RAW_SCHEMA}._load_audit WHERE batch_date = ? AND table_name = ?",
            [batch_date, table],
        )
        con.execute(
            f"INSERT INTO {RAW_SCHEMA}._load_audit VALUES (?, ?, ?, now()::TIMESTAMP, ?)",
            [batch_date, table, rows, str(path)],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return rows

def load_batch(
    batch_date: date,
    tables: list[str] | None = None,
    landing_dir: Path = LANDING_DIR,
    db_path: Path = DUCKDB_PATH,
) -> dict[str, int]:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        return {t: load_table(con, t, batch_date, landing_dir) for t in (tables or TABLES)}
    finally:
        con.close()

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", required=True, help="batch date (YYYY-MM-DD)")
    args = parser.parse_args()
    print(load_batch(date.fromisoformat(args.date)))


if __name__ == "__main__":
    main()