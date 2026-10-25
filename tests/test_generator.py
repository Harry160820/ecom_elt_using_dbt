"""Tests for the source-system simulator. These protect the properties the rest of the
pipeline silently depends on: determinism, consistency between tables, and real changes."""

import csv
from datetime import timedelta
from decimal import Decimal

import pytest

from ingestion.source_generator import (
    START_DATE,
    TABLE_COLUMNS,
    customers_for_day,
    order_items_for_day,
    orders_for_day,
    payments_for_day,
    write_day,
)

SEED = 42


def read(path):
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def test_same_seed_and_date_gives_identical_files(tmp_path):
    write_day(START_DATE + timedelta(days=3), tmp_path / "a", SEED)
    write_day(START_DATE + timedelta(days=3), tmp_path / "b", SEED)
    for table in TABLE_COLUMNS:
        a = (tmp_path / "a" / "dt=2024-01-04" / f"{table}.csv").read_bytes()
        b = (tmp_path / "b" / "dt=2024-01-04" / f"{table}.csv").read_bytes()
        assert a == b, table


def test_files_have_agreed_columns_and_success_marker(tmp_path):
    write_day(START_DATE, tmp_path, SEED)
    out = tmp_path / "dt=2024-01-01"
    assert (out / "_SUCCESS").exists()
    for table, columns in TABLE_COLUMNS.items():
        assert list(read(out / f"{table}.csv")[0].keys()) == columns


def test_date_before_source_start_is_rejected():
    with pytest.raises(ValueError):
        customers_for_day(SEED, START_DATE - timedelta(days=1))


def test_first_day_loads_full_customer_snapshot_then_only_changes():
    assert len(customers_for_day(SEED, START_DATE)) == 200
    assert len(customers_for_day(SEED, START_DATE + timedelta(days=1))) == 5 + 3


def test_customer_changes_are_real_changes():
    """Every change row must differ in city from the customer's previous version,
    otherwise the SCD2 snapshot downstream would have nothing to record."""
    latest_city = {}
    for offset in range(30):
        for row in customers_for_day(SEED, START_DATE + timedelta(days=offset)):
            previous = latest_city.get(row["customer_id"])
            if previous is not None:
                assert row["city"] != previous
            latest_city[row["customer_id"]] = row["city"]


def test_payment_amount_equals_sum_of_valid_order_lines():
    day = START_DATE + timedelta(days=5)
    items = order_items_for_day(SEED, day, dirty_rate=0.3)
    totals = {}
    for item in items:
        if int(item["quantity"]) > 0:
            amount = Decimal(item["unit_price"]) * int(item["quantity"])
            totals[item["order_id"]] = totals.get(item["order_id"], Decimal(0)) + amount
    payments = payments_for_day(SEED, day, dirty_rate=0.3)
    assert payments
    for p in payments:
        assert Decimal(p["amount"]) == totals[p["order_id"]].quantize(Decimal("0.01"))


def test_zero_dirty_rate_means_no_duplicates_and_no_invalid_lines():
    day = START_DATE + timedelta(days=2)
    rows = orders_for_day(SEED, day, dirty_rate=0)
    keys = [(r["order_id"], r["updated_at"]) for r in rows]
    assert len(keys) == len(set(keys))
    assert all(int(i["quantity"]) > 0 for i in order_items_for_day(SEED, day, dirty_rate=0))


def test_dirty_rate_injects_duplicates_and_invalid_lines():
    day = START_DATE + timedelta(days=2)
    rows = orders_for_day(SEED, day, dirty_rate=0.5)
    keys = [(r["order_id"], r["updated_at"]) for r in rows]
    assert len(keys) > len(set(keys))
    assert any(int(i["quantity"]) <= 0 for i in order_items_for_day(SEED, day, dirty_rate=0.5))


def test_status_updates_refer_to_orders_created_on_earlier_days():
    day = START_DATE + timedelta(days=3)
    rows = orders_for_day(SEED, day, 0)
    created_today = {r["order_id"] for r in rows if r["status"] in ("placed", "cancelled")}
    updates = [r for r in rows if r["status"] in ("shipped", "delivered")]
    assert updates
    assert all(u["order_id"] not in created_today for u in updates)


def test_every_order_has_at_least_one_valid_line_even_when_dirty():
    day = START_DATE + timedelta(days=4)
    valid = {i["order_id"] for i in order_items_for_day(SEED, day, 0.9) if int(i["quantity"]) > 0}
    created = {r["order_id"] for r in orders_for_day(SEED, day, 0) if r["status"] in ("placed", "cancelled")}
    assert created <= valid
