"""A deterministic simulator of an e-commerce operational database.

Why simulate? Real pipelines ingest from systems you do not control. A simulator lets
you reproduce *realistic problems* on demand, for free:

* customers move city / change segment      -> needs SCD Type 2 history
* product prices change                     -> order lines must keep the price at purchase time
* orders change status after creation       -> needs incremental processing with updates
* some rows arrive duplicated or invalid    -> needs deduplication and quarantine

Everything is a pure function of (seed, date). Running it twice yields identical files, so
tests, CI and Airflow retries/backfills are all reproducible.

Usage:
    python -m ingestion.source_generator --date 2024-01-01 --days 5
"""

from __future__ import annotations

import csv
import os
import random
import argparse
from datetime import date, timedelta, datetime, time
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
from functools import cache 

from ingestion.config import LANDING_DIR, TABLES, SOURCE_START_DATE

START_DATE = date.fromisoformat(SOURCE_START_DATE)
DEFAULT_SEED = 42

INITIAL_CUSTOMERS = 200
NEW_CUSTOMERS_PER_DAY = 5
CUSTOMER_CHANGES_PER_DAY = 3
N_PRODUCTS = 50
PRICE_CHANGES_PER_DAY = 2

TABLE_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "full_name", "email", "city", "segment", "signup_date", "updated_at"],
    "products": ["product_id", "product_name", "category", "unit_price", "updated_at"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "updated_at"],
    "order_items": ["order_id", "line_no", "product_id", "quantity", "unit_price"],
    "payments": ["payment_id", "order_id", "method", "amount", "status", "paid_at"],
}

FIRST_NAMES = [
    "Aarav",
    "Diya",
    "Vihaan",
    "Anaya",
    "Aditya",
    "Ishita",
    "Kabir",
    "Meera",
    "Rohan",
    "Saanvi",
    "Arjun",
    "Kavya",
    "Neel",
    "Riya",
    "Yash",
    "Tara",
]
LAST_NAMES = [
    "Sharma",
    "Verma",
    "Iyer",
    "Reddy",
    "Nair",
    "Gupta",
    "Mehta",
    "Singh",
    "Das",
    "Patel",
    "Kapoor",
    "Joshi",
    "Bose",
    "Menon",
    "Rao",
    "Khan",
]
CITIES = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Ahmedabad",
    "Jaipur",
    "Lucknow",
]

SEGMENTS = ["standard", "premium", "vip"]
CATEGORIES = ["electronics", "fashion", "home", "beauty", "books", "sports"]
ADJECTIVES = ["Classic", "Smart", "Eco", "Pro", "Compact", "Deluxe", "Urban", "Prime"]
NOUNS = ["Lamp", "Backpack", "Headset", "Notebook", "Bottle", "Mixer", "Watch", "Mat"]
PAYMENT_METHODS = ["upi", "card", "netbanking", "cod"]

CENT = Decimal("0.01")

def _money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)

def day_index(day: date) -> int:
    idx = (day - START_DATE).days

    if idx < 0:
        raise ValueError(f"day {day} is before START_DATE {START_DATE}")
    return idx

def _midnight(day: date) -> datetime:
    return datetime.combine(day, time())

# --------------------------------------------------------------------------- customers
def customers_count(idx: int) -> int:
    return INITIAL_CUSTOMERS + NEW_CUSTOMERS_PER_DAY * idx


def _created_idx(n: int) -> int:
    if n <= INITIAL_CUSTOMERS:
        return 0
    return (n - INITIAL_CUSTOMERS - 1) // NEW_CUSTOMERS_PER_DAY + 1

def _initial_customer(seed: int, n: int) -> dict:
    rng = random.Random(f"{seed}-cust-{n}")
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    return {
        "full_name": f"{first} {last}",
        "email": f"{first}.{last}{n}@example.com".lower(),
        "city": rng.choice(CITIES),
        "segment": rng.choices(SEGMENTS, weights=[70, 22, 8])[0],
    }


@cache
def _customer_changes(seed: int, upto_idx: int) -> dict[int, list[tuple[int, str, str]]]:
    """Replay every customer change from day 1..upto_idx (iteratively, never recursively).

    The city always differs from the previous one, so each change is a *real* change
    and produces exactly one new SCD2 version downstream."""
    state: dict[int, tuple[str, str]] = {}
    changes: dict[int, list[tuple[int, str, str]]] = {}
    for idx in range(1, upto_idx + 1):
        rng = random.Random(f"{seed}-chg-{idx}")
        pool = customers_count(idx - 1)
        today = []
        for n in rng.sample(range(1, pool + 1), CUSTOMER_CHANGES_PER_DAY):
            base = _initial_customer(seed, n)
            city, _segment = state.get(n, (base["city"], base["segment"]))
            new_city = rng.choice([c for c in CITIES if c != city])
            new_segment = rng.choice(SEGMENTS)
            state[n] = (new_city, new_segment)
            today.append((n, new_city, new_segment))
        changes[idx] = today
    return changes


def _customer_row(seed: int, n: int, day: date, city: str, segment: str) -> dict:
    base = _initial_customer(seed, n)
    return {
        "customer_id": f"C{n:05d}",
        "full_name": base["full_name"],
        "email": base["email"],
        "city": city,
        "segment": segment,
        "signup_date": (START_DATE + timedelta(days=_created_idx(n))).isoformat(),
        "updated_at": _midnight(day).isoformat(sep=" "),
    }

def customers_for_day(seed: int, day: date) -> list[dict]:
    idx = day_index(day)
    first_new = 1 if idx == 0 else customers_count(idx - 1) + 1
    rows = []
    for n in range(first_new, customers_count(idx) + 1):
        base = _initial_customer(seed, n)
        rows.append(_customer_row(seed, n, day, base["city"], base["segment"]))
    if idx > 0:
        for n, city, segment in _customer_changes(seed, idx)[idx]:
            rows.append(_customer_row(seed, n, day, city, segment))
    return rows


# --------------------------------------------------------------------------- products
@cache
def _product_prices(seed: int, upto_idx: int) -> tuple[dict[int, Decimal], list[tuple[int, Decimal]]]:
    """Return (prices as of end of day upto_idx, price changes that happened on upto_idx)."""
    prices = {}
    for n in range(1, N_PRODUCTS + 1):
        rng = random.Random(f"{seed}-prod-{n}")
        rng.choice(CATEGORIES)  # keep draw order identical to _product_base
        prices[n] = _money(rng.uniform(5, 500))
    today: list[tuple[int, Decimal]] = []
    for idx in range(1, upto_idx + 1):
        rng = random.Random(f"{seed}-price-{idx}")
        today = []
        for n in rng.sample(range(1, N_PRODUCTS + 1), PRICE_CHANGES_PER_DAY):
            prices[n] = _money(prices[n] * Decimal(str(round(rng.uniform(0.85, 1.15), 4))))
            today.append((n, prices[n]))
    return dict(prices), today

def _product_base(seed: int, n: int) -> tuple[str, str]:
    rng = random.Random(f"{seed}-prod-{n}")
    category = rng.choice(CATEGORIES)
    rng2 = random.Random(f"{seed}-pname-{n}")
    return f"{rng2.choice(ADJECTIVES)} {rng2.choice(NOUNS)} {n}", category

def products_for_day(seed: int, day: date) -> list[dict]:
    idx = day_index(day)
    prices, changed = _product_prices(seed, idx)
    ids_and_prices = list(prices.items()) if idx == 0 else changed
    rows = []
    for n, price in ids_and_prices:
        name, category = _product_base(seed, n)
        rows.append(
            {
                "product_id": f"P{n:04d}",
                "product_name": name,
                "category": category,
                "unit_price": str(price),
                "updated_at": _midnight(day).isoformat(sep=" "),
            }
        )
    return rows


# --------------------------------------------------------------------------- orders
@cache
def _orders_created_on(seed: int, idx: int) -> tuple[dict, ...]:
    """All orders *created* on day idx, with their line items and payment (full truth)."""
    day = START_DATE + timedelta(days=idx)
    prices, _ = _product_prices(seed, idx)
    n_orders = 40 + random.Random(f"{seed}-nord-{idx}").randint(0, 20)
    rng = random.Random(f"{seed}-ord-{idx}")
    orders = []
    for i in range(n_orders):
        order_id = f"O{day:%Y%m%d}{i:04d}"
        customer_n = rng.randint(1, customers_count(idx))
        order_ts = _midnight(day) + timedelta(seconds=rng.randint(0, 86399))
        cancelled = rng.random() < 0.05
        product_ns = rng.sample(range(1, N_PRODUCTS + 1), rng.randint(1, 4))
        items = [
            {
                "order_id": order_id,
                "line_no": line_no,
                "product_id": f"P{p:04d}",
                "quantity": rng.randint(1, 5),
                "unit_price": prices[p],
            }
            for line_no, p in enumerate(product_ns, start=1)
        ]
        method = rng.choice(PAYMENT_METHODS)
        pay_failed = rng.random() < 0.03
        paid_at = order_ts + timedelta(seconds=rng.randint(1, 300))
        orders.append(
            {
                "order_id": order_id,
                "customer_id": f"C{customer_n:05d}",
                "order_ts": order_ts,
                "cancelled": cancelled,
                "items": items,
                "method": method,
                "pay_status": "failed" if pay_failed else "captured",
                "paid_at": paid_at,
            }
        )
    return tuple(orders)

def _ships(seed: int, order_id: str) -> bool:
    return random.Random(f"{seed}-ship-{order_id}").random() < 0.85


def _delivered(seed: int, order_id: str) -> bool:
    return _ships(seed, order_id) and random.Random(f"{seed}-del-{order_id}").random() < 0.9


def _status_update_ts(seed: int, order_id: str, day: date) -> datetime:
    return _midnight(day) + timedelta(seconds=random.Random(f"{seed}-uts-{order_id}-{day}").randint(0, 86399))


def _fmt(ts: datetime) -> str:
    return ts.isoformat(sep=" ")

def orders_for_day(seed: int, day: date, dirty_rate: float) -> list[dict]:
    idx = day_index(day)
    dirty = random.Random(f"{seed}-dirty-ord-{idx}")
    rows: list[dict] = []
    for o in _orders_created_on(seed, idx):
        status = "cancelled" if o["cancelled"] else "placed"
        row = {
            "order_id": o["order_id"],
            "customer_id": o["customer_id"],
            "order_ts": _fmt(o["order_ts"]),
            "status": status,
            "updated_at": _fmt(o["order_ts"]),
        }
        rows.append(row)
        if dirty.random() < dirty_rate:  # the source re-sent the same row
            rows.append(dict(row))
    for back, wanted in ((1, "shipped"), (2, "delivered")):
        if idx - back < 0:
            continue
        for o in _orders_created_on(seed, idx - back):
            if o["cancelled"]:
                continue
            hit = _ships(seed, o["order_id"]) if wanted == "shipped" else _delivered(seed, o["order_id"])
            if hit:
                rows.append(
                    {
                        "order_id": o["order_id"],
                        "customer_id": o["customer_id"],
                        "order_ts": _fmt(o["order_ts"]),
                        "status": wanted,
                        "updated_at": _fmt(_status_update_ts(seed, o["order_id"], day)),
                    }
                )
    return rows

def _item_is_dirty(dirty: random.Random, rate: float, line_no: int) -> bool:
    # Always draw, so the random stream does not depend on `rate`. Line 1 is never dirty,
    # which guarantees every order keeps at least one valid line.
    roll = dirty.random()
    return line_no > 1 and roll < rate

def order_items_for_day(seed: int, day: date, dirty_rate: float) -> list[dict]:
    idx = day_index(day)
    dirty = random.Random(f"{seed}-dirty-item-{idx}")
    rows = []
    for o in _orders_created_on(seed, idx):
        for item in o["items"]:
            qty = item["quantity"]
            if _item_is_dirty(dirty, dirty_rate, item["line_no"]):
                qty = dirty.choice([0, -1])
            rows.append({**item, "quantity": qty, "unit_price": str(item["unit_price"])})
    return rows


def payments_for_day(seed: int, day: date, dirty_rate: float) -> list[dict]:
    idx = day_index(day)
    dirty = random.Random(f"{seed}-dirty-item-{idx}")  # replay the same draws as order_items
    rows = []
    for o in _orders_created_on(seed, idx):
        total = Decimal("0.00")
        for item in o["items"]:
            if _item_is_dirty(dirty, dirty_rate, item["line_no"]):
                dirty.choice([0, -1])
            else:
                total += _money(item["unit_price"] * item["quantity"])
        if o["cancelled"]:
            continue
        rows.append(
            {
                "payment_id": f"PAY{o['order_id'][1:]}",
                "order_id": o["order_id"],
                "method": o["method"],
                "amount": str(total),
                "status": o["pay_status"],
                "paid_at": _fmt(o["paid_at"]),
            }
        )
    return rows


# --------------------------------------------------------------------------- writing
BUILDERS = {
    "customers": lambda seed, day, rate: customers_for_day(seed, day),
    "products": lambda seed, day, rate: products_for_day(seed, day),
    "orders": orders_for_day,
    "order_items": order_items_for_day,
    "payments": payments_for_day,
}

def partition_dir(day: date, landing_dir: Path = LANDING_DIR) -> Path:
    return Path(landing_dir) / f"dt={day.isoformat()}"


def write_day(
    day: date, landing_dir: Path = LANDING_DIR, seed: int = DEFAULT_SEED, dirty_rate: float = 0.02
) -> dict[str, int]:
    """Write one day's files. Each file is written to a temp name and renamed (atomic), and a
    `_SUCCESS` marker is written last. Downstream consumers wait for the marker, never for
    the data files, so they can never read a half-written file."""
    out = partition_dir(day, landing_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "_SUCCESS").unlink(missing_ok=True)
    counts = {}
    for table, columns in TABLE_COLUMNS.items():
        rows = BUILDERS[table](seed, day, dirty_rate)
        tmp = out / f".{table}.csv.tmp"
        with tmp.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp, out / f"{table}.csv")
        counts[table] = len(rows)
    (out / "_SUCCESS").write_text(datetime.now().isoformat())
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", default=SOURCE_START_DATE, help="first day (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--out", default=str(LANDING_DIR))
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--dirty-rate", type=float, default=0.02)
    args = parser.parse_args()
    first = date.fromisoformat(args.date)
    for offset in range(args.days):
        day = first + timedelta(days=offset)
        counts = write_day(day, Path(args.out), args.seed, args.dirty_rate)
        print(day, counts)


if __name__ == "__main__":
    main()
