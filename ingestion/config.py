"""Central configuration. Every path can be overridden by an environment variable,
which is how Docker, CI and your laptop all run the same code with different settings."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Where the "source system" drops daily files (the landing zone).
LANDING_DIR = Path(os.getenv("ECOM_DATA_DIR", REPO_ROOT / "data" / "landing"))

# The DuckDB file that plays the role of the warehouse.
DUCKDB_PATH = Path(os.getenv("ECOM_DUCKDB_PATH", REPO_ROOT / "data" / "warehouse" / "ecom.duckdb"))

DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", REPO_ROOT / "dbt_project"))

# Binary used to call dbt. In Docker, dbt lives in its own virtualenv (see the Dockerfile).
DBT_BIN = os.getenv("DBT_BIN", "dbt")

# The tables the source system produces. Order matters only for readability.
TABLES = ["customers", "products", "orders", "order_items", "payments"]

# The first day the source system existed. The pipeline always backfills from here.
SOURCE_START_DATE = "2024-01-01"