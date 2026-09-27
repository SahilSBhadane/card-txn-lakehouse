"""Sandboxed DuckDB 'warehouse' over the lakehouse Parquet files.

Tables are loaded into memory once, then external file access is switched off and the
configuration is locked. So even if an LLM writes `read_csv('/etc/passwd')` or tries to
ATTACH another database, DuckDB refuses. Guardrail #1 is enforced by the engine itself.
"""
from __future__ import annotations

import duckdb

from lakehouse import config

# table name exposed to the LLM -> parquet location (hive-partitioned by ds where applicable)
TABLES = {
    "daily_category_summary": config.GOLD / "daily_category_summary",
    "customer_daily_spend": config.GOLD / "customer_daily_spend",
    "fraud_alerts": config.GOLD / "fraud_alerts",
    "dim_customer": config.SILVER / "dim_customer",
}


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    for name, path in TABLES.items():
        if not path.exists():
            continue
        glob = f"{path}/**/*.parquet"
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet('{glob}', hive_partitioning = true)")
    con.execute("SET enable_external_access = false")  # no file/network reads from generated SQL
    con.execute("SET lock_configuration = true")       # ...and the SQL can't switch it back on
    return con


def available_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()]
