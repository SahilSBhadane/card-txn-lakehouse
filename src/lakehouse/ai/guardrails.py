"""Checks every LLM-generated query before it runs (defence in depth on top of the DuckDB sandbox)."""
from __future__ import annotations

import re

import duckdb

MAX_ROWS = 200
FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|copy|attach|detach|install|load|"
    r"pragma|export|import|call|set|reset|checkpoint|vacuum)\b|read_\w+\s*\(|glob\s*\(",
    re.IGNORECASE)


class UnsafeSQL(ValueError):
    pass


def extract_sql(text: str) -> str:
    """Pull SQL out of an LLM reply (```sql fenced block, or the raw text)."""
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    sql = (m.group(1) if m else text).strip()
    return sql.rstrip(";").strip()


def validate(sql: str, allowed_tables: set[str]) -> str:
    """Return a safe, row-limited query or raise UnsafeSQL."""
    if not sql:
        raise UnsafeSQL("empty query")
    if ";" in sql:
        raise UnsafeSQL("only one statement is allowed")
    if not re.match(r"^\s*(select|with)\b", sql, re.IGNORECASE):
        raise UnsafeSQL("only SELECT / WITH queries are allowed")
    if FORBIDDEN.search(sql):
        raise UnsafeSQL(f"forbidden keyword or function: {FORBIDDEN.search(sql).group(0)!r}")
    try:
        tables = {t.lower() for t in duckdb.get_table_names(sql)}
    except Exception as e:
        raise UnsafeSQL(f"could not parse SQL: {e}") from e
    unknown = tables - allowed_tables
    if unknown:
        raise UnsafeSQL(f"unknown or disallowed tables: {sorted(unknown)}")
    return f"SELECT * FROM ({sql}) AS q LIMIT {MAX_ROWS}"
