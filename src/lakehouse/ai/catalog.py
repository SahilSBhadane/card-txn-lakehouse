"""Data catalog for retrieval: one document per table + one per business metric.

Descriptions are written by hand (like a real data catalog / dbt docs); column types,
row counts and date ranges are read live from the warehouse so the docs never go stale.
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb

TABLE_DOCS = {
    "daily_category_summary": {
        "grain": "one row per business date (ds) x merchant category x channel",
        "about": "Daily card spend summary by merchant category (grocery, fuel, travel, electronics, dining, "
                 "utilities, fashion, pharmacy) and channel (pos, ecom, contactless). Use for spend, volume, "
                 "average ticket size and category/channel trends.",
        "columns": {"ds": "business date (string YYYY-MM-DD)", "category": "merchant category",
                    "channel": "pos | ecom | contactless", "txn_count": "number of transactions",
                    "total_inr": "total spend in INR", "avg_inr": "average transaction amount in INR",
                    "unique_customers": "distinct customers who transacted"},
    },
    "customer_daily_spend": {
        "grain": "one row per business date (ds) x customer",
        "about": "Per-customer daily spend with the customer's segment, city and risk tier AS OF that day "
                 "(point-in-time join to the SCD2 customer dimension). Use for top spenders, spend by "
                 "segment/city/risk tier, customer counts.",
        "columns": {"ds": "business date (string YYYY-MM-DD)", "customer_id": "customer id like C0001234",
                    "txn_count": "transactions that day", "spend_inr": "total spend that day in INR",
                    "segment": "mass | affluent | premium", "city": "customer's city that day",
                    "risk_tier": "low | medium | high"},
    },
    "fraud_alerts": {
        "grain": "one row per transaction x fraud rule that fired",
        "about": "Rule-based fraud alerts. rule = 'velocity' (5+ transactions on one card within 10 minutes), "
                 "'spike' (amount over 10x the customer's 30-day average and at least 75,000 INR), or "
                 "'foreign_high_value' (non-INR transaction above 1,00,000 INR). Use for suspicious activity, "
                 "alert counts, flagged cards/customers.",
        "columns": {"ds": "business date (string YYYY-MM-DD)", "txn_id": "transaction id",
                    "card_id": "card id", "customer_id": "customer id", "amount_inr": "transaction amount in INR",
                    "event_time": "transaction timestamp (UTC)", "rule": "velocity | spike | foreign_high_value",
                    "detail": "human-readable reason"},
    },
    "dim_customer": {
        "grain": "one row per customer per version (SCD Type 2)",
        "about": "Customer master data with full history. is_current = true marks the latest version; "
                 "valid_from / valid_to give the date range each version was true. Use for customer "
                 "attributes, how many customers moved city or changed segment, current customer counts.",
        "columns": {"customer_id": "customer id", "full_name": "customer name", "card_id": "card id",
                    "city": "city", "segment": "mass | affluent | premium", "risk_tier": "low | medium | high",
                    "valid_from": "date this version started", "valid_to": "date it ended (NULL if current)",
                    "is_current": "true for the latest version", "attr_hash": "internal change-detection hash"},
    },
}

# metric -> (definition, tables it needs). Linking metrics to tables lets retrieval pull in
# every table a metric depends on, even if the question only matches the metric's wording.
METRIC_DOCS = {
    "fraud alert rate": ("Fraud alert rate for a day = count(*) from fraud_alerts for that ds divided by "
                        "sum(txn_count) from daily_category_summary for the same ds.",
                         ["fraud_alerts", "daily_category_summary"]),
    "spend": ("Spend always means amount in INR: total_inr in daily_category_summary, spend_inr in "
             "customer_daily_spend. Foreign-currency transactions are already converted to INR.",
              ["daily_category_summary", "customer_daily_spend"]),
    "average ticket size": ("Average ticket size = sum(total_inr) / sum(txn_count) from daily_category_summary.",
                            ["daily_category_summary"]),
    "top spenders": ("Top spenders = customers ranked by sum(spend_inr) in customer_daily_spend over the period.",
                     ["customer_daily_spend"]),
    "customers who changed": ("Customers who moved city or changed segment = customers with more than one row "
                             "in dim_customer, or rows where is_current = false.",
                              ["dim_customer"]),
}


@dataclass
class Doc:
    id: str
    kind: str        # "table" | "metric"
    table: str | None
    text: str
    links: tuple[str, ...] = ()   # tables a metric doc depends on


def build_docs(con: duckdb.DuckDBPyConnection) -> list[Doc]:
    present = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    docs: list[Doc] = []
    for table, meta in TABLE_DOCS.items():
        if table not in present:
            continue
        types = {r[0]: r[1] for r in con.execute(f"DESCRIBE {table}").fetchall()}
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        date_range = ""
        if "ds" in types:
            lo, hi = con.execute(f"SELECT min(ds), max(ds) FROM {table}").fetchone()
            date_range = f" Dates available: {lo} to {hi}."
        cols = "\n".join(f"  - {c} ({types.get(c, '?')}): {d}" for c, d in meta["columns"].items() if c in types)
        text = (f"TABLE {table}\nGrain: {meta['grain']}.\n{meta['about']}\nRows: {n:,}.{date_range}\n"
                f"Columns:\n{cols}")
        docs.append(Doc(id=f"table:{table}", kind="table", table=table, text=text))
    for name, (text, tables) in METRIC_DOCS.items():
        docs.append(Doc(id=f"metric:{name}", kind="metric", table=None, text=f"METRIC {name}: {text}",
                        links=tuple(t for t in tables if t in present)))
    return docs
