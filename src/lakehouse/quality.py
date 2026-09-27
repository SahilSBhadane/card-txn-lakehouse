"""Data-quality gate between silver and gold.

If any check fails the task raises, Airflow marks it failed, and gold is NOT built for that day,
so bad data never reaches reporting. A JSON report is written for every run (pass or fail).
"""
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import config


class DataQualityError(Exception):
    pass


def run_checks(spark: SparkSession, ds: str) -> dict:
    bronze = spark.read.parquet(str(config.BRONZE / "transactions")).where(F.col("ds") == ds)
    silver = spark.read.parquet(str(config.SILVER / "transactions")).where(F.col("ds") == ds)
    quarantine = spark.read.parquet(str(config.QUARANTINE / "transactions")).where(F.col("ds") == ds)

    n_bronze, n_silver, n_quar = bronze.count(), silver.count(), quarantine.count()
    n_unique_bronze = bronze.select("txn_id").distinct().count()
    dup_ids_silver = silver.groupBy("txn_id").count().where("count > 1").count()
    null_keys = silver.where(F.col("txn_id").isNull() | F.col("customer_id").isNull()
                             | F.col("amount_inr").isNull() | F.col("event_time").isNull()).count()
    orphan_customers = (silver.join(spark.read.parquet(str(config.SILVER / "dim_customer")).where("is_current"),
                                    "customer_id", "left_anti").count())
    quarantine_rate = n_quar / n_bronze if n_bronze else 1.0

    checks = {
        # every unique bronze record must end up in exactly one of silver or quarantine
        "reconciliation": n_unique_bronze == n_silver + n_quar,
        "no_duplicate_txn_ids": dup_ids_silver == 0,
        "no_null_keys": null_keys == 0,
        "quarantine_rate_ok": quarantine_rate <= config.MAX_QUARANTINE_RATE,
        "non_empty": n_silver > 0,
        "no_orphan_customers": orphan_customers == 0,
    }
    report = {
        "ds": ds, "checked_at": datetime.now(timezone.utc).isoformat(),
        "passed": all(checks.values()), "checks": checks,
        "metrics": {"bronze_rows": n_bronze, "bronze_unique_txn_ids": n_unique_bronze, "silver_rows": n_silver,
                    "quarantined_rows": n_quar, "quarantine_rate": round(quarantine_rate, 4),
                    "duplicate_txn_ids_in_silver": dup_ids_silver, "null_key_rows": null_keys,
                    "orphan_customer_rows": orphan_customers},
    }
    out = config.DQ_REPORTS / f"ds={ds}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, indent=2))
    if not report["passed"]:
        failed = [k for k, v in checks.items() if not v]
        raise DataQualityError(f"DQ failed for {ds}: {failed} | metrics={report['metrics']}")
    return report
