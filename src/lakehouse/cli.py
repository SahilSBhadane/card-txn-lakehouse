"""Single entry point, one command per pipeline step. Airflow calls these; you can too.

    python -m lakehouse.cli generate        --ds 2026-09-20
    python -m lakehouse.cli bronze          --ds 2026-09-20
    python -m lakehouse.cli silver_txns     --ds 2026-09-20
    python -m lakehouse.cli dim_customer    --ds 2026-09-20
    python -m lakehouse.cli dq              --ds 2026-09-20
    python -m lakehouse.cli gold            --ds 2026-09-20
    python -m lakehouse.cli all             --ds 2026-09-20   # every step, in order
"""
import argparse
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lakehouse")


def run(step: str, ds: str) -> dict:
    if step == "generate":
        from lakehouse.generate import generate
        return generate(ds)

    from lakehouse.spark import get_spark
    spark = get_spark(f"lakehouse-{step}-{ds}")
    spark.sparkContext.setLogLevel("WARN")
    if step == "bronze":
        from lakehouse import bronze
        return {"transactions": bronze.ingest_transactions(spark, ds),
                "customers": bronze.ingest_customers(spark, ds)}
    if step == "silver_txns":
        from lakehouse.silver import build_silver_transactions
        return build_silver_transactions(spark, ds)
    if step == "dim_customer":
        from lakehouse.silver import build_dim_customer
        return build_dim_customer(spark, ds)
    if step == "dq":
        from lakehouse.quality import run_checks
        return run_checks(spark, ds)
    if step == "gold":
        from lakehouse.gold import build_gold
        return build_gold(spark, ds)
    raise ValueError(f"unknown step {step}")


STEPS = ["generate", "bronze", "silver_txns", "dim_customer", "dq", "gold"]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=STEPS + ["all"])
    p.add_argument("--ds", required=True, help="business date YYYY-MM-DD")
    a = p.parse_args()
    for step in (STEPS if a.step == "all" else [a.step]):
        t0 = time.time()
        result = run(step, a.ds)
        log.info("step=%s ds=%s secs=%.1f result=%s", step, a.ds, time.time() - t0, json.dumps(result, default=str))


if __name__ == "__main__":
    main()
