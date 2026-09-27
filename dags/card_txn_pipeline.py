"""Daily card-transactions lakehouse pipeline (Airflow 3).

    generate_source -> bronze (txns || customers) -> silver (txns || dim_customer SCD2)
                    -> data_quality_gate -> gold -> [ai_retrieval_eval, publish_summary]

Design choices worth knowing:
  * Each step is a separate BashOperator calling the CLI, so Spark runs in its own process
    (in production this would be spark-submit / a Glue or EMR job; the DAG would barely change).
  * {{ ds }} is passed to every step: runs are keyed by business date, so any day can be
    re-run or backfilled and it only overwrites that day's partitions (idempotent).
  * max_active_runs=1 + depends_on_past on the SCD2 task: the customer dimension must be built
    day by day, in order.
  * The DQ gate fails the run before gold is touched, so bad data never reaches reporting.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, task_group

PROJECT_DIR = os.environ.get("LAKEHOUSE_HOME", str(Path(__file__).resolve().parents[1]))
DATA_DIR = os.environ.get("LAKEHOUSE_DATA", f"{PROJECT_DIR}/data")
CLI = f"cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR}/src python -m lakehouse.cli"

log = logging.getLogger(__name__)


def _on_failure(context) -> None:
    # Hook for Slack/email/PagerDuty. Kept as a log line so the project runs without secrets.
    ti = context["task_instance"]
    log.error("ALERT: %s.%s failed for ds=%s (try %s)", ti.dag_id, ti.task_id, context["ds"], ti.try_number)


default_args = {
    "owner": "sahil",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_failure,
}


def step(task_id: str, cmd: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"{CLI} {cmd} --ds {{{{ ds }}}}",
                        env={"LAKEHOUSE_DATA": DATA_DIR}, append_env=True, **kwargs)


@dag(
    dag_id="card_txn_lakehouse",
    description="Card transactions: bronze -> silver (SCD2) -> DQ gate -> gold (fraud rules)",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,                # backfill on demand instead (see README)
    max_active_runs=1,            # days must be processed in order (SCD2)
    default_args=default_args,
    tags=["lakehouse", "pyspark", "bfsi"],
    doc_md=__doc__,
)
def card_txn_lakehouse():
    # In production this would be a sensor waiting for the upstream drop (S3KeySensor / FileSensor).
    generate_source = step("generate_source", "generate", retries=0)

    @task_group(group_id="bronze")
    def bronze():
        step("ingest_bronze", "bronze")

    @task_group(group_id="silver")
    def silver():
        step("silver_transactions", "silver_txns")
        step("dim_customer_scd2", "dim_customer", depends_on_past=True)

    dq_gate = step("data_quality_gate", "dq", retries=0)  # a DQ failure is data, not flakiness: don't retry
    # AI layer: re-check that catalog retrieval still finds the right tables for the golden questions
    ai_eval = BashOperator(
        task_id="ai_retrieval_eval",
        bash_command=f"cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR}/src python -m lakehouse.ai.cli eval-retrieval --min-recall 0.9",
        env={"LAKEHOUSE_DATA": DATA_DIR}, append_env=True, retries=0)
    gold = step("build_gold", "gold")

    @task
    def publish_summary(ds: str | None = None) -> dict:
        """Reads the DQ report and returns key metrics (visible in the UI as an XCom)."""
        report = json.loads(Path(DATA_DIR, "_dq", f"ds={ds}", "report.json").read_text())
        summary = {"ds": ds, **report["metrics"]}
        log.info("Run summary: %s", summary)
        return summary

    generate_source >> bronze() >> silver() >> dq_gate >> gold >> [ai_eval, publish_summary()]


card_txn_lakehouse()
