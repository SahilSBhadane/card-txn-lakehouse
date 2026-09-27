"""Unit tests for the pure transform functions (no files touched)."""
from datetime import date, datetime

from pyspark.sql import functions as F

from lakehouse.gold import customer_daily_spend, fraud_alerts
from lakehouse.silver import clean_transactions, fx_table, scd2_merge

BRONZE_COLS = ["txn_id", "card_id", "customer_id", "merchant_id", "category", "channel", "amount",
               "currency", "event_time", "_source_file", "_ingested_at", "ds"]


def bronze_row(txn_id, customer_id="C1", amount="100.00", currency="INR", event_time="2026-09-21T10:00:00Z",
               ingested="2026-09-21 12:00:00"):
    return (txn_id, "K1", customer_id, "M1", "Grocery ", "pos", amount, currency, event_time, "f1",
            datetime.fromisoformat(ingested), "2026-09-21")


def test_clean_transactions_dedupes_quarantines_and_converts(spark):
    rows = [
        bronze_row("T1"),
        bronze_row("T1", ingested="2026-09-21 12:05:00"),     # resent duplicate -> keep one
        bronze_row("T2", customer_id=None),                    # missing customer
        bronze_row("T3", amount="-5"),                         # negative amount
        bronze_row("T4", currency="XYZ"),                      # unknown currency
        bronze_row("T5", event_time="garbage"),                # unparseable timestamp
        bronze_row("T6", amount="10", currency=" usd "),       # messy but valid -> USD, converted
        bronze_row("T7", event_time="2026-09-20T23:59:00Z"),   # late-arriving event
    ]
    bronze = spark.createDataFrame(rows, BRONZE_COLS)
    valid, quarantined, dups = clean_transactions(bronze, fx_table(spark))

    assert dups == 1
    assert sorted(r.txn_id for r in valid.collect()) == ["T1", "T6", "T7"]
    reasons = {r.txn_id: r.reject_reason for r in quarantined.collect()}
    assert reasons == {"T2": "missing_customer_id", "T3": "non_positive_amount",
                       "T4": "unknown_currency", "T5": "unparseable_event_time"}
    t6 = valid.where("txn_id = 'T6'").first()
    assert t6.currency == "USD" and float(t6.amount_inr) == 830.0
    assert valid.where("txn_id = 'T7'").first().is_late is True
    assert valid.where("txn_id = 'T1'").first().category == "grocery"


def _snapshot(spark, city_c1="Pune", extra=None):
    rows = [("C1", "A", "K1", city_c1, "mass", "low"), ("C2", "B", "K2", "Mumbai", "mass", "low")]
    rows += extra or []
    return spark.createDataFrame(rows, ["customer_id", "full_name", "card_id", "city", "segment", "risk_tier"])


def test_scd2_opens_closes_and_is_idempotent(spark):
    day1 = scd2_merge(None, _snapshot(spark), "2026-09-20")
    day2 = scd2_merge(day1, _snapshot(spark, city_c1="Chennai", extra=[("C3", "C", "K3", "Delhi", "mass", "low")]),
                      "2026-09-21")

    c1 = {(r.city, r.is_current, r.valid_to) for r in day2.where("customer_id = 'C1'").collect()}
    assert c1 == {("Pune", False, date(2026, 9, 20)), ("Chennai", True, None)}
    assert day2.where("customer_id = 'C2'").count() == 1                  # unchanged -> no new version
    assert day2.where("customer_id = 'C3' AND is_current").count() == 1   # new customer
    assert day2.where("is_current").count() == 3

    # Re-running the same day must give the same dimension (no duplicate versions).
    rerun = scd2_merge(day2, _snapshot(spark, city_c1="Chennai", extra=[("C3", "C", "K3", "Delhi", "mass", "low")]),
                       "2026-09-21")
    assert sorted(rerun.drop("attr_hash").collect()) == sorted(day2.drop("attr_hash").collect())


def test_point_in_time_join_uses_segment_valid_on_that_day(spark):
    dim = spark.createDataFrame(
        [("C1", "mass", "Pune", "low", date(2026, 9, 1), date(2026, 9, 20)),
         ("C1", "premium", "Pune", "low", date(2026, 9, 21), None)],
        "customer_id string, segment string, city string, risk_tier string, valid_from date, valid_to date")
    txns = spark.createDataFrame([("2026-09-20", "C1", 100.0), ("2026-09-21", "C1", 200.0)],
                                 ["ds", "customer_id", "amount_inr"])
    out = {r.ds: r.segment for r in customer_daily_spend(txns, dim).collect()}
    assert out == {"2026-09-20": "mass", "2026-09-21": "premium"}


def test_velocity_rule_flags_burst(spark):
    base = datetime(2026, 9, 21, 10, 0, 0)
    rows = [("2026-09-21", f"T{i}", "K9", "C9", 500.0, "INR", 500.0, base.replace(minute=i)) for i in range(5)]
    rows.append(("2026-09-21", "T99", "K8", "C8", 500.0, "INR", 500.0, base))  # lone txn on another card
    today = spark.createDataFrame(rows, "ds string, txn_id string, card_id string, customer_id string, "
                                        "amount double, currency string, amount_inr double, event_time timestamp")
    history = today.limit(0)
    alerts = fraud_alerts(today, history)
    # 5th txn inside 10 minutes is the first to hit the threshold (>= 5)
    assert {r.txn_id for r in alerts.where(F.col("rule") == "velocity").collect()} == {"T4"}
    assert alerts.where("card_id = 'K8'").count() == 0
