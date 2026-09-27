"""Gold: business-ready tables built from silver.

  daily_category_summary - spend by category x channel per day (dashboard feed)
  customer_daily_spend   - per-customer spend, joined point-in-time to the SCD2 customer dimension
                           (the segment the customer had ON that day, not today's)
  fraud_alerts           - rule-based alerts using window functions:
                             velocity : >= N txns on one card inside a 10-minute window
                             spike    : txn > 10x the customer's trailing 30-day average
                             foreign  : non-INR txn above an INR threshold
"""
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import config


def daily_category_summary(txns: DataFrame) -> DataFrame:
    return (txns.groupBy("ds", "category", "channel")
            .agg(F.count("*").alias("txn_count"),
                 F.sum("amount_inr").alias("total_inr"),
                 F.round(F.avg("amount_inr"), 2).alias("avg_inr"),
                 F.countDistinct("customer_id").alias("unique_customers")))


def customer_daily_spend(txns: DataFrame, dim_customer: DataFrame) -> DataFrame:
    spend = (txns.groupBy("ds", "customer_id")
             .agg(F.count("*").alias("txn_count"), F.sum("amount_inr").alias("spend_inr")))
    # Point-in-time (as-of) join against SCD2: pick the version valid on the business date.
    dim = dim_customer.select("customer_id", "segment", "city", "risk_tier", "valid_from", "valid_to")
    ds_date = F.to_date(F.col("ds"))
    return (spend.join(dim, "customer_id", "left")
            .where((ds_date >= F.col("valid_from")) & (F.col("valid_to").isNull() | (ds_date <= F.col("valid_to"))))
            .drop("valid_from", "valid_to"))


def fraud_alerts(today: DataFrame, history: DataFrame) -> DataFrame:
    ts = F.col("event_time").cast("long")

    # Rule 1: velocity - count txns on the same card in the trailing 10-minute window (range frame on epoch secs)
    w_vel = Window.partitionBy("card_id").orderBy(ts).rangeBetween(-config.VELOCITY_WINDOW_SEC, 0)
    velocity = (today.withColumn("txns_10min", F.count("*").over(w_vel))
                .where(F.col("txns_10min") >= config.VELOCITY_MAX_TXNS)
                .withColumn("rule", F.lit("velocity"))
                .withColumn("detail", F.concat(F.col("txns_10min"), F.lit(" txns in 10 min"))))

    # Rule 2: spike vs the customer's trailing average (history = previous N days of silver)
    baseline = (history.groupBy("customer_id")
                .agg(F.avg("amount_inr").alias("avg_30d"), F.count("*").alias("n_30d"))
                .where("n_30d >= 5"))  # need some history before judging a spike
    spike = (today.join(baseline, "customer_id")
             .where((F.col("amount_inr") > F.col("avg_30d") * config.SPIKE_MULTIPLIER)
                    & (F.col("amount_inr") >= config.SPIKE_MIN_AMOUNT_INR))  # ignore small-ticket "spikes"
             .withColumn("rule", F.lit("spike"))
             .withColumn("detail", F.concat(F.lit("amount "), F.col("amount_inr"),
                                            F.lit(" vs 30d avg "), F.round("avg_30d", 2))))

    # Rule 3: large foreign-currency transaction
    foreign = (today.where((F.col("currency") != "INR") & (F.col("amount_inr") > config.FOREIGN_HIGH_AMOUNT_INR))
               .withColumn("rule", F.lit("foreign_high_value"))
               .withColumn("detail", F.concat(F.col("currency"), F.lit(" "), F.col("amount"))))

    cols = ["ds", "txn_id", "card_id", "customer_id", "amount_inr", "event_time", "rule", "detail"]
    return velocity.select(cols).unionByName(spike.select(cols)).unionByName(foreign.select(cols))


def build_gold(spark: SparkSession, ds: str) -> dict:
    silver = spark.read.parquet(str(config.SILVER / "transactions"))
    today = silver.where(F.col("ds") == ds).cache()  # reused by 3 outputs -> cache once
    d = date.fromisoformat(ds)
    lookback = [str(d - timedelta(days=i)) for i in range(1, config.SPIKE_LOOKBACK_DAYS + 1)]
    history = silver.where(F.col("ds").isin(lookback))  # partition pruning: only reads those folders
    dim = spark.read.parquet(str(config.SILVER / "dim_customer"))

    outputs = {
        "daily_category_summary": daily_category_summary(today),
        "customer_daily_spend": customer_daily_spend(today, dim),
        "fraud_alerts": fraud_alerts(today, history),
    }
    counts = {}
    for name, df in outputs.items():
        df.write.mode("overwrite").partitionBy("ds").parquet(str(config.GOLD / name))
        counts[name] = spark.read.parquet(str(config.GOLD / name)).where(F.col("ds") == ds).count()
    alerts = spark.read.parquet(str(config.GOLD / "fraud_alerts")).where(F.col("ds") == ds)
    counts["alerts_by_rule"] = {r["rule"]: r["n"] for r in alerts.groupBy("rule").agg(F.count("*").alias("n")).collect()}
    today.unpersist()
    return counts
