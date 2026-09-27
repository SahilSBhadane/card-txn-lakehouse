"""Silver: typed, validated, deduplicated, conformed data.

Transactions
  * cast types, standardise text (trim/upper currency, lower category)
  * validate each row; invalid rows go to quarantine/ with a reject_reason instead of failing the job
  * dedupe on txn_id (upstream resends), keeping the latest ingested copy
  * convert amounts to INR with a broadcast join to a small FX table

Customers -> dim_customer as a Slowly Changing Dimension Type 2 (SCD2)
  * one row per customer per version: valid_from, valid_to, is_current
  * idempotent: re-running a ds first rolls the dimension back to its state before that ds
"""
import shutil

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import config

SCD2_TRACKED = ["city", "segment", "risk_tier"]


# ---------------------------------------------------------------- transactions
def clean_transactions(bronze: DataFrame, fx: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Pure transform (no I/O) so it can be unit-tested. Returns (valid, quarantined, duplicates_dropped)."""
    # Spark 4 runs in ANSI mode: a plain cast/to_timestamp THROWS on bad input and kills the job.
    # try_* variants return NULL instead, so bad rows can be quarantined by the rules below.
    typed = (bronze
             .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(14,2))"))
             .withColumn("currency", F.upper(F.trim("currency")))
             .withColumn("category", F.lower(F.trim("category")))
             .withColumn("event_time", F.try_to_timestamp("event_time", F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")))
             .withColumn("event_date", F.to_date("event_time")))

    # Dedupe first: a resent event is a duplicate, not a data-quality failure.
    w = Window.partitionBy("txn_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    ranked = typed.withColumn("_rn", F.row_number().over(w))
    duplicates_dropped = ranked.where("_rn > 1").count()
    deduped = ranked.where("_rn = 1").drop("_rn")

    # Validation rules -> first failing rule becomes the reject reason.
    reason = (F.when(F.col("txn_id").isNull(), "missing_txn_id")
               .when(F.col("customer_id").isNull(), "missing_customer_id")
               .when(F.col("amount").isNull(), "unparseable_amount")
               .when(F.col("amount") <= 0, "non_positive_amount")
               .when(F.col("event_time").isNull(), "unparseable_event_time")
               .when(~F.col("currency").isin(list(config.FX_TO_INR)), "unknown_currency"))
    checked = deduped.withColumn("reject_reason", reason)

    quarantined = checked.where(F.col("reject_reason").isNotNull())
    valid = (checked.where(F.col("reject_reason").isNull()).drop("reject_reason")
             # FX table is tiny -> broadcast join avoids shuffling the big side
             .join(F.broadcast(fx), on="currency", how="left")
             .withColumn("amount_inr", F.round(F.col("amount") * F.col("fx_to_inr"), 2).cast("decimal(16,2)"))
             .withColumn("is_late", F.col("event_date") < F.to_date(F.col("ds")))
             .select("txn_id", "card_id", "customer_id", "merchant_id", "category", "channel",
                     "amount", "currency", "amount_inr", "event_time", "event_date", "is_late",
                     "_ingested_at", "ds"))
    return valid, quarantined, duplicates_dropped


def fx_table(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([(k, float(v)) for k, v in config.FX_TO_INR.items()], ["currency", "fx_to_inr"])


def build_silver_transactions(spark: SparkSession, ds: str) -> dict:
    bronze = spark.read.parquet(str(config.BRONZE / "transactions")).where(F.col("ds") == ds)
    bronze_rows = bronze.count()
    valid, quarantined, dups = clean_transactions(bronze, fx_table(spark))
    valid.write.mode("overwrite").partitionBy("ds").parquet(str(config.SILVER / "transactions"))
    quarantined.write.mode("overwrite").partitionBy("ds").parquet(str(config.QUARANTINE / "transactions"))
    silver_rows = spark.read.parquet(str(config.SILVER / "transactions")).where(F.col("ds") == ds).count()
    q = spark.read.parquet(str(config.QUARANTINE / "transactions")).where(F.col("ds") == ds)
    reasons = {r["reject_reason"]: r["n"] for r in q.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()}
    return {"bronze_rows": bronze_rows, "silver_rows": silver_rows, "quarantined_rows": sum(reasons.values()),
            "duplicates_dropped": dups, "reject_reasons": reasons}


# ---------------------------------------------------------------- dim_customer (SCD2)
def scd2_merge(current_dim: DataFrame | None, snapshot: DataFrame, ds: str) -> DataFrame:
    """Pure SCD2 logic. `snapshot` = today's full customer list; returns the full new dimension."""
    snap = (snapshot.select("customer_id", "full_name", "card_id", *SCD2_TRACKED)
            .withColumn("attr_hash", F.sha2(F.concat_ws("||", *SCD2_TRACKED), 256)))
    ds_col, prev_day = F.to_date(F.lit(ds)), F.date_sub(F.to_date(F.lit(ds)), 1)

    if current_dim is None:  # first load: everyone opens a version today
        return (snap.withColumn("valid_from", ds_col)
                    .withColumn("valid_to", F.lit(None).cast("date"))
                    .withColumn("is_current", F.lit(True)))

    # Idempotency: undo anything a previous run of this same ds (or a later one) did.
    dim = (current_dim.where(F.col("valid_from") < ds_col)
           .withColumn("valid_to", F.when(F.col("valid_to") >= prev_day, F.lit(None).cast("date"))
                                    .otherwise(F.col("valid_to")))
           .withColumn("is_current", F.col("valid_to").isNull()))

    history = dim.where(~F.col("is_current"))
    current = dim.where(F.col("is_current"))

    joined = current.alias("c").join(snap.alias("s"), "customer_id", "full_outer")
    changed_or_same = joined.where(F.col("c.attr_hash").isNotNull() & F.col("s.attr_hash").isNotNull())
    unchanged = (changed_or_same.where(F.col("c.attr_hash") == F.col("s.attr_hash"))
                 .select("customer_id", *[F.col(f"c.{c}").alias(c) for c in
                                          ["full_name", "card_id", *SCD2_TRACKED, "attr_hash", "valid_from",
                                           "valid_to", "is_current"]]))
    changed = changed_or_same.where(F.col("c.attr_hash") != F.col("s.attr_hash"))
    closed = changed.select("customer_id", *[F.col(f"c.{c}").alias(c) for c in
                                             ["full_name", "card_id", *SCD2_TRACKED, "attr_hash", "valid_from"]]) \
                    .withColumn("valid_to", prev_day).withColumn("is_current", F.lit(False))
    opened = (changed.select("customer_id", *[F.col(f"s.{c}").alias(c) for c in
                                              ["full_name", "card_id", *SCD2_TRACKED, "attr_hash"]])
              .unionByName(joined.where(F.col("c.attr_hash").isNull())      # brand-new customers
                           .select("customer_id", *[F.col(f"s.{c}").alias(c) for c in
                                                    ["full_name", "card_id", *SCD2_TRACKED, "attr_hash"]]))
              .withColumn("valid_from", ds_col).withColumn("valid_to", F.lit(None).cast("date"))
              .withColumn("is_current", F.lit(True)))
    # Customers missing from the snapshot keep their current version (we don't delete on absence).
    missing = (joined.where(F.col("s.attr_hash").isNull())
               .select("customer_id", *[F.col(f"c.{c}").alias(c) for c in
                                        ["full_name", "card_id", *SCD2_TRACKED, "attr_hash", "valid_from",
                                         "valid_to", "is_current"]]))
    return history.unionByName(unchanged).unionByName(closed).unionByName(opened).unionByName(missing)


def build_dim_customer(spark: SparkSession, ds: str) -> dict:
    path = config.SILVER / "dim_customer"
    tmp = config.SILVER / "_dim_customer_tmp"
    snapshot = spark.read.parquet(str(config.BRONZE / "customers")).where(F.col("ds") == ds)
    current = spark.read.parquet(str(path)) if path.exists() else None
    new_dim = scd2_merge(current, snapshot, ds)
    # Spark can't overwrite a path it is reading from: write to tmp, then swap directories.
    new_dim.write.mode("overwrite").parquet(str(tmp))
    shutil.rmtree(path, ignore_errors=True)
    shutil.move(str(tmp), str(path))
    dim = spark.read.parquet(str(path))
    return {
        "dim_rows": dim.count(),
        "current_customers": dim.where("is_current").count(),
        "versions_opened_today": dim.where(F.col("valid_from") == F.to_date(F.lit(ds))).count(),
        "versions_closed_today": dim.where(F.col("valid_to") == F.date_sub(F.to_date(F.lit(ds)), 1)).count(),
    }
