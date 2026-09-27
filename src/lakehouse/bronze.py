"""Bronze: land raw files as-is into Parquet, add ingestion metadata. No business logic here.

Everything is read as STRING with an explicit schema, so a bad value upstream can never
break ingestion; typing and validation happen in silver. Each run overwrites only its own
ds partition (dynamic partition overwrite), so re-running a day is safe (idempotent).
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse import config

TXN_RAW_SCHEMA = StructType([StructField(c, StringType()) for c in [
    "txn_id", "card_id", "customer_id", "merchant_id", "category", "channel",
    "amount", "currency", "event_time",
]])
CUSTOMER_RAW_SCHEMA = StructType([StructField(c, StringType()) for c in [
    "customer_id", "full_name", "city", "segment", "risk_tier", "card_id",
]])


def _with_metadata(df: DataFrame, ds: str) -> DataFrame:
    return (df.withColumn("_source_file", F.input_file_name())
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("ds", F.lit(ds)))


def ingest_transactions(spark: SparkSession, ds: str) -> int:
    src = config.LANDING / "transactions" / f"ds={ds}"
    df = _with_metadata(spark.read.schema(TXN_RAW_SCHEMA).json(str(src)), ds)
    df.write.mode("overwrite").partitionBy("ds").parquet(str(config.BRONZE / "transactions"))
    return spark.read.parquet(str(config.BRONZE / "transactions")).where(F.col("ds") == ds).count()


def ingest_customers(spark: SparkSession, ds: str) -> int:
    src = config.LANDING / "customers" / f"ds={ds}"
    df = _with_metadata(spark.read.schema(CUSTOMER_RAW_SCHEMA).option("header", True).csv(str(src)), ds)
    df.write.mode("overwrite").partitionBy("ds").parquet(str(config.BRONZE / "customers"))
    return spark.read.parquet(str(config.BRONZE / "customers")).where(F.col("ds") == ds).count()
