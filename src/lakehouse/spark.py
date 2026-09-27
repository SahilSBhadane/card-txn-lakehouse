from pyspark.sql import SparkSession


def get_spark(app: str = "card-txn-lakehouse") -> SparkSession:
    """Local-mode Spark session. On a cluster (Glue/EMR/Databricks) only this function changes."""
    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")          # small data: avoid 200 tiny tasks
        .config("spark.sql.adaptive.enabled", "true")          # AQE: coalesce partitions, handle skew
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # rerun a day = replace only that day
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
