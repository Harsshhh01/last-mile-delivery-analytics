"""Spark settings shared by the local runner and the AWS Glue job."""

from __future__ import annotations

import warnings

from pyspark.sql import SparkSession

warnings.filterwarnings("ignore", message="PySpark does not yet fully support pandas")

# Applied in both environments so behaviour is identical locally and on Glue.
SPARK_CONF = {
    # Malformed values become NULL and are caught by data-quality rules instead of failing the job.
    "spark.sql.ansi.enabled": "false",
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    # Delivery dates are business dates in India.
    "spark.sql.session.timeZone": "Asia/Kolkata",
    # Re-running a batch replaces only its own partitions (idempotent loads).
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    # Microsecond timestamps load cleanly into Redshift via COPY ... FORMAT AS PARQUET.
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
}


def apply_conf(spark: SparkSession) -> SparkSession:
    for key, value in SPARK_CONF.items():
        spark.conf.set(key, value)
    return spark


def build_local_spark(app_name: str = "delivery-pipeline", cores: str = "*",
                      driver_memory: str = "3g") -> SparkSession:
    builder = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
    )
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
