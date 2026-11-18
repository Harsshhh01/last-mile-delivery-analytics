"""Extract and transform: read raw JSON, clean, mask PII, de-duplicate and enrich."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import CORRUPT_RECORD, DSP_SCHEMA, RAW_EVENT_SCHEMA, RAW_STRING_FIELDS, STATION_SCHEMA

UPPERCASE_FIELDS = ("station_code", "dsp_code", "status", "failure_reason", "service_type")
PII_FIELDS_DROPPED = ("driver_id", "customer_phone", "delivery_postcode")


def read_raw_events(spark: SparkSession, path: str, ingest_date: str) -> DataFrame:
    """Read gzipped JSON Lines; malformed lines are kept in _corrupt_record, not dropped."""
    df = (
        spark.read.schema(RAW_EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .json(path)
    )
    if "ingest_date" in df.columns:
        df = df.drop("ingest_date")
    return df.withColumn("ingest_date", F.lit(ingest_date).cast("date"))


def read_reference(spark: SparkSession, reference_root: str) -> tuple[DataFrame, DataFrame]:
    dsps = spark.read.option("header", True).schema(DSP_SCHEMA).csv(f"{reference_root}/dsps")
    stations = spark.read.option("header", True).schema(STATION_SCHEMA).csv(f"{reference_root}/stations")
    return dsps, stations


def parse_timestamp(col: Column) -> Column:
    """Accept ISO-8601 with offset, 'dd/MM/yyyy HH:mm' (IST) and epoch milliseconds."""
    c = F.trim(col)
    return F.coalesce(
        F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}T"), F.to_timestamp(c)),
        F.to_timestamp(c, "dd/MM/yyyy HH:mm"),
        F.when(c.rlike(r"^\d{13}$"), (c.cast("double") / 1000).cast("timestamp")),
    )


def clean_events(raw: DataFrame, pii_salt: str) -> DataFrame:
    """Standardise text, cast types, parse timestamps and minimise PII."""
    df = raw
    for name in RAW_STRING_FIELDS:
        trimmed = F.trim(F.col(name))
        df = df.withColumn(name, F.when(trimmed == "", F.lit(None)).otherwise(trimmed))
    for name in UPPERCASE_FIELDS:
        df = df.withColumn(name, F.upper(F.col(name)))
    return (
        df.withColumn("attempt_number", F.col("attempt_number").cast("int"))
        .withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
        .withColumn("package_weight_kg", F.col("package_weight_kg").cast("double"))
        .withColumn("event_ts", parse_timestamp(F.col("event_ts")))
        .withColumn("promised_by", parse_timestamp(F.col("promised_by")))
        # Privacy: drivers become a salted SHA-256 pseudonym, phone numbers are dropped and
        # postcodes are generalised to their 3-digit sorting district.
        .withColumn(
            "driver_hash",
            F.when(
                F.col("driver_id").isNotNull(),
                F.sha2(F.concat(F.lit(pii_salt), F.col("driver_id")), 256),
            ),
        )
        .withColumn("postcode_prefix", F.substring("delivery_postcode", 1, 3))
        .drop(*PII_FIELDS_DROPPED)
    )


def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Drop repeated event_ids (upstream retries). Rows without an id are kept for the DQ gate."""
    keyed = df.where(F.col("event_id").isNotNull()).dropDuplicates(["event_id"])
    unkeyed = df.where(F.col("event_id").isNull())
    return keyed.unionByName(unkeyed)


def enrich(df: DataFrame, dsps: DataFrame, stations: DataFrame) -> DataFrame:
    """Join master data and derive delivery KPIs used downstream."""
    dsp_ref = F.broadcast(dsps.select("dsp_code", "dsp_name"))
    station_ref = F.broadcast(stations.select("station_code", "city", "state", "region"))
    delivered = F.col("status") == "DELIVERED"
    return (
        df.join(dsp_ref, "dsp_code", "left")
        .join(station_ref, "station_code", "left")
        .withColumn("delivery_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn(
            "minutes_vs_promise",
            F.round((F.col("event_ts").cast("long") - F.col("promised_by").cast("long")) / 60).cast("int"),
        )
        .withColumn("is_on_time", delivered & (F.col("event_ts") <= F.col("promised_by")))
        .withColumn("is_first_attempt_delivery", delivered & (F.col("attempt_number") == 1))
    )
