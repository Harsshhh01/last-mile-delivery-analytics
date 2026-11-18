"""Explicit schemas: raw events are read as strings so bad values never break the read."""

from __future__ import annotations

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

RAW_STRING_FIELDS = (
    "event_id", "package_id", "route_id", "station_code", "dsp_code", "driver_id",
    "customer_phone", "delivery_postcode", "service_type", "stop_sequence", "attempt_number",
    "status", "failure_reason", "package_weight_kg", "promised_by", "event_ts",
)
CORRUPT_RECORD = "_corrupt_record"

RAW_EVENT_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in RAW_STRING_FIELDS]
    + [StructField(CORRUPT_RECORD, StringType(), True)]
)

DSP_SCHEMA = StructType([
    StructField("dsp_code", StringType(), False),
    StructField("dsp_name", StringType(), False),
    StructField("onboarded_on", DateType(), True),
    StructField("fleet_size", IntegerType(), True),
])

STATION_SCHEMA = StructType([
    StructField("station_code", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("region", StringType(), False),
    StructField("pincode_prefix", StringType(), True),
])
