import hashlib
from datetime import datetime, timezone

from pyspark.sql import functions as F

from delivery_pipeline.transform import (
    PII_FIELDS_DROPPED,
    clean_events,
    drop_exact_duplicates,
    enrich,
    parse_timestamp,
)
from tests.helpers import raw_df, reference


def test_parse_timestamp_accepts_all_upstream_formats(spark):
    # 2026-10-01 09:14 IST == 03:44 UTC == epoch 1790826240000 ms
    df = spark.createDataFrame(
        [("2026-10-01T09:14:00+05:30",), ("01/10/2026 09:14",), ("1790826240000",), ("N/A",), ("",)],
        "v string",
    )
    ts = parse_timestamp(F.col("v"))
    out = df.select(
        F.date_format(ts, "yyyy-MM-dd HH:mm").alias("ist"),  # rendered in the session zone (IST)
        F.unix_timestamp(ts).alias("epoch"),
    ).collect()
    assert [r.ist for r in out[:3]] == ["2026-10-01 09:14"] * 3
    expected_utc = datetime(2026, 10, 1, 3, 44, tzinfo=timezone.utc).timestamp()
    assert [r.epoch for r in out[:3]] == [expected_utc] * 3
    assert out[3].ist is None and out[4].ist is None


def test_clean_standardises_and_masks_pii(spark):
    raw = raw_df(spark, {"status": " delivered ", "station_code": "hyd1", "package_weight_kg": "2.25"})
    row = clean_events(raw, pii_salt="s3cret").collect()[0].asDict()
    assert row["status"] == "DELIVERED"
    assert row["station_code"] == "HYD1"
    assert row["package_weight_kg"] == 2.25
    assert row["attempt_number"] == 1
    assert row["driver_hash"] == hashlib.sha256(b"s3cretARROW-DRV-0001").hexdigest()
    assert row["postcode_prefix"] == "500"
    assert not set(PII_FIELDS_DROPPED) & set(row)


def test_blank_strings_become_null(spark):
    row = clean_events(raw_df(spark, {"route_id": "   "}), "s").collect()[0]
    assert row.route_id is None


def test_dedupe_drops_repeats_but_keeps_rows_without_id(spark):
    raw = raw_df(spark, {}, {}, {}, {"event_id": None}, {"event_id": None})
    raw = raw.withColumn("event_id", F.when(F.col("event_id").isNull(), None).otherwise(F.lit("same")))
    out = drop_exact_duplicates(raw)
    assert out.where(F.col("event_id") == "same").count() == 1
    assert out.where(F.col("event_id").isNull()).count() == 2


def test_enrich_derives_delivery_kpis(spark):
    dsps, stations = reference(spark)
    raw = raw_df(
        spark,
        {},  # delivered 90 minutes early on attempt 1
        {"event_ts": "2026-10-01T22:00:00+05:30"},  # delivered late
        {"status": "FAILED", "failure_reason": "WEATHER_DELAY"},
        {"attempt_number": "2"},
    )
    rows = enrich(clean_events(raw, "s"), dsps, stations).orderBy("event_id").collect()
    early, late, failed, second = rows
    assert (early.is_on_time, early.minutes_vs_promise, early.is_first_attempt_delivery) == (True, -90, True)
    assert (late.is_on_time, late.minutes_vs_promise) == (False, 60)
    assert failed.is_on_time is False and failed.is_first_attempt_delivery is False
    assert second.is_first_attempt_delivery is False
    assert early.city == "Hyderabad" and early.dsp_name == "Arrow Last Mile"
    assert str(early.delivery_date) == "2026-10-01"
