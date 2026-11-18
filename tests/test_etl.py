"""End-to-end ETL on a generated dirty batch."""

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from delivery_pipeline.config import PipelineConfig
from delivery_pipeline.etl import run_etl
from delivery_pipeline.generate_data import generate_to_lake
from delivery_pipeline.ingest import ingest_local
from delivery_pipeline.quality import DataQualityError

# injected defect -> rule that must catch it
DEFECT_TO_RULE = {
    "event_ts_invalid": "event_ts_valid",
    "package_id_missing": "package_id_present",
    "weight_out_of_range": "weight_in_range",
    "dsp_code_unknown": "dsp_code_known",
    "attempt_number_out_of_range": "attempt_number_in_range",
    "delivered_with_failure_reason": "failure_reason_consistent",
}


def test_every_injected_defect_is_quarantined(pipeline_run):
    quality = pipeline_run["result"]["quality"]
    injected = pipeline_run["generated"]["injected"]
    malformed = injected["malformed_json_lines"]
    assert quality["failures_by_rule"]["record_parseable"] == malformed
    for defect, rule in DEFECT_TO_RULE.items():
        # A malformed line can coincide with an injected defect, so allow that overlap.
        assert injected[defect] <= quality["failures_by_rule"][rule] <= injected[defect] + malformed, defect
    # A duplicate whose copy was truncated is quarantined instead of de-duplicated.
    dupes = injected["exact_duplicates"]
    assert dupes - malformed <= quality["duplicates_removed"] <= dupes
    assert quality["pass_rate"] > 0.95


def test_cleanable_issues_are_repaired(spark, pipeline_run):
    processed = spark.read.parquet(pipeline_run["cfg"].processed_events)
    assert processed.where(F.col("station_code") != F.upper("station_code")).count() == 0
    assert {r.status for r in processed.select("status").distinct().collect()} == {"DELIVERED", "FAILED"}
    assert processed.where(F.col("event_ts").isNull()).count() == 0
    # ~3% of rows had non-ISO timestamps; all of them must have survived.
    assert processed.count() == pipeline_run["result"]["quality"]["passed"]


def test_no_pii_leaves_the_raw_zone(spark, pipeline_run):
    cfg = pipeline_run["cfg"]
    for path in (cfg.processed_events, f"{cfg.zone('curated')}/fact_delivery_attempt",
                 f"{cfg.zone('curated')}/dim_driver"):
        df = spark.read.parquet(path)
        assert not {"driver_id", "customer_phone", "delivery_postcode"} & set(df.columns)
    drivers = spark.read.parquet(f"{cfg.zone('curated')}/dim_driver")
    assert drivers.where(F.length("driver_hash") != 64).count() == 0
    assert drivers.where(F.col("driver_hash").contains("DRV")).count() == 0


def test_quarantine_explains_every_rejection(spark, pipeline_run):
    q = spark.read.parquet(pipeline_run["cfg"].quarantine_events)
    assert q.count() == pipeline_run["result"]["quality"]["failed"]
    assert q.where(F.size("dq_failed_rules") == 0).count() == 0


def test_dq_report_is_written(pipeline_run):
    cfg = pipeline_run["cfg"]
    report = Path(cfg.zone("quality_reports")) / f"ingest_date={cfg.run_date}" / "dq_report.json"
    assert report.exists()


def test_rerun_is_idempotent(spark, pipeline_run):
    before = pipeline_run["result"]["curated_row_counts"]
    again = run_etl(spark, pipeline_run["cfg"])
    assert again["curated_row_counts"] == before


def test_bad_batch_is_blocked_before_publishing(spark, tmp_path):
    generate_to_lake(str(tmp_path), 300, date(2026, 10, 1), 5, "2026-11-02", seed=11)
    ingest_local(str(tmp_path), "2026-11-02")
    cfg = PipelineConfig(lake_root=str(tmp_path), run_date="2026-11-02", min_dq_pass_rate=0.9999)
    with pytest.raises(DataQualityError):
        run_etl(spark, cfg)
    assert not Path(cfg.processed_events).exists()
    assert not Path(cfg.zone("curated")).exists()
    assert Path(cfg.quarantine_events).exists()  # bad rows are still kept for investigation
