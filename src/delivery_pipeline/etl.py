"""The ETL job: raw -> processed (silver) -> curated star schema (gold).

Shared by the local runner and the AWS Glue job, so the same code is tested locally
and deployed to Glue.
"""

from __future__ import annotations

import json

from pyspark.sql import SparkSession

from .config import PipelineConfig
from .io_utils import write_text
from .metrics import RunMetrics
from .model import build_star_schema
from .quality import (
    apply_rules,
    assert_quality,
    quarantined_records,
    summarize,
    valid_records,
)
from .transform import clean_events, drop_exact_duplicates, enrich, read_raw_events, read_reference


def run_etl(spark: SparkSession, cfg: PipelineConfig, metrics: RunMetrics | None = None) -> dict:
    m = metrics or RunMetrics(run_date=cfg.run_date)

    with m.stage("extract"):
        raw = read_raw_events(spark, cfg.raw_glob, cfg.run_date)
        dsps, stations = read_reference(spark, cfg.zone("reference"))
        raw_count = raw.count()
    m.set("raw_records", raw_count)

    with m.stage("transform"):
        deduped = drop_exact_duplicates(clean_events(raw, cfg.pii_salt))
        checked = apply_rules(enrich(deduped, dsps, stations)).cache()

    with m.stage("data_quality"):
        report = summarize(checked)
        report.duplicates_removed = raw_count - report.total
        quarantined_records(checked).write.mode("overwrite").partitionBy("ingest_date").parquet(
            cfg.quarantine_events
        )
        write_text(
            f"{cfg.zone('quality_reports')}/ingest_date={cfg.run_date}/dq_report.json",
            json.dumps(report.to_dict(), indent=2),
        )
        m.set("valid_records", report.passed)
        m.set("quarantined_records", report.failed)
        m.set("duplicates_removed", report.duplicates_removed)
        m.set("dq_pass_rate_pct", round(report.pass_rate * 100, 3))
        # Circuit breaker: a bad batch is quarantined and reported but never published.
        assert_quality(report, cfg.min_dq_pass_rate)

    with m.stage("load_processed"):
        valid_records(checked).write.mode("overwrite").partitionBy("ingest_date").parquet(
            cfg.processed_events
        )
    checked.unpersist()

    with m.stage("build_curated"):
        processed = spark.read.parquet(cfg.processed_events)
        tables = build_star_schema(spark, processed, dsps, stations)
        row_counts = {}
        for name, df in tables.items():
            path = f"{cfg.zone('curated')}/{name}"
            if name == "fact_delivery_attempt":
                # Gold is rebuilt from silver each run, so replace the whole table.
                (df.write.mode("overwrite").option("partitionOverwriteMode", "static")
                   .partitionBy("delivery_date").parquet(path))
            else:
                df.coalesce(1).write.mode("overwrite").parquet(path)
            row_counts[name] = spark.read.parquet(path).count()
    m.set("curated_row_counts", row_counts)

    return {"quality": report.to_dict(), "curated_row_counts": row_counts, "metrics": m.to_dict()}
