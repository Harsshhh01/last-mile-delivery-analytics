"""Run the whole pipeline locally: generate -> ingest -> Spark ETL -> warehouse -> KPIs.

    python -m delivery_pipeline.run_pipeline --packages 1000000

Writes:
    lake/                      data lake zones (landing, raw, processed, quarantine, curated)
    warehouse/delivery.duckdb  star schema (local stand-in for Amazon Redshift)
    reports/                   KPI CSVs, data-quality report and run metrics
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import date, timedelta
from pathlib import Path

from .config import PipelineConfig
from .generate_data import generate_to_lake
from .ingest import ingest_local
from .metrics import RunMetrics


def main(argv: list[str] | None = None) -> dict:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lake", default="lake")
    p.add_argument("--warehouse", default="warehouse/delivery.duckdb")
    p.add_argument("--reports", default="reports")
    p.add_argument("--packages", type=int, default=100_000, help="packages to simulate (~1.1 attempts each)")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--start-date", default=None, help="first delivery day (default: run date - days)")
    p.add_argument("--run-date", default=date.today().isoformat(), help="ingest date of this batch")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--min-dq-pass-rate", type=float, default=0.95)
    p.add_argument("--skip-generate", action="store_true", help="use files already in lake/landing")
    p.add_argument("--fresh", action="store_true", help="delete the lake and warehouse first")
    a = p.parse_args(argv)

    if a.fresh:
        shutil.rmtree(a.lake, ignore_errors=True)
        Path(a.warehouse).unlink(missing_ok=True)
    start = date.fromisoformat(a.start_date) if a.start_date else date.fromisoformat(a.run_date) - timedelta(
        days=a.days)
    cfg = PipelineConfig(lake_root=str(Path(a.lake).resolve()), run_date=a.run_date,
                         min_dq_pass_rate=a.min_dq_pass_rate)
    metrics = RunMetrics(run_date=a.run_date)

    if not a.skip_generate:
        with metrics.stage("generate"):
            gen = generate_to_lake(cfg.lake_root, a.packages, start, a.days, a.run_date, a.seed)
        print(f"[generate] {gen['events_written']:,} raw events ({gen['packages']:,} packages)")
        metrics.set("injected_issues", gen["injected"])

    with metrics.stage("ingest"):
        ingested = ingest_local(cfg.lake_root, a.run_date)
    print(f"[ingest]   {len(ingested['accepted'])} files accepted, {len(ingested['rejected'])} rejected")

    from .etl import run_etl
    from .spark_session import build_local_spark

    spark = build_local_spark()
    try:
        etl = run_etl(spark, cfg, metrics)
    finally:
        spark.stop()
    q = etl["quality"]
    print(f"[etl]      {q['total']:,} unique events | {q['passed']:,} valid | {q['failed']:,} quarantined "
          f"| {q['duplicates_removed']:,} duplicates removed | pass rate {q['pass_rate']:.2%}")

    from .warehouse import load_star_schema, run_kpis

    with metrics.stage("load_warehouse"):
        counts = load_star_schema(cfg.zone("curated"), a.warehouse)
    print("[load]     " + ", ".join(f"{k}={v:,}" for k, v in counts.items()))
    with metrics.stage("kpis"):
        kpis = run_kpis(a.warehouse, a.reports)
    print(f"[kpis]     {len(kpis)} reports written to {a.reports}/")

    reports = Path(a.reports)
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "dq_report.json").write_text(json.dumps(q, indent=2), encoding="utf-8")
    summary = metrics.to_dict()
    summary["warehouse_row_counts"] = counts
    (reports / "run_metrics.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"[done]     total {summary['total_seconds']}s; metrics in {reports / 'run_metrics.json'}")
    return summary


if __name__ == "__main__":
    main()
