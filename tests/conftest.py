"""Shared fixtures: one Spark session and one end-to-end pipeline run for the whole suite."""

from __future__ import annotations

from datetime import date

import pytest

RUN_DATE = "2026-11-01"


@pytest.fixture(scope="session")
def spark():
    from delivery_pipeline.spark_session import build_local_spark

    session = build_local_spark("tests", cores="2", driver_memory="1g")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def pipeline_run(spark, tmp_path_factory):
    """Generate a small dirty batch, ingest it and run the full ETL once."""
    from delivery_pipeline.config import PipelineConfig
    from delivery_pipeline.etl import run_etl
    from delivery_pipeline.generate_data import generate_to_lake
    from delivery_pipeline.ingest import ingest_local

    root = tmp_path_factory.mktemp("lake")
    generated = generate_to_lake(str(root), 3000, date(2026, 10, 1), 30, RUN_DATE, seed=7)
    ingested = ingest_local(str(root), RUN_DATE)
    cfg = PipelineConfig(lake_root=str(root), run_date=RUN_DATE, pii_salt="test-salt")
    result = run_etl(spark, cfg)
    return {"cfg": cfg, "generated": generated, "ingested": ingested, "result": result}


@pytest.fixture(scope="session")
def warehouse(pipeline_run, tmp_path_factory):
    from delivery_pipeline.warehouse import load_star_schema

    db = str(tmp_path_factory.mktemp("warehouse") / "delivery.duckdb")
    counts = load_star_schema(pipeline_run["cfg"].zone("curated"), db)
    return {"db": db, "counts": counts}


@pytest.fixture
def aws_env(monkeypatch):
    """Fake credentials so moto never touches a real AWS account."""
    for key, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(key, value)
