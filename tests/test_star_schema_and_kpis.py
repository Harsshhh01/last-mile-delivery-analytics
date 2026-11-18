import csv
from pathlib import Path

import duckdb
import sqlglot

from delivery_pipeline.reference_data import DSPS, STATIONS
from delivery_pipeline.warehouse import DEFAULT_KPI_DIR, run_kpis

REDSHIFT_SQL = Path(__file__).resolve().parents[1] / "sql" / "redshift"


def test_star_schema_counts(pipeline_run, warehouse):
    counts = warehouse["counts"]
    assert counts["fact_delivery_attempt"] == pipeline_run["result"]["quality"]["passed"]
    assert counts["dim_dsp"] == len(DSPS)
    assert counts["dim_station"] == len(STATIONS)
    assert counts["dim_driver"] > 0


def test_dimensions_have_unique_keys_and_continuous_dates(warehouse):
    with duckdb.connect(warehouse["db"], read_only=True) as con:
        for table, key in (("dim_dsp", "dsp_key"), ("dim_station", "station_key"),
                           ("dim_driver", "driver_key"), ("dim_date", "date_key")):
            total, distinct = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT {key}) FROM {table}").fetchone()
            assert total == distinct, table
        days, span = con.execute(
            "SELECT COUNT(*), MAX(calendar_date) - MIN(calendar_date) + 1 FROM dim_date"
        ).fetchone()
        assert days == span


def test_fact_grain_one_row_per_event(warehouse):
    with duckdb.connect(warehouse["db"], read_only=True) as con:
        total, distinct = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT event_id) FROM fact_delivery_attempt"
        ).fetchone()
    assert total == distinct


def test_kpi_reports(warehouse, tmp_path):
    results = run_kpis(warehouse["db"], str(tmp_path))
    assert len(results) == 4 and all(n > 0 for n in results.values())
    for path in tmp_path.glob("*.csv"):
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        for row in rows:
            for col, value in row.items():
                if col.endswith("_pct") and value not in ("", None):
                    assert 0 <= float(value) <= 100, (path.name, col, value)
    with open(tmp_path / "01_dsp_weekly_scorecard.csv", newline="") as fh:
        assert {r["dsp_code"] for r in csv.DictReader(fh)} == {d[0] for d in DSPS}


def test_kpi_sql_is_valid_redshift():
    for path in list(DEFAULT_KPI_DIR.glob("*.sql")) + list(REDSHIFT_SQL.glob("*.sql")):
        sql = path.read_text().replace(":bucket", "bucket").replace(":role_arn", "arn:aws:iam::1:role/r")
        statements = [s for s in sqlglot.parse(sql, read="redshift") if s is not None]
        assert statements, path.name
