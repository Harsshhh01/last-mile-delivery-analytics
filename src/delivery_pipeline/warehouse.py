"""Local warehouse: loads the curated star schema into DuckDB and runs the KPI SQL.

DuckDB stands in for Amazon Redshift on a laptop. The KPI queries in sql/kpis are
written in the SQL subset both engines share, and sql/redshift holds the Redshift DDL
and COPY commands for the AWS deployment.
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

STAR_TABLES = ("dim_date", "dim_dsp", "dim_station", "dim_driver", "fact_delivery_attempt")
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_KPI_DIR = REPO_ROOT / "sql" / "kpis"


def load_star_schema(curated_root: str, db_path: str) -> dict[str, int]:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    with duckdb.connect(db_path) as con:
        for table in STAR_TABLES:
            hive = "true" if table == "fact_delivery_attempt" else "false"
            pattern = f"{curated_root}/{table}/**/*.parquet"
            con.execute(
                f"CREATE OR REPLACE TABLE {table} AS "
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = {hive})"
            )
            counts[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        orphans = con.execute("""
            SELECT COUNT(*) FROM fact_delivery_attempt f
            LEFT JOIN dim_dsp d ON f.dsp_key = d.dsp_key
            LEFT JOIN dim_station s ON f.station_key = s.station_key
            LEFT JOIN dim_driver r ON f.driver_key = r.driver_key
            LEFT JOIN dim_date t ON f.date_key = t.date_key
            WHERE d.dsp_key IS NULL OR s.station_key IS NULL OR r.driver_key IS NULL OR t.date_key IS NULL
        """).fetchone()[0]
    if orphans:
        raise ValueError(f"{orphans} fact rows have no matching dimension row (referential integrity)")
    return counts


def run_kpis(db_path: str, out_dir: str, sql_dir: str | Path = DEFAULT_KPI_DIR) -> dict[str, int]:
    """Run every sql/kpis/*.sql file and write each result to <out_dir>/<name>.csv."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    results = {}
    with duckdb.connect(db_path, read_only=True) as con:
        for sql_file in sorted(Path(sql_dir).glob("*.sql")):
            cur = con.execute(sql_file.read_text(encoding="utf-8"))
            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
            with open(out / f"{sql_file.stem}.csv", "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(columns)
                writer.writerows(rows)
            results[sql_file.stem] = len(rows)
    return results
