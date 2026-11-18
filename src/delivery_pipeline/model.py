"""Gold layer: a Kimball-style star schema built with Spark SQL.

fact_delivery_attempt (one row per attempt)
    -> dim_date, dim_dsp, dim_station, dim_driver

Surrogate keys are deterministic 64-bit hashes of the natural key, so they are stable
across rebuilds and identical in Redshift and the local DuckDB warehouse.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

STAR_SCHEMA_SQL = {
    "dim_dsp": """
        SELECT xxhash64(dsp_code) AS dsp_key, dsp_code, dsp_name, onboarded_on, fleet_size
        FROM ref_dsps
    """,
    "dim_station": """
        SELECT xxhash64(station_code) AS station_key, station_code, city, state, region
        FROM ref_stations
    """,
    "dim_driver": """
        SELECT xxhash64(driver_hash) AS driver_key, driver_hash, MAX(dsp_code) AS dsp_code
        FROM latest_events
        GROUP BY driver_hash
    """,
    "dim_date": """
        SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
               d AS calendar_date,
               year(d) AS year,
               month(d) AS month,
               dayofmonth(d) AS day_of_month,
               date_format(d, 'EEEE') AS day_name,
               weekofyear(d) AS week_of_year,
               dayofweek(d) IN (1, 7) AS is_weekend
        FROM (
            SELECT explode(sequence(MIN(delivery_date), MAX(delivery_date), INTERVAL 1 DAY)) AS d
            FROM latest_events
        ) days
    """,
    # Column order matters: Redshift COPY maps Parquet columns by position.
    # delivery_date is the partition column, so it is not stored inside the files.
    "fact_delivery_attempt": """
        SELECT event_id,
               package_id,
               route_id,
               CAST(date_format(delivery_date, 'yyyyMMdd') AS INT) AS date_key,
               xxhash64(dsp_code) AS dsp_key,
               xxhash64(station_code) AS station_key,
               xxhash64(driver_hash) AS driver_key,
               service_type,
               attempt_number,
               stop_sequence,
               status,
               failure_reason,
               package_weight_kg,
               event_ts,
               promised_by,
               minutes_vs_promise,
               is_on_time,
               is_first_attempt_delivery,
               postcode_prefix,
               delivery_date
        FROM latest_events
    """,
}

# Latest version of each event across all ingest batches (late or re-sent data).
LATEST_EVENTS_SQL = """
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY event_id ORDER BY ingest_date DESC, event_ts DESC
        ) AS _rn
        FROM processed_events
    ) ranked
    WHERE _rn = 1
"""


def build_star_schema(spark: SparkSession, processed: DataFrame, dsps: DataFrame,
                      stations: DataFrame) -> dict[str, DataFrame]:
    processed.createOrReplaceTempView("processed_events")
    dsps.createOrReplaceTempView("ref_dsps")
    stations.createOrReplaceTempView("ref_stations")
    spark.sql(LATEST_EVENTS_SQL).drop("_rn").createOrReplaceTempView("latest_events")
    return {name: spark.sql(sql) for name, sql in STAR_SCHEMA_SQL.items()}
