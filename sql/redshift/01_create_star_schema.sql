-- Amazon Redshift star schema for last-mile delivery analytics.
-- Small dimensions are replicated to every node (DISTSTYLE ALL); the fact table is
-- distributed on station_key (the most common join/filter) and sorted on date_key
-- so date-range scans skip blocks.

CREATE SCHEMA IF NOT EXISTS delivery;

CREATE TABLE IF NOT EXISTS delivery.dim_date (
    date_key        INTEGER      NOT NULL PRIMARY KEY,
    calendar_date   DATE         NOT NULL,
    year            INTEGER      NOT NULL,
    month           INTEGER      NOT NULL,
    day_of_month    INTEGER      NOT NULL,
    day_name        VARCHAR(10)  NOT NULL,
    week_of_year    INTEGER      NOT NULL,
    is_weekend      BOOLEAN      NOT NULL
) DISTSTYLE ALL SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS delivery.dim_dsp (
    dsp_key         BIGINT       NOT NULL PRIMARY KEY,
    dsp_code        VARCHAR(32)  NOT NULL,
    dsp_name        VARCHAR(128) NOT NULL,
    onboarded_on    DATE,
    fleet_size      INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS delivery.dim_station (
    station_key     BIGINT       NOT NULL PRIMARY KEY,
    station_code    VARCHAR(16)  NOT NULL,
    city            VARCHAR(64)  NOT NULL,
    state           VARCHAR(64)  NOT NULL,
    region          VARCHAR(16)  NOT NULL
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS delivery.dim_driver (
    driver_key      BIGINT       NOT NULL PRIMARY KEY,
    driver_hash     CHAR(64)     NOT NULL,   -- salted SHA-256 pseudonym, never the real id
    dsp_code        VARCHAR(32)
) DISTSTYLE ALL;

-- Column order matches the Parquet files written by the Glue job (COPY maps by position).
CREATE TABLE IF NOT EXISTS delivery.fact_delivery_attempt (
    event_id                   VARCHAR(64)   NOT NULL,
    package_id                 VARCHAR(32)   NOT NULL,
    route_id                   VARCHAR(48),
    date_key                   INTEGER       NOT NULL REFERENCES delivery.dim_date (date_key),
    dsp_key                    BIGINT        NOT NULL REFERENCES delivery.dim_dsp (dsp_key),
    station_key                BIGINT        NOT NULL REFERENCES delivery.dim_station (station_key),
    driver_key                 BIGINT        NOT NULL REFERENCES delivery.dim_driver (driver_key),
    service_type               VARCHAR(16),
    attempt_number             INTEGER,
    stop_sequence              INTEGER,
    status                     VARCHAR(16)   NOT NULL,
    failure_reason             VARCHAR(32),
    package_weight_kg          DOUBLE PRECISION,
    event_ts                   TIMESTAMPTZ   NOT NULL,
    promised_by                TIMESTAMPTZ   NOT NULL,
    minutes_vs_promise         INTEGER,
    is_on_time                 BOOLEAN,
    is_first_attempt_delivery  BOOLEAN,
    postcode_prefix            CHAR(3)
)
DISTKEY (station_key)
COMPOUND SORTKEY (date_key, dsp_key);
