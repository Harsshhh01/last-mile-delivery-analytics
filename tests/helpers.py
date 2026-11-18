"""Build tiny raw-event DataFrames for Spark unit tests."""

from __future__ import annotations

from delivery_pipeline.schemas import RAW_EVENT_SCHEMA

BASE_EVENT = {
    "event_id": "e1",
    "package_id": "PKG1",
    "route_id": "RT-HYD1-20261001-001",
    "station_code": "HYD1",
    "dsp_code": "DSP-ARROW",
    "driver_id": "ARROW-DRV-0001",
    "customer_phone": "+919876543210",
    "delivery_postcode": "500081",
    "service_type": "STANDARD",
    "stop_sequence": "12",
    "attempt_number": "1",
    "status": "DELIVERED",
    "failure_reason": None,
    "package_weight_kg": "1.5",
    "promised_by": "2026-10-01T21:00:00+05:30",
    "event_ts": "2026-10-01T19:30:00+05:30",
    "_corrupt_record": None,
}


def raw_df(spark, *overrides: dict):
    rows = []
    for i, override in enumerate(overrides or ({},)):
        event = dict(BASE_EVENT, event_id=f"e{i}")
        event.update(override)
        rows.append(tuple(event[f.name] for f in RAW_EVENT_SCHEMA.fields))
    return spark.createDataFrame(rows, RAW_EVENT_SCHEMA)


def reference(spark):
    import datetime as dt

    from delivery_pipeline.reference_data import DSPS, STATIONS
    from delivery_pipeline.schemas import DSP_SCHEMA, STATION_SCHEMA

    dsps = spark.createDataFrame(
        [(c, n, dt.date.fromisoformat(o), f) for c, n, o, f, *_ in DSPS], DSP_SCHEMA
    )
    stations = spark.createDataFrame(STATIONS, STATION_SCHEMA)
    return dsps, stations
