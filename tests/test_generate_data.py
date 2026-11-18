import gzip
import json
from datetime import date

from delivery_pipeline.generate_data import COLUMNS, generate_events, inject_dirty_data, write_jsonl_gz
from delivery_pipeline.reference_data import DSPS, STATIONS, write_reference_csvs


def test_events_have_expected_columns_and_are_deterministic():
    a = generate_events(500, date(2026, 10, 1), 10, seed=3)
    b = generate_events(500, date(2026, 10, 1), 10, seed=3)
    assert list(a.columns) == COLUMNS
    assert a.equals(b)


def test_attempt_rules_hold():
    events = generate_events(2000, date(2026, 10, 1), 10, seed=1)
    assert set(events["attempt_number"]) <= {1, 2, 3}
    assert events["package_id"].nunique() == 2000
    failed = events[events["status"] == "FAILED"]
    delivered = events[events["status"] == "DELIVERED"]
    assert failed["failure_reason"].notna().all()
    assert delivered["failure_reason"].isna().all()
    # A package is delivered at most once, and retries only follow a failure.
    assert delivered["package_id"].is_unique
    retried = set(events.loc[events["attempt_number"] == 2, "package_id"])
    first_attempt_failed = (events["attempt_number"] == 1) & (events["status"] == "FAILED")
    first_failed = set(events.loc[first_attempt_failed, "package_id"])
    assert retried <= first_failed


def test_stations_belong_to_their_dsp():
    events = generate_events(1000, date(2026, 10, 1), 5, seed=2)
    served = {code: set(stations) for code, _, _, _, stations, _, _ in DSPS}
    assert all(st in served[dsp] for dsp, st in zip(events["dsp_code"], events["station_code"]))


def test_dirty_data_injection_is_counted():
    clean = generate_events(5000, date(2026, 10, 1), 10, seed=4)
    dirty, injected = inject_dirty_data(clean, seed=4)
    assert len(dirty) - len(clean) == injected["exact_duplicates"]
    # Duplicated rows can repeat a corrupted row, so counts are lower bounds.
    assert (dirty["event_ts"] == "N/A").sum() >= injected["event_ts_invalid"]
    assert (dirty["dsp_code"] == "DSP-UNKNOWN").sum() >= injected["dsp_code_unknown"]
    assert dirty["package_id"].isna().sum() >= injected["package_id_missing"]


def test_jsonl_writer_injects_malformed_lines(tmp_path):
    events = generate_events(4000, date(2026, 10, 1), 5, seed=5)
    paths = write_jsonl_gz(events, tmp_path, "batch", rows_per_file=10_000, malformed_share=0.001)
    bad = total = 0
    with gzip.open(paths[0], "rt", encoding="utf-8") as fh:
        for line in fh:
            total += 1
            try:
                json.loads(line)
            except json.JSONDecodeError:
                bad += 1
    assert total == len(events)
    assert bad == int(len(events) * 0.001)


def test_reference_csvs(tmp_path):
    paths = write_reference_csvs(tmp_path)
    assert len(paths["dsps"].read_text().strip().splitlines()) == len(DSPS) + 1
    assert len(paths["stations"].read_text().strip().splitlines()) == len(STATIONS) + 1
