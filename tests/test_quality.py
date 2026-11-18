import pytest

from delivery_pipeline.quality import (
    RULE_NAMES,
    DataQualityError,
    QualityReport,
    apply_rules,
    assert_quality,
    rules,
    summarize,
)
from delivery_pipeline.transform import clean_events, enrich
from tests.helpers import raw_df, reference


def _check(spark, *overrides):
    dsps, stations = reference(spark)
    checked = apply_rules(enrich(clean_events(raw_df(spark, *overrides), "s"), dsps, stations))
    return checked.orderBy("event_id").collect()


def test_rule_names_are_in_sync(spark):
    assert tuple(name for name, _ in rules()) == RULE_NAMES


def test_clean_record_passes(spark):
    assert _check(spark, {})[0].dq_failed_rules == []


@pytest.mark.parametrize(
    "override, rule",
    [
        ({"_corrupt_record": "{broken"}, "record_parseable"),
        ({"package_id": None}, "package_id_present"),
        ({"event_ts": "N/A"}, "event_ts_valid"),
        ({"promised_by": "tomorrow"}, "promised_by_valid"),
        ({"status": "LOST"}, "status_valid"),
        ({"attempt_number": "0"}, "attempt_number_in_range"),
        ({"attempt_number": "nine"}, "attempt_number_in_range"),
        ({"package_weight_kg": "-1"}, "weight_in_range"),
        ({"package_weight_kg": "999"}, "weight_in_range"),
        ({"service_type": "TELEPORT"}, "service_type_valid"),
        ({"failure_reason": "CUSTOMER_UNAVAILABLE"}, "failure_reason_consistent"),
        ({"status": "FAILED", "failure_reason": None}, "failure_reason_consistent"),
        ({"status": "FAILED", "failure_reason": "ALIENS"}, "failure_reason_consistent"),
        ({"dsp_code": "DSP-UNKNOWN"}, "dsp_code_known"),
        ({"station_code": "XXX9"}, "station_code_known"),
    ],
)
def test_each_rule_catches_its_defect(spark, override, rule):
    assert rule in _check(spark, override)[0].dq_failed_rules


def test_summary_counts_every_failed_rule(spark):
    dsps, stations = reference(spark)
    checked = apply_rules(enrich(clean_events(raw_df(
        spark, {}, {}, {"package_id": None, "dsp_code": "DSP-UNKNOWN"}, {"event_ts": "N/A"},
    ), "s"), dsps, stations))
    report = summarize(checked)
    assert (report.total, report.passed, report.failed) == (4, 2, 2)
    assert report.pass_rate == 0.5
    assert report.failures_by_rule["package_id_present"] == 1
    assert report.failures_by_rule["dsp_code_known"] == 1
    assert report.failures_by_rule["event_ts_valid"] == 1


def test_circuit_breaker():
    assert_quality(QualityReport(100, 99, 1, 0.99), 0.95)
    with pytest.raises(DataQualityError, match="below"):
        assert_quality(QualityReport(100, 90, 10, 0.90, {"weight_in_range": 10}), 0.95)
    with pytest.raises(DataQualityError, match="empty"):
        assert_quality(QualityReport(0, 0, 0, 0.0), 0.95)
