"""Data-quality gate: rule checks, quarantine split and a pass-rate circuit breaker."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .reference_data import FAILURE_REASONS, SERVICE_TYPES
from .schemas import CORRUPT_RECORD

VALID_STATUSES = ("DELIVERED", "FAILED")
DQ_COLUMN = "dq_failed_rules"


class DataQualityError(RuntimeError):
    """Raised when a batch falls below the configured pass rate; nothing is published."""


def rules() -> list[tuple[str, Column]]:
    """(rule name, condition every valid record must satisfy)."""
    status = F.col("status")
    reason = F.col("failure_reason")
    return [
        ("record_parseable", F.col(CORRUPT_RECORD).isNull()),
        ("event_id_present", F.col("event_id").isNotNull()),
        ("package_id_present", F.col("package_id").isNotNull()),
        ("event_ts_valid", F.col("event_ts").isNotNull()),
        ("promised_by_valid", F.col("promised_by").isNotNull()),
        ("status_valid", status.isin(*VALID_STATUSES)),
        ("attempt_number_in_range", F.col("attempt_number").between(1, 3)),
        ("weight_in_range", F.col("package_weight_kg").between(0.01, 50.0)),
        ("service_type_valid", F.col("service_type").isin(*SERVICE_TYPES)),
        (
            "failure_reason_consistent",
            ((status == "FAILED") & reason.isin(*FAILURE_REASONS))
            | ((status == "DELIVERED") & reason.isNull()),
        ),
        ("dsp_code_known", F.col("dsp_name").isNotNull()),
        ("station_code_known", F.col("city").isNotNull()),
    ]


# Static copy (Spark columns can only be built once a session exists); a test keeps it in sync.
RULE_NAMES = (
    "record_parseable", "event_id_present", "package_id_present", "event_ts_valid",
    "promised_by_valid", "status_valid", "attempt_number_in_range", "weight_in_range",
    "service_type_valid", "failure_reason_consistent", "dsp_code_known", "station_code_known",
)


def apply_rules(df: DataFrame) -> DataFrame:
    """Add dq_failed_rules: an array with the name of every rule the record breaks."""
    flags = F.array(*[
        F.when(F.coalesce(cond, F.lit(False)), F.lit(None).cast("string")).otherwise(F.lit(name))
        for name, cond in rules()
    ])
    return df.withColumn(DQ_COLUMN, F.filter(flags, lambda x: x.isNotNull()))


def valid_records(checked: DataFrame) -> DataFrame:
    return checked.where(F.size(DQ_COLUMN) == 0).drop(DQ_COLUMN, CORRUPT_RECORD)


def quarantined_records(checked: DataFrame) -> DataFrame:
    return checked.where(F.size(DQ_COLUMN) > 0)


@dataclass
class QualityReport:
    total: int
    passed: int
    failed: int
    pass_rate: float
    failures_by_rule: dict[str, int] = field(default_factory=dict)
    duplicates_removed: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def summarize(checked: DataFrame, duplicates_removed: int = 0) -> QualityReport:
    """One aggregation pass: totals plus a failure count per rule."""
    aggs = [
        F.count(F.lit(1)).alias("_total"),
        F.sum(F.when(F.size(DQ_COLUMN) == 0, 1).otherwise(0)).alias("_passed"),
    ] + [F.sum(F.when(F.array_contains(DQ_COLUMN, n), 1).otherwise(0)).alias(n) for n in RULE_NAMES]
    row = checked.agg(*aggs).collect()[0].asDict()
    total = int(row.pop("_total") or 0)
    passed = int(row.pop("_passed") or 0)
    return QualityReport(
        total=total,
        passed=passed,
        failed=total - passed,
        pass_rate=round(passed / total, 6) if total else 0.0,
        failures_by_rule={k: int(v or 0) for k, v in row.items()},
        duplicates_removed=duplicates_removed,
    )


def assert_quality(report: QualityReport, min_pass_rate: float) -> None:
    if report.total == 0:
        raise DataQualityError("batch is empty")
    if report.pass_rate < min_pass_rate:
        worst = sorted(report.failures_by_rule.items(), key=lambda kv: -kv[1])[:3]
        raise DataQualityError(
            f"pass rate {report.pass_rate:.2%} is below the {min_pass_rate:.2%} threshold; "
            f"top failing rules: {worst}"
        )
