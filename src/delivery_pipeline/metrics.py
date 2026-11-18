"""Run metrics: stage timings and record counts, published to CloudWatch on AWS."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any

# metric name -> (key in RunMetrics.values, CloudWatch unit)
CLOUDWATCH_METRICS = {
    "RawRecords": ("raw_records", "Count"),
    "ValidRecords": ("valid_records", "Count"),
    "QuarantinedRecords": ("quarantined_records", "Count"),
    "DuplicatesRemoved": ("duplicates_removed", "Count"),
    "DQPassRate": ("dq_pass_rate_pct", "Percent"),
    "PipelineDurationSeconds": ("total_seconds", "Seconds"),
}


class RunMetrics:
    def __init__(self, pipeline: str = "delivery-pipeline", run_date: str = "") -> None:
        self.pipeline = pipeline
        self.run_date = run_date
        self.stage_seconds: dict[str, float] = {}
        self.values: dict[str, Any] = {}

    @contextmanager
    def stage(self, name: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.stage_seconds[name] = round(time.perf_counter() - start, 2)

    def set(self, key: str, value: Any) -> None:
        self.values[key] = value

    def to_dict(self) -> dict:
        total = round(sum(self.stage_seconds.values()), 2)
        return {
            "pipeline": self.pipeline,
            "run_date": self.run_date,
            "stage_seconds": self.stage_seconds,
            "total_seconds": total,
            **self.values,
        }

    def cloudwatch_metric_data(self) -> list[dict]:
        data = self.to_dict()
        dims = [{"Name": "Pipeline", "Value": self.pipeline}]
        out = []
        for metric, (key, unit) in CLOUDWATCH_METRICS.items():
            if data.get(key) is not None:
                out.append(
                    {"MetricName": metric, "Dimensions": dims, "Value": float(data[key]), "Unit": unit}
                )
        return out

    def publish_cloudwatch(self, namespace: str = "DeliveryPipeline", client=None) -> int:
        if client is None:
            import boto3

            client = boto3.client("cloudwatch")
        metric_data = self.cloudwatch_metric_data()
        if metric_data:
            client.put_metric_data(Namespace=namespace, MetricData=metric_data)
        return len(metric_data)
