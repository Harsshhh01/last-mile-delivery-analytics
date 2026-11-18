"""Lambda trigger, Firehose producer and CloudWatch metrics against moto-mocked AWS."""

import gzip
import json
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from delivery_pipeline.aws.firehose_producer import send_events
from delivery_pipeline.aws.lambda_ingest import handler
from delivery_pipeline.ingest import REQUIRED_FIELDS, today_ist
from delivery_pipeline.metrics import RunMetrics

ROOT = Path(__file__).resolve().parents[1]


def _s3_event(bucket, key):
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


def _keys(s3, bucket):
    return sorted(o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", []))


@mock_aws
def test_lambda_moves_valid_file_and_starts_glue(aws_env, monkeypatch):
    s3 = boto3.client("s3")
    glue = boto3.client("glue")
    s3.create_bucket(Bucket="lake")
    glue.create_job(Name="etl", Role="arn:aws:iam::123456789012:role/glue",
                    Command={"Name": "glueetl", "ScriptLocation": "s3://artifacts/job.py"})
    body = gzip.compress((json.dumps({f: "x" for f in REQUIRED_FIELDS}) + "\n").encode())
    s3.put_object(Bucket="lake", Key="landing/batch+1.jsonl.gz", Body=body)
    monkeypatch.setenv("GLUE_JOB_NAME", "etl")

    out = handler(_s3_event("lake", "landing/batch%2B1.jsonl.gz"), s3=s3, glue=glue)

    expected = f"raw/batch/ingest_date={today_ist()}/batch+1.jsonl.gz"
    assert out["accepted"] == [expected] and out["rejected"] == []
    assert out["glue_job_run_id"]
    assert _keys(s3, "lake") == [expected]
    assert s3.head_object(Bucket="lake", Key=expected)["ServerSideEncryption"] == "AES256"


class _FakeGlue:
    class _Busy(Exception):
        pass

    exceptions = SimpleNamespace(ConcurrentRunsExceededException=_Busy)

    def __init__(self, busy=False):
        self.busy, self.calls = busy, []

    def start_job_run(self, **kwargs):
        self.calls.append(kwargs)
        if self.busy:
            raise self._Busy()
        return {"JobRunId": "jr_1"}


@mock_aws
def test_lambda_quarantines_bad_file_without_starting_glue(aws_env, monkeypatch):
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="lake")
    s3.put_object(Bucket="lake", Key="landing/oops.csv", Body=b"a,b\n1,2\n")
    monkeypatch.setenv("GLUE_JOB_NAME", "etl")
    glue = _FakeGlue()
    out = handler(_s3_event("lake", "landing/oops.csv"), s3=s3, glue=glue)
    assert out["accepted"] == [] and "unsupported" in out["rejected"][0]["reason"]
    assert glue.calls == []
    assert _keys(s3, "lake") == [f"quarantine/rejected_files/ingest_date={today_ist()}/oops.csv"]


@mock_aws
def test_lambda_tolerates_a_running_glue_job(aws_env, monkeypatch):
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="lake")
    s3.put_object(Bucket="lake", Key="landing/a.jsonl",
                  Body=(json.dumps({f: "x" for f in REQUIRED_FIELDS}) + "\n").encode())
    monkeypatch.setenv("GLUE_JOB_NAME", "etl")
    out = handler(_s3_event("lake", "landing/a.jsonl"), s3=s3, glue=_FakeGlue(busy=True))
    assert out["accepted"] and out["glue_job_run_id"] is None


class _FlakyFirehose:
    """Rejects the first record of every first attempt, like a throttled stream."""

    def __init__(self):
        self.calls = []

    def put_record_batch(self, DeliveryStreamName, Records):
        self.calls.append(len(Records))
        first_try = len(self.calls) % 2 == 1
        responses = [{"RecordId": str(i)} for i in range(len(Records))]
        if first_try:
            responses[0] = {"ErrorCode": "ServiceUnavailableException", "ErrorMessage": "slow down"}
        return {"FailedPutCount": int(first_try), "RequestResponses": responses}


def test_firehose_batches_and_retries_only_failed_records():
    client = _FlakyFirehose()
    out = send_events(({"event_id": str(i)} for i in range(1200)), "stream", client=client, backoff_seconds=0)
    assert out == {"sent": 1200, "failed": 0, "batches": 3}
    assert client.calls == [500, 1, 500, 1, 200, 1]


@mock_aws
def test_firehose_producer_against_mocked_aws(aws_env):
    boto3.client("s3").create_bucket(Bucket="lake")
    firehose = boto3.client("firehose")
    firehose.create_delivery_stream(
        DeliveryStreamName="events",
        ExtendedS3DestinationConfiguration={
            "RoleARN": "arn:aws:iam::123456789012:role/firehose",
            "BucketARN": "arn:aws:s3:::lake",
            "Prefix": "raw/stream/",
        },
    )
    out = send_events(({"event_id": str(i)} for i in range(20)), "events", client=firehose)
    assert out["sent"] == 20 and out["failed"] == 0


@mock_aws
def test_metrics_publish_to_cloudwatch(aws_env):
    m = RunMetrics(pipeline="etl", run_date="2026-11-01")
    with m.stage("transform"):
        pass
    for key, value in {"raw_records": 1000, "valid_records": 980, "quarantined_records": 20,
                       "duplicates_removed": 5, "dq_pass_rate_pct": 98.0}.items():
        m.set(key, value)
    cw = boto3.client("cloudwatch")
    assert m.publish_cloudwatch(client=cw) == 6
    names = {x["MetricName"] for x in cw.list_metrics(Namespace="DeliveryPipeline")["Metrics"]}
    assert {"DQPassRate", "QuarantinedRecords", "RawRecords"} <= names


def test_cloudformation_template_is_valid_and_secure():
    template = (ROOT / "infra" / "cloudformation.yaml").read_text()
    for setting in ("SSEAlgorithm: AES256", "aws:SecureTransport", "BlockPublicAcls: true",
                    "PubliclyAccessible: false", "AWS::SecretsManager::Secret", "MaxConcurrentRuns: 1"):
        assert setting in template, setting
    cfn_lint = shutil.which("cfn-lint")
    if cfn_lint is None:
        pytest.skip("cfn-lint not installed")
    template_path = str(ROOT / "infra" / "cloudformation.yaml")
    lint = subprocess.run([cfn_lint, template_path], capture_output=True, text=True)
    assert lint.returncode == 0, lint.stdout + lint.stderr


def test_glue_entry_point_compiles():
    compile((ROOT / "jobs" / "glue_etl_job.py").read_text(), "glue_etl_job.py", "exec")
