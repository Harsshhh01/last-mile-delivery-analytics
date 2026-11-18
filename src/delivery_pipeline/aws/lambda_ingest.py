"""AWS Lambda: S3 landing/ upload -> validate -> raw/ (or quarantine) -> start the Glue job.

Handler: delivery_pipeline.aws.lambda_ingest.handler
Environment: GLUE_JOB_NAME
"""

from __future__ import annotations

import json
import logging
import os
import urllib.parse

import boto3

from ..ingest import HEAD_BYTES, IngestError, raw_key_for, rejected_key_for, today_ist, validate_head

log = logging.getLogger()
log.setLevel(logging.INFO)


def _move(s3, bucket: str, src: str, dest: str) -> None:
    s3.copy_object(Bucket=bucket, Key=dest, CopySource={"Bucket": bucket, "Key": src},
                   ServerSideEncryption="AES256")
    s3.delete_object(Bucket=bucket, Key=src)


def handler(event, context=None, s3=None, glue=None) -> dict:
    s3 = s3 or boto3.client("s3")
    glue = glue or boto3.client("glue")
    job_name = os.environ.get("GLUE_JOB_NAME")
    ingest_date = today_ist()
    accepted, rejected = [], []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        try:
            head = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{HEAD_BYTES - 1}")["Body"].read()
            validate_head(key, head)
            dest = raw_key_for(key, ingest_date)
            _move(s3, bucket, key, dest)
            accepted.append(dest)
        except IngestError as exc:
            dest = rejected_key_for(key, ingest_date)
            _move(s3, bucket, key, dest)
            rejected.append({"key": key, "reason": str(exc)})
            log.warning(json.dumps({"event": "file_rejected", "key": key, "reason": str(exc)}))

    run_id = None
    if accepted and job_name:
        try:
            run_id = glue.start_job_run(JobName=job_name, Arguments={"--run_date": ingest_date})["JobRunId"]
        except glue.exceptions.ConcurrentRunsExceededException:
            # A run is already in progress; the hourly Glue trigger will pick these files up.
            log.info(json.dumps({"event": "glue_run_already_active", "job": job_name}))

    result = {
        "ingest_date": ingest_date,
        "accepted": accepted,
        "rejected": rejected,
        "glue_job_run_id": run_id,
    }
    log.info(json.dumps(result))
    return result
