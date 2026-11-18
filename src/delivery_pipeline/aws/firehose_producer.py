"""Stream delivery events into Amazon Kinesis Data Firehose (near-real-time ingestion path).

Firehose buffers the records and writes them to s3://<lake>/raw/stream/ingest_date=YYYY-MM-DD/,
where the same Glue job processes them alongside batch files.

    python -m delivery_pipeline.aws.firehose_producer --stream <name> --file events.jsonl.gz
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
from collections.abc import Iterable, Iterator

MAX_BATCH_RECORDS = 500  # Firehose PutRecordBatch limit


def _batches(records: Iterable[dict], size: int) -> Iterator[list[dict]]:
    batch: list[dict] = []
    for rec in records:
        batch.append(rec)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def send_events(records: Iterable[dict], stream_name: str, client=None, batch_size: int = MAX_BATCH_RECORDS,
                max_retries: int = 3, backoff_seconds: float = 0.5) -> dict:
    """Send records as newline-delimited JSON, retrying only the records Firehose rejected."""
    if client is None:
        import boto3

        client = boto3.client("firehose")
    sent = failed = batches = 0
    for batch in _batches(records, min(batch_size, MAX_BATCH_RECORDS)):
        batches += 1
        pending = [{"Data": (json.dumps(r) + "\n").encode("utf-8")} for r in batch]
        for attempt in range(max_retries + 1):
            response = client.put_record_batch(DeliveryStreamName=stream_name, Records=pending)
            results = response["RequestResponses"]
            retry = [rec for rec, res in zip(pending, results) if "ErrorCode" in res]
            sent += len(pending) - len(retry)
            if not retry:
                break
            pending = retry
            if attempt < max_retries:
                time.sleep(backoff_seconds * (2 ** attempt))
        else:
            failed += len(pending)
    return {"sent": sent, "failed": failed, "batches": batches}


def read_jsonl(path: str) -> Iterator[dict]:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stream", required=True)
    p.add_argument("--file", required=True)
    p.add_argument("--limit", type=int, default=None)
    a = p.parse_args(argv)
    records = read_jsonl(a.file)
    if a.limit:
        records = (r for i, r in enumerate(records) if i < a.limit)
    print(send_events(records, a.stream))


if __name__ == "__main__":
    main()
