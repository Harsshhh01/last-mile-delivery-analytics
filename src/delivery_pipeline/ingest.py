"""Landing -> raw ingestion rules (standard library only).

Used by the AWS Lambda trigger and by the local runner, so file validation behaves the
same in both places. Files that fail validation are moved to quarantine/rejected_files.
"""

from __future__ import annotations

import json
import shutil
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))
REQUIRED_FIELDS = (
    "event_id", "package_id", "route_id", "station_code", "dsp_code", "driver_id",
    "status", "attempt_number", "event_ts", "promised_by",
)
ACCEPTED_SUFFIXES = (".jsonl", ".jsonl.gz", ".json", ".json.gz")
HEAD_BYTES = 65536


class IngestError(ValueError):
    """The landing file cannot be accepted into the raw zone."""


def today_ist() -> str:
    return datetime.now(IST).date().isoformat()


def check_file_name(name: str) -> None:
    if not name.endswith(ACCEPTED_SUFFIXES):
        raise IngestError(f"unsupported file type {name!r}; expected one of {ACCEPTED_SUFFIXES}")


def first_line(head: bytes, gzipped: bool) -> str:
    """First line of a (possibly gzipped, possibly truncated) byte prefix."""
    if gzipped:
        try:
            head = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(head)
        except zlib.error as exc:
            raise IngestError(f"file is not valid gzip: {exc}") from exc
    return head.decode("utf-8", errors="replace").split("\n", 1)[0].strip()


def validate_first_record(line: str) -> None:
    """Schema check on the first record: fail fast on a wrong feed, cheaply."""
    if not line:
        raise IngestError("file is empty")
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        raise IngestError(f"first line is not valid JSON: {exc.msg}") from exc
    if not isinstance(record, dict):
        raise IngestError("first record is not a JSON object")
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        raise IngestError(f"missing required fields: {', '.join(missing)}")


def validate_head(name: str, head: bytes) -> None:
    check_file_name(name)
    validate_first_record(first_line(head, name.endswith(".gz")))


def raw_key_for(landing_key: str, ingest_date: str, source: str = "batch") -> str:
    name = landing_key.rsplit("/", 1)[-1]
    return f"raw/{source}/ingest_date={ingest_date}/{name}"


def rejected_key_for(landing_key: str, ingest_date: str) -> str:
    name = landing_key.rsplit("/", 1)[-1]
    return f"quarantine/rejected_files/ingest_date={ingest_date}/{name}"


def ingest_local(lake_root: str, ingest_date: str) -> dict:
    """Move every file in <lake>/landing into raw (or rejected), like the Lambda does in S3."""
    root = Path(lake_root)
    accepted, rejected = [], []
    for f in sorted((root / "landing").glob("*")):
        if not f.is_file():
            continue
        try:
            with open(f, "rb") as fh:
                validate_head(f.name, fh.read(HEAD_BYTES))
            dest = root / raw_key_for(f.name, ingest_date)
            accepted.append(f.name)
        except IngestError as exc:
            dest = root / rejected_key_for(f.name, ingest_date)
            rejected.append({"file": f.name, "reason": str(exc)})
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(f), str(dest))
    return {"accepted": accepted, "rejected": rejected}
