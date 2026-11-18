import gzip
import json

import pytest

from delivery_pipeline.ingest import (
    REQUIRED_FIELDS,
    IngestError,
    ingest_local,
    raw_key_for,
    rejected_key_for,
    validate_head,
)

GOOD = {f: "x" for f in REQUIRED_FIELDS}


def _gz(record) -> bytes:
    return gzip.compress((json.dumps(record) + "\n").encode())


def test_valid_gzip_head_passes():
    validate_head("batch.jsonl.gz", _gz(GOOD))


@pytest.mark.parametrize(
    "name, data, message",
    [
        ("batch.csv", b"a,b\n", "unsupported file type"),
        ("batch.jsonl", b"", "empty"),
        ("batch.jsonl", b"{not json\n", "not valid JSON"),
        ("batch.jsonl", b"[1, 2]\n", "not a JSON object"),
        ("batch.jsonl.gz", b"plain text, not gzip", "not valid gzip"),
    ],
)
def test_bad_files_are_rejected(name, data, message):
    with pytest.raises(IngestError, match=message):
        validate_head(name, data)


def test_missing_fields_are_listed():
    record = dict(GOOD)
    del record["event_ts"], record["dsp_code"]
    with pytest.raises(IngestError, match="dsp_code, event_ts"):
        validate_head("b.jsonl.gz", _gz(record))


def test_key_layout():
    assert raw_key_for("landing/a.jsonl.gz", "2026-11-01") == "raw/batch/ingest_date=2026-11-01/a.jsonl.gz"
    assert rejected_key_for("landing/a.csv", "2026-11-01") == (
        "quarantine/rejected_files/ingest_date=2026-11-01/a.csv"
    )


def test_ingest_local_moves_files(tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "good.jsonl.gz").write_bytes(_gz(GOOD))
    (landing / "bad.csv").write_text("a,b")
    out = ingest_local(str(tmp_path), "2026-11-01")
    assert out["accepted"] == ["good.jsonl.gz"]
    assert out["rejected"][0]["file"] == "bad.csv"
    assert (tmp_path / "raw/batch/ingest_date=2026-11-01/good.jsonl.gz").exists()
    assert (tmp_path / "quarantine/rejected_files/ingest_date=2026-11-01/bad.csv").exists()
    assert not any(landing.iterdir())
