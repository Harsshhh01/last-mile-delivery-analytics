"""Tiny helpers that work for both local paths and s3:// URIs."""

from __future__ import annotations

from pathlib import Path


def _split_s3(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def write_text(path: str, text: str) -> None:
    if path.startswith("s3://"):
        import boto3

        bucket, key = _split_s3(path)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"),
                                      ContentType="application/json")
    else:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
