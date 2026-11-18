"""Pipeline configuration: data-lake zones, run date and quality thresholds."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# Data-lake layout (same prefixes locally and in S3):
#   landing/     files as delivered by upstream systems (S3 event triggers ingestion)
#   raw/         immutable copies, partitioned by source and ingest_date
#   processed/   cleaned, validated, PII-masked events (silver)
#   quarantine/  records / files that failed data-quality checks
#   curated/     star schema loaded into the warehouse (gold)
#   reference/   DSP and station master data
ZONES = ("landing", "raw", "processed", "quarantine", "curated", "reference", "quality_reports")


@dataclass(frozen=True)
class PipelineConfig:
    lake_root: str
    run_date: str  # YYYY-MM-DD ingest date being processed
    min_dq_pass_rate: float = 0.95
    pii_salt: str = field(default_factory=lambda: os.environ.get("PII_SALT", "local-dev-salt"))

    def zone(self, name: str) -> str:
        if name not in ZONES:
            raise ValueError(f"unknown zone {name!r}; expected one of {ZONES}")
        return f"{self.lake_root.rstrip('/')}/{name}"

    @property
    def raw_glob(self) -> str:
        """All raw sources (batch files and Firehose stream) for this ingest date."""
        return f"{self.zone('raw')}/*/ingest_date={self.run_date}"

    @property
    def processed_events(self) -> str:
        return f"{self.zone('processed')}/delivery_events"

    @property
    def quarantine_events(self) -> str:
        return f"{self.zone('quarantine')}/delivery_events"
