"""Synthetic last-mile delivery events with realistic dirty data.

Each output row is one delivery *attempt*. Packages that fail are re-attempted on the
following day, up to three attempts. A small, controlled share of rows is corrupted
(bad timestamps, unknown partners, duplicates, malformed JSON, ...) so the cleaning
and data-quality stages have real work to do.

Usage:
    python -m delivery_pipeline.generate_data --lake ./lake --packages 1000000
"""

from __future__ import annotations

import argparse
import gzip
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .reference_data import (
    DSPS,
    FAILURE_REASON_WEIGHTS,
    FAILURE_REASONS,
    SERVICE_TYPE_WEIGHTS,
    SERVICE_TYPES,
    STATIONS,
    write_reference_csvs,
)

PINCODE = {code: prefix for code, _, _, _, prefix in STATIONS}
COLUMNS = [
    "event_id", "package_id", "route_id", "station_code", "dsp_code", "driver_id",
    "customer_phone", "delivery_postcode", "service_type", "stop_sequence", "attempt_number",
    "status", "failure_reason", "package_weight_kg", "promised_by", "event_ts",
]


def _iso(ts: pd.Series) -> pd.Series:
    return ts.dt.strftime("%Y-%m-%dT%H:%M:%S") + "+05:30"


def generate_events(n_packages: int, start: date, days: int, seed: int = 42) -> pd.DataFrame:
    """Return clean delivery-attempt events (one row per attempt), timestamps in IST."""
    rng = np.random.default_rng(seed)
    n = n_packages

    dsp_idx = rng.integers(0, len(DSPS), n)
    station = np.empty(n, dtype=object)
    driver_num = np.empty(n, dtype=np.int64)
    p_first = np.empty(n)
    late_mean = np.empty(n)
    for i, (_, _, _, fleet, stations, p_ok, lateness) in enumerate(DSPS):
        mask = dsp_idx == i
        k = int(mask.sum())
        station[mask] = np.array(stations, dtype=object)[rng.integers(0, len(stations), k)]
        driver_num[mask] = rng.integers(1, fleet + 1, k)
        p_first[mask] = p_ok
        late_mean[mask] = lateness
    dsp_code = np.array([d[0] for d in DSPS], dtype=object)[dsp_idx]

    service = rng.choice(SERVICE_TYPES, size=n, p=SERVICE_TYPE_WEIGHTS)
    promise_hour = np.where(service == "SAME_DAY", 18, 21)
    day0 = pd.Timestamp(start) + pd.to_timedelta(rng.integers(0, days, n), unit="D")
    promised_by = day0 + pd.to_timedelta(promise_hour, unit="h")

    phones = rng.integers(6_000_000_000, 9_999_999_999, n)
    pin_suffix = rng.integers(0, 1000, n)
    packages = pd.DataFrame({
        "package_id": [f"PKG{seed % 97:02d}{i:09d}" for i in range(n)],
        "station_code": station,
        "dsp_code": dsp_code,
        "driver_id": [f"{c[4:]}-DRV-{k:04d}" for c, k in zip(dsp_code, driver_num)],
        "customer_phone": [f"+91{p}" for p in phones],
        "delivery_postcode": [f"{PINCODE[s]}{x:03d}" for s, x in zip(station, pin_suffix)],
        "service_type": service,
        "stop_sequence": rng.integers(1, 201, n),
        "package_weight_kg": np.round(np.clip(rng.gamma(2.0, 1.2, n), 0.05, 30.0), 2),
        "promised_by": promised_by,
        "day0": day0,
        "late_mean": late_mean,
    })

    # Attempt outcomes: first attempt depends on the DSP, retries get progressively harder.
    ok1 = rng.random(n) < p_first
    ok2 = rng.random(n) < 0.80
    ok3 = rng.random(n) < 0.70
    attempts = []
    for attempt, include, success in (
        (1, np.ones(n, dtype=bool), ok1),
        (2, ~ok1, ok2),
        (3, ~ok1 & ~ok2, ok3),
    ):
        a = packages[include].copy()
        succ = success[include]
        m = len(a)
        attempt_day = a["day0"] + pd.Timedelta(days=attempt - 1)
        a["route_id"] = [
            f"RT-{s}-{d}-{r:03d}"
            for s, d, r in zip(a["station_code"], attempt_day.dt.strftime("%Y%m%d"), rng.integers(1, 61, m))
        ]
        a["attempt_number"] = attempt
        a["status"] = np.where(succ, "DELIVERED", "FAILED")
        a["failure_reason"] = np.where(
            succ, None, rng.choice(FAILURE_REASONS, size=m, p=FAILURE_REASON_WEIGHTS)
        )
        promised_on_day = attempt_day + (a["promised_by"] - a["day0"])
        delivered_offset = rng.normal(a["late_mean"].values, 70.0)
        failed_offset = -rng.uniform(30, 600, m)
        offset_min = np.where(succ, delivered_offset, failed_offset)
        event_ts = promised_on_day + pd.to_timedelta(offset_min, unit="m").to_numpy()
        earliest = attempt_day + pd.Timedelta(hours=8)
        morning = earliest + pd.to_timedelta(rng.uniform(0, 90, m), unit="m").to_numpy()
        a["event_ts"] = event_ts.where(event_ts >= earliest, morning)
        attempts.append(a)

    events = pd.concat(attempts, ignore_index=True)
    ids = rng.integers(0, 2**63 - 1, size=(len(events), 2), dtype=np.int64)
    events["event_id"] = [f"{a:016x}{b:016x}" for a, b in ids]
    events["promised_by"] = _iso(events["promised_by"])
    events["event_ts"] = _iso(events["event_ts"].dt.floor("s"))
    return events[COLUMNS].sample(frac=1.0, random_state=seed).reset_index(drop=True)


def inject_dirty_data(events: pd.DataFrame, seed: int = 42) -> tuple[pd.DataFrame, dict[str, int]]:
    """Corrupt a controlled share of rows. Returns the dirty frame and what was injected."""
    rng = np.random.default_rng(seed + 1)
    df = events.astype({
        "stop_sequence": object, "attempt_number": object, "package_weight_kg": object,
        "status": object, "station_code": object, "event_ts": object, "package_id": object,
        "dsp_code": object, "failure_reason": object,
    })
    n = len(df)
    pool = rng.permutation(n)  # disjoint row sets, so each corruption is counted exactly once
    cursor = 0

    def pick(share: float) -> np.ndarray:
        nonlocal cursor
        k = max(1, int(n * share))
        chosen = pool[cursor:cursor + k]
        cursor += k
        return chosen

    injected: dict[str, int] = {}
    # Cleanable issues: the transform stage should repair these.
    idx = pick(0.02)
    df.loc[idx, "status"] = df.loc[idx, "status"].str.lower().radd(" ") + " "
    injected["status_case_whitespace"] = len(idx)
    idx = pick(0.03)
    df.loc[idx, "station_code"] = df.loc[idx, "station_code"].str.lower()
    injected["station_code_lowercase"] = len(idx)
    idx = pick(0.02)
    ts = pd.to_datetime(df.loc[idx, "event_ts"].str[:19])
    df.loc[idx, "event_ts"] = ts.dt.strftime("%d/%m/%Y %H:%M")
    injected["event_ts_ddmmyyyy"] = len(idx)
    idx = pick(0.01)
    ts = pd.to_datetime(df.loc[idx, "event_ts"], format="ISO8601", utc=True)
    millis = (ts - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(milliseconds=1)
    df.loc[idx, "event_ts"] = millis.astype("int64").astype(str).to_numpy(dtype=object)
    injected["event_ts_epoch_millis"] = len(idx)

    # Invalid records: data-quality checks must quarantine these.
    idx = pick(0.0025)
    df.loc[idx, "event_ts"] = "N/A"
    injected["event_ts_invalid"] = len(idx)
    idx = pick(0.002)
    df.loc[idx, "package_id"] = None
    injected["package_id_missing"] = len(idx)
    idx = pick(0.002)
    df.loc[idx, "package_weight_kg"] = rng.choice([-1.0, 999.0], size=len(idx))
    injected["weight_out_of_range"] = len(idx)
    idx = pick(0.0015)
    df.loc[idx, "dsp_code"] = "DSP-UNKNOWN"
    injected["dsp_code_unknown"] = len(idx)
    idx = pick(0.0015)
    df.loc[idx, "attempt_number"] = rng.choice([0, 9], size=len(idx))
    injected["attempt_number_out_of_range"] = len(idx)
    remaining = pool[cursor:]
    is_delivered = (df["status"].astype(str).str.strip().str.upper() == "DELIVERED").to_numpy()
    idx = remaining[is_delivered[remaining]][: max(1, int(n * 0.0015))]
    df.loc[idx, "failure_reason"] = "CUSTOMER_UNAVAILABLE"
    injected["delivered_with_failure_reason"] = len(idx)

    # Exact duplicates (e.g. an upstream retry): the dedupe step must drop them.
    dup_idx = pick(0.01)
    df = pd.concat([df, df.loc[dup_idx]], ignore_index=True)
    injected["exact_duplicates"] = len(dup_idx)
    df = df.sample(frac=1.0, random_state=seed + 2).reset_index(drop=True)
    return df, injected


def write_jsonl_gz(df: pd.DataFrame, out_dir: Path, prefix: str, rows_per_file: int = 250_000,
                   malformed_share: float = 0.0003, seed: int = 42) -> list[Path]:
    """Write gzipped JSON Lines files; a few lines are truncated to simulate malformed JSON."""
    rng = np.random.default_rng(seed + 3)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for part, start in enumerate(range(0, len(df), rows_per_file)):
        chunk = df.iloc[start:start + rows_per_file]
        lines = chunk.to_json(orient="records", lines=True, force_ascii=False).splitlines()
        bad = rng.choice(len(lines), size=int(len(lines) * malformed_share), replace=False)
        for i in bad:
            lines[i] = lines[i][: len(lines[i]) // 2]
        path = out_dir / f"{prefix}_part-{part:03d}.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        paths.append(path)
    return paths


def generate_to_lake(lake_root: str, n_packages: int, start: date, days: int, run_date: str,
                     seed: int = 42) -> dict:
    """Write reference CSVs and a landing batch of dirty events into the lake."""
    root = Path(lake_root)
    write_reference_csvs(root / "reference")
    clean = generate_events(n_packages, start, days, seed)
    dirty, injected = inject_dirty_data(clean, seed)
    files = write_jsonl_gz(dirty, root / "landing", f"delivery_events_{run_date}", seed=seed)
    injected["malformed_json_lines"] = sum(
        int(min(250_000, len(dirty) - start) * 0.0003) for start in range(0, len(dirty), 250_000)
    )
    return {
        "packages": n_packages,
        "clean_events": len(clean),
        "events_written": len(dirty),
        "files": [str(p) for p in files],
        "injected": injected,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lake", default="lake")
    p.add_argument("--packages", type=int, default=100_000)
    p.add_argument("--start-date", default=(date.today() - timedelta(days=30)).isoformat())
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--run-date", default=date.today().isoformat())
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    info = generate_to_lake(a.lake, a.packages, date.fromisoformat(a.start_date), a.days, a.run_date, a.seed)
    print(f"wrote {info['events_written']:,} events in {len(info['files'])} files to {a.lake}/landing")


if __name__ == "__main__":
    main()
