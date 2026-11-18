# Design decisions and trade-offs

Why the pipeline is built the way it is, and what would change at larger scale.

## 1. Data lake zones (medallion layout)

`landing → raw → processed (silver) → curated (gold)`, plus `quarantine/` and `quality_reports/`.

- **landing** is the only place upstream systems write to. An S3 event triggers the ingestion Lambda, so
  there is a single entry point to validate.
- **raw** is an immutable copy partitioned by `source` (batch / stream) and `ingest_date`. Any day can be
  re-processed from raw if a bug is found later.
- **processed** holds cleaned, validated, PII-masked events as Parquet, partitioned by `ingest_date`.
- **curated** holds the star schema that Redshift loads.

The 90-day lifecycle rule on `raw/` keeps storage costs bounded; processed data is the long-term record.

## 2. Read raw data as strings

The raw schema declares every field as `STRING` and keeps malformed lines in `_corrupt_record`
(`PERMISSIVE` mode). Type casting happens afterwards, in code we control. A single bad value therefore never
fails the job or silently drops a row; instead it becomes `NULL` and is caught by a named data-quality rule.
`spark.sql.ansi.enabled=false` and `timeParserPolicy=CORRECTED` are set explicitly, so behaviour is identical
on Glue 4.0 (Spark 3.3) and on Spark 3.5/4.x locally.

## 3. Repair what is safe, quarantine the rest

| Issue | Treatment | Why |
|---|---|---|
| lowercase codes, padded statuses | repaired | meaning is unambiguous |
| `dd/MM/yyyy HH:mm` and epoch-millis timestamps | parsed | known upstream formats |
| exact duplicate `event_id` | dropped, counted | upstream retries |
| missing IDs, unparseable timestamps, unknown DSP or station, impossible weights or attempt numbers, contradictory status/reason | **quarantined** with the list of failed rules | guessing would corrupt KPIs |

Each quarantined record carries `dq_failed_rules`, so an engineer can see *why* without re-running anything.

## 4. Circuit breaker before publishing

The batch pass rate is computed before anything is written to `processed/` or `curated/`. Below the
threshold (95% by default) the job writes the quarantine and the DQ report and then fails. CloudWatch gets
the `DQPassRate` metric and alarms, and EventBridge alerts on the failed Glue run. Consumers keep seeing
yesterday's correct numbers instead of today's broken ones. A test proves nothing is published for a bad batch.

## 5. Idempotent, re-runnable loads

- processed and quarantine are written with **dynamic partition overwrite** on `ingest_date`: re-running
  a day replaces only that day.
- gold is **rebuilt from silver** each run. Late or re-sent events are resolved with
  `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_date DESC, event_ts DESC)`, so the latest version wins.
- A test runs the ETL twice and asserts identical row counts.

*At larger scale:* rebuilding gold from all history becomes expensive. The next step is an incremental merge
into an Apache Iceberg or Delta table (`MERGE INTO ... ON event_id`) and loading only changed partitions into
Redshift.

## 6. Star schema (Kimball)

- **Grain:** one row per delivery *attempt*. Package-level metrics (first-attempt success, attempts per
  package) are derived from it, while attempt-level analysis (failure reasons, time of day) stays possible.
- **Dimensions:** date, DSP, station, driver (pseudonymised). `route_id` and `package_id` are degenerate
  dimensions on the fact.
- **Surrogate keys** are `xxhash64(natural_key)`: deterministic, so dimensions and facts built in separate runs
  always join, and the same key appears in DuckDB and Redshift.
- **Redshift physical design:** dimensions use `DISTSTYLE ALL` (small, joined everywhere); the fact table
  uses `DISTKEY(station_key)` for co-located station analysis and `COMPOUND SORTKEY(date_key, dsp_key)` so
  date-range queries skip blocks. Parquet column order matches the DDL because `COPY` maps by position.
- A reconciliation query after load fails if any fact row has no matching dimension row.

## 7. Privacy by design

The raw feed contains personal data (driver IDs, customer phone numbers, full postcodes).

- driver IDs become `sha2(salt || driver_id, 256)`. The salt lives in **AWS Secrets Manager**, so the
  pseudonym cannot be reversed with a lookup table, but the same driver still maps to the same key.
- phone numbers are dropped at the first transform (data minimisation).
- postcodes are generalised to the 3-digit sorting district.
- only the raw zone (90-day retention, encrypted, no public access, TLS-only) ever holds the originals.

## 8. Batch and streaming in one job

Batch files and the Firehose stream land in sibling folders (`raw/batch/`, `raw/stream/`) under the same
`ingest_date` partition, and the Glue job reads `raw/*/ingest_date=<date>`. One code path handles both, run by
the Lambda for batch arrivals and by an hourly trigger for stream data. `MaxConcurrentRuns: 1` prevents
overlapping runs; the Lambda treats "already running" as normal rather than an error.

## 9. Least-privilege IAM

Each service has its own role, limited to the prefixes it touches: the Lambda can read and delete only
`landing/*` and write only `raw/*` and `quarantine/*`; Firehose can write only `raw/stream/*`; Redshift can
read only `curated/*`; and Glue may publish metrics only to the `DeliveryPipeline` namespace.

## 10. GenAI agent: trust, but verify

LLM-generated SQL and LLM-written answers are treated as untrusted input:

1. **Schema grounding:** the prompt contains the live warehouse schema and a business glossary
   (what "on-time" and "first-attempt success" mean), not a hand-written copy that can drift.
2. **AST guardrails (sqlglot):** exactly one statement; SELECT/CTE only; no DDL/DML; no file-reading
   functions (`read_csv`, `read_parquet`, ...); only warehouse tables; `LIMIT` enforced.
3. **Read-only connection** as a second layer of defence.
4. **Self-correction:** a rejected or failing query is sent back with the error for one retry.
5. **Result sanity checks:** empty results and percentages outside 0–100 raise warnings.
6. **Answer grounding:** every number in the LLM's answer must appear in the result rows (allowing for
   rounding); otherwise the answer is flagged as possibly hallucinated.

Amazon Bedrock (Converse API) is the default provider on AWS; the Anthropic API is supported for local use.

## 11. Testing strategy

| Layer | How |
|---|---|
| Pure logic (ingest rules, SQL guardrails, grounding check) | plain pytest |
| Spark transforms and every DQ rule | local Spark session, tiny hand-built DataFrames |
| Whole ETL, idempotency, circuit breaker, PII absence | generated dirty batch through `run_etl` |
| Star schema, KPIs | DuckDB: unique keys, continuous dates, fact grain, KPI ranges; KPI and Redshift SQL parsed with the Redshift dialect |
| Lambda, Firehose, CloudWatch | **moto**-mocked AWS plus fakes for throttling and concurrent runs |
| Infrastructure | cfn-lint plus assertions on security settings |

## 12. What I would add next

- Apache Iceberg tables in the Glue Data Catalog with incremental `MERGE` (see section 5).
- Step Functions to orchestrate Glue → Redshift load → KPI refresh with retries.
- Great Expectations / AWS Glue Data Quality for declarative rules and data docs.
- Slowly changing dimension (Type 2) for DSP attributes such as fleet size.
- QuickSight dashboard on the KPI views.
