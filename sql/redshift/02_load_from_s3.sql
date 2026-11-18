-- Full refresh of the star schema from the curated zone in S3.
-- Replace :bucket and :role_arn (deploy_aws.sh prints both), or run through the
-- Redshift Data API as shown in the README.

BEGIN;

TRUNCATE delivery.fact_delivery_attempt;
TRUNCATE delivery.dim_date;
TRUNCATE delivery.dim_dsp;
TRUNCATE delivery.dim_station;
TRUNCATE delivery.dim_driver;

COPY delivery.dim_date    FROM 's3://:bucket/curated/dim_date/'    IAM_ROLE ':role_arn' FORMAT AS PARQUET;
COPY delivery.dim_dsp     FROM 's3://:bucket/curated/dim_dsp/'     IAM_ROLE ':role_arn' FORMAT AS PARQUET;
COPY delivery.dim_station FROM 's3://:bucket/curated/dim_station/' IAM_ROLE ':role_arn' FORMAT AS PARQUET;
COPY delivery.dim_driver  FROM 's3://:bucket/curated/dim_driver/'  IAM_ROLE ':role_arn' FORMAT AS PARQUET;
-- The fact table is Hive-partitioned by delivery_date; COPY reads every partition folder.
COPY delivery.fact_delivery_attempt
FROM 's3://:bucket/curated/fact_delivery_attempt/'
IAM_ROLE ':role_arn'
FORMAT AS PARQUET;

COMMIT;

ANALYZE delivery.fact_delivery_attempt;

-- Post-load reconciliation: every fact row must join to all four dimensions.
SELECT COUNT(*) AS orphan_fact_rows
FROM delivery.fact_delivery_attempt f
LEFT JOIN delivery.dim_dsp d     ON f.dsp_key = d.dsp_key
LEFT JOIN delivery.dim_station s ON f.station_key = s.station_key
LEFT JOIN delivery.dim_driver r  ON f.driver_key = r.driver_key
LEFT JOIN delivery.dim_date t    ON f.date_key = t.date_key
WHERE d.dsp_key IS NULL OR s.station_key IS NULL OR r.driver_key IS NULL OR t.date_key IS NULL;
