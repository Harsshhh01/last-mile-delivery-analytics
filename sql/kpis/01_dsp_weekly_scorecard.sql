-- Weekly DSP scorecard: volume, on-time delivery rate and first-attempt delivery success.
-- Runs unchanged on Amazon Redshift and DuckDB.
SELECT
    d.dsp_code,
    d.dsp_name,
    CAST(DATE_TRUNC('week', dt.calendar_date) AS DATE) AS week_start,
    COUNT(*) AS attempts,
    COUNT(DISTINCT f.package_id) AS packages,
    SUM(CASE WHEN f.status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered,
    ROUND(100.0 * SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.status = 'DELIVERED' THEN 1 ELSE 0 END), 0), 2) AS on_time_pct,
    ROUND(100.0 * SUM(CASE WHEN f.is_first_attempt_delivery THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN f.attempt_number = 1 THEN 1 ELSE 0 END), 0), 2) AS first_attempt_success_pct
FROM fact_delivery_attempt f
JOIN dim_dsp d ON f.dsp_key = d.dsp_key
JOIN dim_date dt ON f.date_key = dt.date_key
GROUP BY d.dsp_code, d.dsp_name, CAST(DATE_TRUNC('week', dt.calendar_date) AS DATE)
ORDER BY week_start, on_time_pct DESC;
