-- Driver productivity per DSP: attempts and deliveries per driver-day (drivers are pseudonymised).
WITH driver_day AS (
    SELECT
        f.driver_key,
        f.dsp_key,
        f.date_key,
        COUNT(*) AS attempts,
        SUM(CASE WHEN f.status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered
    FROM fact_delivery_attempt f
    GROUP BY f.driver_key, f.dsp_key, f.date_key
)
SELECT
    d.dsp_code,
    COUNT(DISTINCT dd.driver_key) AS active_drivers,
    ROUND(AVG(dd.attempts), 1) AS avg_attempts_per_driver_day,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dd.delivered) AS median_delivered_per_driver_day
FROM driver_day dd
JOIN dim_dsp d ON dd.dsp_key = d.dsp_key
GROUP BY d.dsp_code
ORDER BY median_delivered_per_driver_day DESC;
