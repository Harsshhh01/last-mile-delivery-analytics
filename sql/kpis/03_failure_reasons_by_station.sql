-- Why deliveries fail: share and rank of each failure reason per station.
WITH reasons AS (
    SELECT
        s.station_code,
        f.failure_reason,
        COUNT(*) AS failed_attempts
    FROM fact_delivery_attempt f
    JOIN dim_station s ON f.station_key = s.station_key
    WHERE f.status = 'FAILED'
    GROUP BY s.station_code, f.failure_reason
)
SELECT
    station_code,
    failure_reason,
    failed_attempts,
    ROUND(100.0 * failed_attempts / SUM(failed_attempts) OVER (PARTITION BY station_code), 2) AS share_pct,
    RANK() OVER (PARTITION BY station_code ORDER BY failed_attempts DESC) AS reason_rank
FROM reasons
ORDER BY station_code, reason_rank;
