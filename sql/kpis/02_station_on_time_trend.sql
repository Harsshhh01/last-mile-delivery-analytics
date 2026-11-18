-- Daily on-time delivery rate per station with a 7-day moving average (window function).
WITH daily AS (
    SELECT
        s.station_code,
        s.city,
        dt.calendar_date,
        COUNT(*) AS delivered,
        AVG(CASE WHEN f.is_on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate
    FROM fact_delivery_attempt f
    JOIN dim_station s ON f.station_key = s.station_key
    JOIN dim_date dt ON f.date_key = dt.date_key
    WHERE f.status = 'DELIVERED'
    GROUP BY s.station_code, s.city, dt.calendar_date
)
SELECT
    station_code,
    city,
    calendar_date,
    delivered,
    ROUND(100 * on_time_rate, 2) AS on_time_pct,
    ROUND(100 * AVG(on_time_rate) OVER (
        PARTITION BY station_code
        ORDER BY calendar_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS on_time_pct_7d_avg
FROM daily
ORDER BY station_code, calendar_date;
