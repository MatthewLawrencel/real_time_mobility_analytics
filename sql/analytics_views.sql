USE mobility_dw;
GO

-- ============================================
-- View 1: City-Level KPI Summary
-- ============================================
IF OBJECT_ID('vw_city_kpis', 'V') IS NOT NULL
    DROP VIEW vw_city_kpis;
GO

CREATE VIEW vw_city_kpis AS
SELECT
    city,
    COUNT(*) AS total_records,
    AVG(mobility_stress_score) AS avg_mobility_stress,
    AVG(congestion_index) AS avg_congestion_index,
    SUM(is_rainy) * 100.0 / COUNT(*) AS rainy_hours_pct
FROM fact_mobility
GROUP BY city;
GO

-- ============================================
-- View 2: Hourly Stress Pattern
-- ============================================
IF OBJECT_ID('vw_hourly_stress', 'V') IS NOT NULL
    DROP VIEW vw_hourly_stress;
GO

CREATE VIEW vw_hourly_stress AS
SELECT
    DATEPART(HOUR, [timestamp]) AS hour_of_day,
    city,
    AVG(mobility_stress_score) AS avg_stress
FROM fact_mobility
GROUP BY DATEPART(HOUR, [timestamp]), city;
GO

-- ============================================
-- View 3: Peak Stress Hours per City
-- ============================================
IF OBJECT_ID('vw_peak_stress_hour', 'V') IS NOT NULL
    DROP VIEW vw_peak_stress_hour;
GO

CREATE VIEW vw_peak_stress_hour AS
WITH hourly AS (
    SELECT
        city,
        DATEPART(HOUR, [timestamp]) AS hour_of_day,
        AVG(mobility_stress_score) AS avg_stress
    FROM fact_mobility
    GROUP BY city, DATEPART(HOUR, [timestamp])
)
SELECT h.city, h.hour_of_day, h.avg_stress
FROM hourly h
JOIN (
    SELECT city, MAX(avg_stress) AS max_stress
    FROM hourly
    GROUP BY city
) mx
ON h.city = mx.city AND h.avg_stress = mx.max_stress;
GO

PRINT 'Analytics views created successfully';

