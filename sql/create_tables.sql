
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'mobility_dw')
BEGIN
    CREATE DATABASE mobility_dw;
END;
GO

USE mobility_dw;
GO

IF OBJECT_ID('fact_mobility', 'U') IS NOT NULL
    DROP TABLE fact_mobility;
GO

CREATE TABLE fact_mobility (
    city VARCHAR(50) NOT NULL,
    [timestamp] DATETIME2 NOT NULL,

    temp_c FLOAT NULL,
    humidity INT NULL,
    wind_speed FLOAT NULL,
    precip_mm FLOAT NULL,
    weather VARCHAR(50) NULL,

    congestion_index FLOAT NULL,
    is_rainy INT NULL,
    is_humid INT NULL,
    is_windy INT NULL,

    mobility_stress_score INT NULL
);
GO

PRINT 'fact_mobility table created successfully';
