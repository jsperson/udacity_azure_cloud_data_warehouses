IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 USE_TYPE_DEFAULT = FALSE,
			 FIRST_ROW = 2
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET
            )
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'datamart_external_source') 
	CREATE EXTERNAL DATA SOURCE [datamart_external_source] 
	WITH (
		LOCATION = 'abfss://datamart@sajspudacity.dfs.core.windows.net' 
	)
GO

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_station')
	DROP EXTERNAL TABLE dbo.dim_station
GO

CREATE EXTERNAL TABLE dbo.dim_station
WITH (
    LOCATION     = 'dim_station',
    DATA_SOURCE = [datamart_external_source],
    FILE_FORMAT = [SynapseParquetFormat]
)  
AS
SELECT --TOP 100 
    ROW_NUMBER() OVER(ORDER BY station_id) as dim_station_id,
    LEFT(station_id,250) AS station_id,
    LEFT(name,250) AS station_name,
    CAST(latitude AS FLOAT) AS latitude,
    CAST(longitude AS FLOAT) AS longitude
FROM sourcedata.dbo.stg_station;
GO
--select top 100 * from dbo.dim_station

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_rider')
	DROP EXTERNAL TABLE dbo.dim_rider
GO

CREATE EXTERNAL TABLE dbo.dim_rider
WITH (
    LOCATION     = 'dim_rider',
    DATA_SOURCE = [datamart_external_source],
    FILE_FORMAT = [SynapseParquetFormat]
)  
AS
SELECT
    ROW_NUMBER() OVER(ORDER BY rider_id) as dim_rider_id,
    CONVERT(INT,rider_id) AS rider_id,
    LEFT(first,250) AS rider_first_name,
    LEFT(last,250) AS rider_last_name,
    address,
    CAST(birthday AS DATE) AS birthday,
    CONVERT(INT,FORMAT (CAST(birthday AS DATE), 'yyyyMMdd')) AS birthday_dim_date_id,
    DATEDIFF(year, CAST(birthday AS DATE), CAST(account_start_date AS DATE)) AS rider_age_at_account_start,
    CAST(account_start_date AS DATE) AS account_start_date,
    CONVERT(INT,FORMAT (CAST(account_start_date AS DATE), 'yyyyMMdd')) AS account_start_dim_date_id,
    CAST(account_end_date AS DATE) AS account_end_date,
    COALESCE(CONVERT(INT,FORMAT (CAST(account_end_date AS DATE), 'yyyyMMdd')),99990101) AS account_end_dim_date_id,
    CASE WHEN is_member = 'True' THEN 'Y' ELSE 'N' END AS member_flag
FROM sourcedata.dbo.stg_rider;
GO

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'fact_trip')
	DROP EXTERNAL TABLE dbo.fact_trip
GO

CREATE EXTERNAL TABLE dbo.fact_trip
WITH (
    LOCATION     = 'fact_trip',
    DATA_SOURCE = [datamart_external_source],
    FILE_FORMAT = [SynapseParquetFormat]
)  
AS
SELECT
    ROW_NUMBER() OVER(ORDER BY t.trip_id) as fact_trip_id,
    t.trip_id,
    r.dim_rider_id,
    COALESCE(CONVERT(INT,FORMAT (CAST(LEFT(t.start_at,19) AS DATE), 'yyyyMMdd')),99990101) AS start_dim_date_id,
    COALESCE(CONVERT(INT,FORMAT (CAST(LEFT(t.ended_at,19) AS DATE), 'yyyyMMdd')),99990101) AS end_dim_date_id,
    CAST(LEFT(t.start_at,19) AS DATETIME) AS start_datetime,
    CAST(LEFT(t.ended_at,19) AS DATETIME) AS end_datetime,
    DATEDIFF(year,CAST(r.birthday AS DATE),getdate()) AS rider_age,
    ss.dim_station_id AS start_dim_station_id,
    es.dim_station_id AS end_dim_station_id,
    t.rideable_type, --degenerate dimension
    DATEDIFF(second,CAST(LEFT(t.start_at,19) AS DATETIME),CAST(LEFT(t.ended_at,19) AS DATETIME)) AS ride_duration,
    1 AS ride_count
FROM sourcedata.dbo.stg_trip t
    INNER JOIN datamart.dbo.dim_rider r
    ON t.rider_id = r.rider_id
    INNER JOIN datamart.dbo.dim_station ss
    ON t.start_station_id = ss.station_id
    INNER JOIN datamart.dbo.dim_station es
    ON t.end_station_id = es.station_id;
GO

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'fact_payment')
	DROP EXTERNAL TABLE dbo.fact_payment
GO

CREATE EXTERNAL TABLE dbo.fact_payment
WITH (
    LOCATION     = 'fact_payment',
    DATA_SOURCE = [datamart_external_source],
    FILE_FORMAT = [SynapseParquetFormat]
)  
AS
SELECT
    ROW_NUMBER() OVER(ORDER BY p.payment_id) AS fact_payment_id,
    CAST(payment_id AS INT) AS payment_id,
    r.dim_rider_id,
    COALESCE(CONVERT(INT,FORMAT (CAST(LEFT(p.date,19) AS DATE), 'yyyyMMdd')),99990101) AS payment_dim_date_id,
    1 AS payment_count
FROM sourcedata.dbo.stg_payment p
    INNER JOIN datamart.dbo.dim_rider r
    ON p.rider_id = r.rider_id;
GO

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_date')
	DROP EXTERNAL TABLE dbo.dim_date
GO

-- Note to the reviewer: this is not how I like creating date dimensions
-- With the serverless pool, I'm somewhat limited. Recursion doesn't work
-- beyond 100 so my usual method of creating a dimension didn't work out
-- this is pretty kludgy, but the other query methods that I know for 
-- creating numbers without recursion are downright janky. This at least
-- works for the data provided. At work I have a pretty ambitious set of 
-- TSQL that constructs a nice date dimension. Won't work on serverless...
CREATE EXTERNAL TABLE dbo.dim_date
WITH (
    LOCATION     = 'dim_date',
    DATA_SOURCE = [datamart_external_source],
    FILE_FORMAT = [SynapseParquetFormat]
)  
AS
WITH dates_cte AS (
    SELECT start_dim_date_id AS dim_date_id
    FROM datamart.dbo.fact_trip
    UNION
    SELECT end_dim_date_id AS dim_date_id
    FROM datamart.dbo.fact_trip
    UNION
    SELECT birthday_dim_date_id AS dim_date_id
    FROM datamart.dbo.dim_rider
    UNION
    SELECT account_start_dim_date_id AS dime_date_id
    FROM datamart.dbo.dim_rider
    UNION
    SELECT account_end_dim_date_id AS dime_date_id
    FROM datamart.dbo.dim_rider
    UNION
    SELECT payment_dim_date_id AS dim_date_id
    FROM datamart.dbo.fact_payment
)
SELECT
    dim_date_id,
    CONVERT(DATE, CAST(dim_date_id AS CHAR), 112) AS date,
    CAST(SUBSTRING(CAST(dim_date_id AS CHAR),5,2) AS INT) AS month,
    CAST(SUBSTRING(CAST(dim_date_id AS CHAR),1,4) AS INT) AS year,
    DATEPART(QUARTER,CONVERT(DATE, CAST(dim_date_id AS CHAR), 112)) AS quarter
FROM dates_cte;
GO
