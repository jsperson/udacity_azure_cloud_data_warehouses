IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'stg_trip')
	DROP EXTERNAL TABLE dbo.stg_trip
GO

CREATE EXTERNAL TABLE dbo.stg_trip (
	[trip_id] VARCHAR(4000),
    [rideable_type] VARCHAR(4000),
    [start_at] VARCHAR(4000),
    [ended_at] VARCHAR(4000),
    [start_station_id] VARCHAR(4000),
    [end_station_id] VARCHAR(4000),
    [rider_id] VARCHAR(4000)
	)
	WITH (
	LOCATION = 'publictrip.txt',
	DATA_SOURCE = [import_sajspudacity_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM dbo.stg_trip
GO

--SELECT ROW_NUMBER() OVER(ORDER BY trip_id) AS rownum, trip_id FROM dbo.stg_trip
