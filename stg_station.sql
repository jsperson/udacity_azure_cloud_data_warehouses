IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'stg_station')
	DROP EXTERNAL TABLE dbo.stg_station
GO

CREATE EXTERNAL TABLE dbo.stg_station (
	[station_id] VARCHAR(4000),
	[name] VARCHAR(4000),
	[latitude] VARCHAR(4000),
	[longitude] VARCHAR(4000)
	)
	WITH (
	LOCATION = 'publicstation.txt',
	DATA_SOURCE = [import_sajspudacity_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM dbo.stg_station
GO
