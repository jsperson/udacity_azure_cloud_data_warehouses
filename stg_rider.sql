IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'stg_rider')
	DROP EXTERNAL TABLE dbo.stg_rider
GO

CREATE EXTERNAL TABLE dbo.stg_rider (
	[rider_id] VARCHAR(4000),
	[first] VARCHAR(4000),
	[last] VARCHAR(4000),
	[address] VARCHAR(4000),
	[birthday] VARCHAR(4000),
	[account_start_date] VARCHAR(4000),
	[account_end_date] VARCHAR(4000),
	[is_member] VARCHAR(4000)
	)
	WITH (
	LOCATION = 'publicrider.txt',
	DATA_SOURCE = [import_sajspudacity_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM dbo.stg_rider
GO
