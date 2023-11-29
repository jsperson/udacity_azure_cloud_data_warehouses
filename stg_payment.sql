IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 STRING_DELIMITER = '"',
			 FIELD_TERMINATOR = ',',
			 USE_TYPE_DEFAULT = FALSE,
			 FIRST_ROW = 2
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'import_sajspudacity_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [import_sajspudacity_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://import@sajspudacity.dfs.core.windows.net' 
	)
GO

IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'stg_payment')
	DROP EXTERNAL TABLE dbo.stg_payment
GO

CREATE EXTERNAL TABLE dbo.stg_payment (
	[payment_id] VARCHAR(4000),
	[date] VARCHAR(4000),
	[amount] VARCHAR(4000),
	[rider_id] VARCHAR(4000)
	)
	WITH (
	LOCATION = 'publicpayment.txt',
	DATA_SOURCE = [import_sajspudacity_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM dbo.stg_payment
GO
