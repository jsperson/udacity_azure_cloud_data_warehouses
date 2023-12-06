DROP EXTERNAL DATA SOURCE MyExternalDataSource;
GO
CREATE EXTERNAL DATA SOURCE MyExternalDataSource
WITH (
    LOCATION = 'abfss://datalake@jspersonadlsgen2.dfs.core.windows.net'
);

DROP EXTERNAL FILE FORMAT MyCsvFileFormat;
GO
CREATE EXTERNAL FILE FORMAT MyCsvFileFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        USE_TYPE_DEFAULT = TRUE,
        FIRST_ROW = 2
    )--,
    --DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'

);

drop external table myexternaltable;
GO
CREATE EXTERNAL TABLE MyExternalTable
(
    Id INT,
    Name NVARCHAR(50),
    Age INT
)
WITH (
    LOCATION = '/files/data.csv.sz',
    DATA_SOURCE = MyExternalDataSource,
    FILE_FORMAT = MyCsvFileFormat
);
GO

drop external table myexternaltableCSV;
GO
CREATE EXTERNAL TABLE MyExternalTableCSV
(
    Id INT,
    Name NVARCHAR(50),
    Age INT
)
WITH (
    LOCATION = '/files/data.csv',
    DATA_SOURCE = MyExternalDataSource,
    FILE_FORMAT = MyCsvFileFormat
);
GO

select * from MyExternalTable;
select * from MyExternalTableCSV;
