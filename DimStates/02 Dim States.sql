USE Contoso
GO

---CREATE AN EXTERNAL TABLE
IF OBJECT_ID('Contoso.DimStates') IS NOT NULL
DROP EXTERNAL TABLE Contoso.DimStates
GO

CREATE EXTERNAL TABLE Contoso.DimStates
(
    GeoAreaKey INT,
    State VARCHAR(28),
    StateFull VARCHAR(28),
    Country VARCHAR(2),
    CountryID TINYINT
)
WITH(
    LOCATION = 'DimStates/**',
    DATA_SOURCE = ContosoSilver,
    FILE_FORMAT = PARQUET_file_format
);

--CREATE STORED PROCEDURE TO UPDATE THE StatesTable
CREATE OR ALTER PROCEDURE Contoso.Update_States_Dimensions
AS
BEGIN
    DECLARE @MaxBanch INT,
            @CreateStmt NVARCHAR(MAX),
            @DropStmt NVARCHAR(MAX);

    SELECT @MaxBanch = MAX(banch) + 1
    FROM (
        SELECT DISTINCT
            CONVERT(INT, Banches.filepath(1)) AS banch
        FROM OPENROWSET(
            BULK('DimStates/batch=*/'),
            DATA_SOURCE = 'ContosoSilver',
            FORMAT = 'PARQUET'
        ) AS Banches
    ) AS FinalBanches;

    SET @CreateStmt = 
        'CREATE EXTERNAL TABLE Contoso.DimStates_'+ CAST(@MaxBanch AS NVARCHAR(10))+'
        WITH(
            LOCATION =''DimStates/batch='+ CAST(@MaxBanch AS NVARCHAR(10)) +''',
            DATA_SOURCE = ContosoSilver,
            FILE_FORMAT = PARQUET_file_format
        )
        AS
            WITH Countries AS (
                SELECT CountryID, Country
                FROM Contoso.DimCountries
            ),
            StatesData AS (
                SELECT DISTINCT
                    GeoAreaKey,
                    State,
                    StateFull,
                    Country
                FROM OPENROWSET(
                    BULK ''customer.parquet'',
                    DATA_SOURCE = ''ContosoBronze'',
                    FORMAT = ''PARQUET''
                ) AS States
            )
            SELECT 
                SD.GeoAreaKey,
                CAST(SD.State AS VARCHAR(28)) AS State,
                CAST(SD.StateFull AS VARCHAR(28)) AS StateFull,
                CAST(SD.Country AS VARCHAR(2)) AS Country,
                C.CountryID
            FROM StatesData SD
            JOIN Countries C
                ON SD.Country = C.Country
            WHERE NOT EXISTS (
                SELECT 1
                FROM Contoso.DimStates DS
                WHERE SD.GeoAreaKey = DS.GeoAreaKey
            )
            ORDER BY SD.GeoAreaKey ASC';
    SET @DropStmt = 
        'DROP EXTERNAL TABLE Contoso.DimStates_'+ CAST(@MaxBanch AS NVARCHAR(10))+';'

    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;
END
GO

