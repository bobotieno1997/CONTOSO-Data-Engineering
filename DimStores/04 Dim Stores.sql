USE Contoso
GO

--CREATE EXTERNAL TABLE
IF OBJECT_ID('Contoso.DimStores')IS NOT NULL
DROP EXTERNAL TABLE Contoso.DimStores
GO

CREATE EXTERNAL TABLE Contoso.DimStores
(
    StoreKey INT ,
    StoreCode TINYINT,
    GeoAreaKey SMALLINT,
    OpenDate DATETIME2(7),
    CloseDate DATETIME2(7),
    Description VARCHAR(42),
    SquareMeters SMALLINT,
     Status VARCHAR(15)
)
WITH (
      LOCATION = 'DimStores/**',
      DATA_SOURCE = ContosoSilver,
      FILE_FORMAT =  PARQUET_file_format
    )
GO

-- STORED PROCEDURE TO ALWAYS UPDATE THE DimProductCategories
CREATE OR ALTER PROCEDURE Contoso.USP_Stores_Dimensions
AS
BEGIN
    DECLARE @CreateStmt NVARCHAR(MAX),
            @DropStmt NVARCHAR(MAX),
            @MaxBanch INT;

    SELECT @MaxBanch = MAX(banch) + 1
    FROM (
        SELECT DISTINCT
            CONVERT(INT, Banches.filepath(1)) AS banch
        FROM OPENROWSET(
            BULK 'DimStores/batch=*/',
            DATA_SOURCE = 'ContosoSilver',
            FORMAT = 'PARQUET'
        ) AS Banches
    ) AS FinalBanches;

    SET @CreateStmt =
       'CREATE EXTERNAL TABLE Contoso.DimStores_'+CAST(@MaxBanch AS VARCHAR(10))+'
       WITH(
                LOCATION = ''DimStores/batch='+CAST(@MaxBanch AS VARCHAR(10))+''',
                DATA_SOURCE = ContosoSilver,
                FILE_FORMAT = PARQUET_file_format
       )AS
            SELECT
                CAST(StoreKey AS INT) AS StoreKey,
                CAST(CASE WHEN StoreCode = -1 THEN 0 ELSE StoreCode END AS TINYINT) AS StoreCode,
                CAST(CASE WHEN GeoAreaKey = -1 THEN 0 ELSE GeoAreaKey END AS SMALLINT) AS GeoAreaKey,
                OpenDate,
                CloseDate,
                CAST(Description AS VARCHAR(42)) AS Description,
                CAST(SquareMeters AS SMALLINT) AS SquareMeters,
                CAST(CASE 
                    WHEN Status IS NULL THEN ''Online''
                    WHEN Status = '''' THEN ''Operational''
                    ELSE Status 
                END AS VARCHAR(15)) AS Status
            FROM
                OPENROWSET(
                    BULK ''store.parquet'',
                    DATA_SOURCE = ''ContosoBronze'',
                    FORMAT = ''PARQUET''
                ) AS [result]
            WHERE NOT EXISTS (
                SELECT 1 
                FROM Contoso.DimStores AS ds
                WHERE ds.StoreKey = result.StoreKey
            );'
   
    SET @DropStmt =
        'DROP EXTERNAL TABLE Contoso.DimStores_'+CAST(@MaxBanch AS VARCHAR(10))+';'

    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;

END
GO

EXEC Contoso.USP_Stores_Dimensions
GO





