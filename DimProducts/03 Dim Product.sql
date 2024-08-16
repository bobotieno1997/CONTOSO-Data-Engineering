USE Contoso
GO

--CREATE EXTERNAL TABLE
IF OBJECT_ID('Contoso.DimProducts')IS NOT NULL
DROP EXTERNAL TABLE Contoso.DimProducts
GO

CREATE EXTERNAL TABLE Contoso.DimProducts
(
    ProductKey SMALLINT,
    ProductCode VARCHAR(10),
    ProductName VARCHAR(90),
    Manufacturer VARCHAR(30),
    Brand VARCHAR(30),
    Color VARCHAR(15),
    WeightUnit VARCHAR(10),
    Weight NUMERIC(20,5),
    Cost NUMERIC(20,5),
    Price NUMERIC(20,5),
    SubCategoryKey SMALLINT
)
WITH (
      LOCATION = 'DimProducts/**',
      DATA_SOURCE = ContosoSilver,
      FILE_FORMAT =  PARQUET_file_format
    )
GO

-- STORED PROCEDURE TO ALWAYS UPDATE THE DimProductCategories
CREATE OR ALTER PROCEDURE Contoso.USP_Products_Dimensions
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
            BULK 'DimProducts/batch=*/',
            DATA_SOURCE = 'ContosoSilver',
            FORMAT = 'PARQUET'
        ) AS Banches
    ) AS FinalBanches;

    SET @CreateStmt =
       'CREATE EXTERNAL TABLE Contoso.DimProducts_'+CAST(@MaxBanch AS VARCHAR(10))+'
       WITH(
                LOCATION = ''DimProducts/batch='+CAST(@MaxBanch AS VARCHAR(10))+''',
                DATA_SOURCE = ContosoSilver,
                FILE_FORMAT = PARQUET_file_format
       )AS
            SELECT DISTINCT
                CAST(result.ProductKey AS SMALLINT) AS ProductKey, 
                CAST(result.ProductCode AS VARCHAR(10)) AS ProductCode,
                CAST(result.ProductName AS VARCHAR(90)) AS ProductName,
                CAST(result.Manufacturer AS VARCHAR(30)) AS Manufacturer,
                CAST(result.Brand AS VARCHAR(30)) AS Brand,
                CAST(result.Color AS VARCHAR(15)) AS Color,
                CAST(result.WeightUnit AS VARCHAR(10)) AS WeightUnit,
                CAST(result.Weight AS NUMERIC(20,5)) AS Weight,
                CAST(result.Cost AS NUMERIC(20,5)) AS Cost,
                CAST(result.Price AS NUMERIC(20,5)) AS Price,
                CAST(result.SubCategoryKey AS SMALLINT) AS SubCategoryKey
            FROM
                OPENROWSET(
                    BULK ''product.parquet'',
                    DATA_SOURCE = ''ContosoBronze'',
                    FORMAT = ''PARQUET''
                ) AS result
            LEFT JOIN
                Contoso.DimProducts AS dp
            ON
                result.ProductKey = dp.ProductKey
            WHERE
                dp.ProductKey IS NULL
            ORDER BY
                ProductKey ASC;'
   
    SET @DropStmt =
        'DROP EXTERNAL TABLE Contoso.DimProducts_'+CAST(@MaxBanch AS VARCHAR(10))+';'

    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;

END
GO

EXEC Contoso.USP_Products_Dimensions
GO





