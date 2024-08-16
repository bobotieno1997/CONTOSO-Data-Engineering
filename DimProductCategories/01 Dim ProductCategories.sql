USE Contoso
GO

--CREATE EXTERNAL TABLE
IF OBJECT_ID('Contoso.DimProductCategories')IS NOT NULL
DROP EXTERNAL TABLE Contoso.DimProductCategories
GO

CREATE EXTERNAL TABLE Contoso.DimProductCategories
(
    CategoryKey TINYINT,
    CategoryName VARCHAR(30)
)
WITH (
      LOCATION = 'DimProductCategories/**',
      DATA_SOURCE = ContosoSilver,
      FILE_FORMAT =  PARQUET_file_format
    )
GO

-- STORED PROCEDURE TO ALWAYS UPDATE THE DimProductCategories
CREATE OR ALTER PROCEDURE Contoso.USP_ProductCategories_Dimensions
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
            BULK('DimProductCategories/batch=*/'),
            DATA_SOURCE = 'ContosoSilver',
            FORMAT = 'PARQUET'
        ) AS Banches
    ) AS FinalBanches;

    SET @CreateStmt ='
    CREATE EXTERNAL TABLE Contoso.DimProductCat_'+CAST(@MaxBanch AS NVARCHAR(10))+'
    WITH(
        LOCATION = ''DimProductCategories/batch='+CAST(@MaxBanch AS NVARCHAR(10))+''',
        DATA_SOURCE = ContosoSilver,
        FILE_FORMAT= PARQUET_file_format
    )
    AS
        SELECT DISTINCT
            Categories.CategoryKey,
            Categories.CategoryName
        FROM
            OPENROWSET(
                BULK ''product.parquet'',
                DATA_SOURCE = ''ContosoBronze'',
                FORMAT = ''PARQUET''
                ) AS Categories
        LEFT JOIN Contoso.DimProductCategories ExistingCategories
        ON Categories.CategoryKey = ExistingCategories.CategoryKey
            WHERE ExistingCategories.CategoryKey IS NULL
        ORDER BY Categories.CategoryKey ASC;'
    
    SET @DropStmt =
        'DROP EXTERNAL TABLE Contoso.DimProductCat_'+CAST(@MaxBanch AS NVARCHAR(10))+';'

    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;

END
GO

EXEC Contoso.USP_ProductCategories_Dimensions
GO





