USE Contoso
GO

--CREATE EXTERNAL TABLE
IF OBJECT_ID('Contoso.DimProductSubCategories')IS NOT NULL
DROP EXTERNAL TABLE Contoso.DimProductSubCategories
GO

CREATE EXTERNAL TABLE Contoso.DimProductSubCategories
(
    SubCategoryKey SMALLINT,
    SubCategoryName VARCHAR(36)
)
WITH (
      LOCATION = 'DimProductSubCategories/**',
      DATA_SOURCE = ContosoSilver,
      FILE_FORMAT =  PARQUET_file_format
    )
GO

-- STORED PROCEDURE TO ALWAYS UPDATE THE DimProductCategories
CREATE OR ALTER PROCEDURE Contoso.USP_ProductSubCategories_Dimensions
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
            BULK('DimProductSubCategories/batch=*/'),
            DATA_SOURCE = 'ContosoSilver',
            FORMAT = 'PARQUET'
        ) AS Banches
    ) AS FinalBanches;

    SET @CreateStmt =
        'CREATE EXTERNAL TABLE Contoso.Subcategory_'+CAST(@MaxBanch AS VARCHAR(10))+'
            WITH(
                LOCATION = ''DimProductSubCategories/batch='+CAST(@MaxBanch AS VARCHAR(10))+''',
                DATA_SOURCE = ContosoSilver,
                FILE_FORMAT = PARQUET_file_format
            )
            AS
                SELECT DISTINCT 
                    CAST(result.SubCategoryKey AS SMALLINT) AS SubCategoryKey,
                    CAST(result.SubCategoryName AS VARCHAR(36)) AS SubCategoryName
                FROM
                    OPENROWSET(
                        BULK ''product.parquet'',
                        DATA_SOURCE = ''ContosoBronze'',
                        FORMAT = ''PARQUET''
                    ) AS result
                LEFT JOIN Contoso.DimProductSubcategories ExistingSubcategories
                    ON result.SubCategoryKey = ExistingSubcategories.SubCategoryKey
                WHERE ExistingSubcategories.SubCategoryKey IS NULL
                ORDER BY SubCategoryKey ASC;'
        
    SET @DropStmt =
        'DROP EXTERNAL TABLE Contoso.Subcategory_'+CAST(@MaxBanch AS VARCHAR(10))+';'

    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;

END
GO

EXEC Contoso.USP_ProductSubCategories_Dimensions
GO





