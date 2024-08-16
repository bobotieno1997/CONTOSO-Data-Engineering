USE CONTOSO 
GO

---- CREATE EXTERNAL TABLE 
IF OBJECT_ID('Contoso.DimCustomers') IS NOT NULL
DROP EXTERNAL TABLE Contoso.DimCustomers
GO

CREATE EXTERNAL TABLE Contoso.DimCustomers
(
    CustomerKey INT ,
    GeoAreaKey SMALLINT ,
    StartDT DATETIME2(7),
    EndDt DATETIME2(7),
    Gender  VARCHAR(6) ,
    Title VARCHAR(4),
    GivenName  VARCHAR(15),
    Surname  VARCHAR(23),
    StreetAddress VARCHAR(60),
    City VARCHAR(44),
    ZipCode VARCHAR(8),
    Birthday DATETIME2(7),
    Age TINYINT,
    Occupation VARCHAR(62),
    Company VARCHAR(35),
    Vehicle VARCHAR(43),
    Latitude SMALLINT,
    Longitude SMALLINT
)
WITH(
    LOCATION = 'DimCustomers/**',
    DATA_SOURCE = ContosoSilver,
    FILE_FORMAT = PARQUET_file_format
) 
GO

--- STORED PROCEDURE TO ALWAYS UPDATE THE TABLE
CREATE OR ALTER PROCEDURE Contoso.USP_Customers_Dimensions
AS
BEGIN
    DECLARE @MaxBanch INT,
            @CreateStmt NVARCHAR(MAX),
            @DropStmt NVARCHAR(MAX)
    --RETRIEVE THE MAXIMUM BANCH NUMBER AND ADD ONE WHEN CREATING A NEW ONE
    SELECT @MaxBanch = MAX(banch) + 1
    FROM (
        SELECT DISTINCT
            CONVERT(INT, Banches.filepath(1)) AS banch
        FROM OPENROWSET(
            BULK('DimCustomers/batch=*/'),
            DATA_SOURCE = 'ContosoSilver',
            FORMAT = 'PARQUET'
        ) AS Banches
    ) AS FinalBanches;

    --CREATE EXTERNAL TABLE STATEMENT FOR THE NEW SET OF PARQUET
    SET @CreateStmt = 
    'CREATE EXTERNAL TABLE Contoso.DimCustomer_'+CAST(@MaxBanch AS NVARCHAR(10))+'
    WITH(
        LOCATION = ''DimCustomers/batch='+CAST(@MaxBanch AS NVARCHAR(10))+''',
        DATA_SOURCE = ContosoSilver,
        FILE_FORMAT = PARQUET_file_format
    )AS
        SELECT 
            CAST(CustomerKey AS INT) AS CustomerKey,
            CAST(GeoAreaKey AS SMALLINT) AS GeoAreaKey,
            StartDT,
            EndDt,
            CAST(Gender AS VARCHAR(6)) AS Gender,
            CAST(Title AS VARCHAR(4)) AS Title,
            CAST(GivenName AS VARCHAR(15)) AS GivenName,
            CAST(Surname AS VARCHAR(23)) AS Surname,
            CAST(StreetAddress AS VARCHAR(60)) AS StreetAddress,
            CAST(City AS VARCHAR(44)) AS City,
            CAST(ZipCode AS VARCHAR(8)) AS ZipCode,
            Birthday,
            CAST(Age AS TINYINT) AS Age,
            CAST(Occupation AS VARCHAR(62)) AS Occupation,
            CAST(Company AS VARCHAR(35)) AS Company,
            CAST(Vehicle AS VARCHAR(43)) AS Vehicle,
            CAST(Latitude AS SMALLINT) AS Latitude,
            CAST(Longitude AS SMALLINT) AS Longitude
        FROM 
            OPENROWSET(
                BULK ''customer.parquet'',
                DATA_SOURCE = ''ContosoBronze'',
                FORMAT = ''PARQUET''
            ) AS Customers
        WHERE 
            CustomerKey NOT IN(
                SELECT
                CustomerKey FROM Contoso.DimCustomers)
        ORDER BY 
            CustomerKey ASC;'
    SET @DropStmt = 
        'DROP EXTERNAL TABLE Contoso.DimCustomer_'+CAST(@MaxBanch AS NVARCHAR(10))+';'

    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;
END
GO

EXEC Contoso.USP_Customers_Dimensions
