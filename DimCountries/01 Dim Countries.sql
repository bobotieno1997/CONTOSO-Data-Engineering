USE Contoso
GO

---CREATE EXTERNAL TABLE
IF OBJECT_ID('Contoso.DimCountries') IS NOT NULL
DROP EXTERNAL TABLE Contoso.DimCountries
GO

CREATE EXTERNAL TABLE Contoso.DimCountries 
(
    CountryID TINYINT,
    Continent VARCHAR(20),
    CountryFull VARCHAR(20),
    Country VARCHAR(2)
)
WITH(
    LOCATION = 'DimCountries/**',
    DATA_SOURCE = ContosoSilver,
    FILE_FORMAT = PARQUET_file_format
);

--- STORED PROCEDURE TO UPDATE THE TABLE EVER SINGLE TIME NEW COUNTRIES ARE ADDED
CREATE OR ALTER PROCEDURE Contoso.Update_Countries_Dimensions
AS
BEGIN
    DECLARE @MaxBanch INT,
            @CreateStmt NVARCHAR(MAX),
            @DropStmt NVARCHAR(MAX);

    -- Get the latest branch number
    SELECT @MaxBanch = MAX(banch) + 1
    FROM (
        SELECT DISTINCT
            CONVERT(INT, Banches.filepath(1)) AS banch
        FROM OPENROWSET(
            BULK('DimCountries/batch=*/'),
            DATA_SOURCE = 'ContosoSilver',
            FORMAT = 'PARQUET'
        ) AS Banches
    ) AS FinalBanches;

    -- Create the dynamic SQL statement for creating the new external table
    SET @CreateStmt = N'
    CREATE EXTERNAL TABLE Contoso.NewDimCountries_' + CAST(@MaxBanch AS NVARCHAR(10)) + '
    WITH (
        LOCATION = ''DimCountries/batch=' + CAST(@MaxBanch AS NVARCHAR(10)) + ''',
        DATA_SOURCE = ContosoSilver,
        FILE_FORMAT = PARQUET_file_format
    )
    AS
    WITH MaxCountryID AS (
        SELECT MAX(CountryID) AS MaxID
        FROM Contoso.DimCountries
    ),
    NewCountries AS (
        SELECT DISTINCT
            Continent,
            CountryFull,
            Country
        FROM OPENROWSET(
            BULK ''customer.parquet'',
            DATA_SOURCE = ''ContosoBronze'',
            FORMAT = ''PARQUET''
        ) AS NCountries
        WHERE NOT EXISTS (
            SELECT 1
            FROM Contoso.DimCountries CT
            WHERE NCountries.Country = CT.Country
        )
    )
    SELECT
        CAST(MaxID + ROW_NUMBER() OVER (ORDER BY Continent, CountryFull, Country) AS TINYINT) AS CountryID,
        CAST(Continent AS VARCHAR(15)) AS Continent,
        CAST(CountryFull AS VARCHAR(15)) AS CountryFull,
        CAST(Country AS VARCHAR(2)) AS Country
    FROM NewCountries
    CROSS JOIN MaxCountryID
    ORDER BY Continent, CountryFull, Country;';

    -- Create the dynamic SQL statement for dropping the old external table
    SET @DropStmt = N'
        DROP EXTERNAL TABLE Contoso.NewDimCountries_' + CAST(@MaxBanch AS NVARCHAR(10)) + ';'

    -- Execute the drop and create statements
    
    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;
END
GO
