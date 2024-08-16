USE Contoso
GO
---CREATE VIEW FOR THE CURRENCY PARTITIONES
DROP VIEW Contoso.view_fctCurrencyExchange 
GO

CREATE VIEW Contoso.view_fctCurrencyExchange 
AS
    SELECT 
        CONCAT(Year, Month, Day) AS YearMonthDay,
        FinalExchange.*
    FROM (
        SELECT
            exchange.filepath(1) AS Year,
            exchange.filepath(2) AS Month,
            exchange.filepath(3) AS Day,
            exchange.*
        FROM
            OPENROWSET(
                BULK 'FactCurrencyExchange/Year=*/Month=*/Day=*/*.parquet',
                DATA_SOURCE = 'ContosoSilver',
                FORMAT = 'PARQUET'
            ) AS exchange
    ) AS FinalExchange;


--STORED PROCEDURE TO ADD NEW CURRENCY PARTITIONS IN THE DATA
CREATE OR ALTER PROCEDURE Contoso.USP_Fact_CurrencyExchange
@Year NVARCHAR(4),
@Month NVARCHAR(2),
@Day NVARCHAR(2)
AS
BEGIN
    DECLARE @CreateStmt NVARCHAR(MAX),
            @DropStmt NVARCHAR(MAX);

    SET @DropStmt = 
        'DROP EXTERNAL TABLE Contoso.FactCurrencyExchange_' + @Year + '_' + @Month + '_' + @Day + ';'
    
    SET @CreateStmt = 
        'CREATE EXTERNAL TABLE Contoso.FactCurrencyExchange_' + @Year + '_' + @Month + '_' + @Day + '
        WITH(
            LOCATION = ''FactCurrencyExchange/Year=' + @Year + '/Month=' + @Month + '/Day=' + @Day + ''',
            DATA_SOURCE = ContosoSilver,
            FILE_FORMAT = PARQUET_file_format
        )
        AS
        SELECT
            CONVERT(DATE, Date) AS Date,
            CAST(FromCurrency AS VARCHAR(3)) AS FromCurrency,
            CAST(ToCurrency AS VARCHAR(3)) AS ToCurrency,
            Exchange
        FROM
            OPENROWSET(
                BULK ''currencyexchange.parquet'',
                DATA_SOURCE = ''ContosoBronze'',
                FORMAT = ''PARQUET''
            ) AS [result]
        WHERE YEAR(Date) = ' + @Year + ' AND 
              FORMAT(MONTH(Date), ''00'') = ''' + @Month + ''' AND 
              FORMAT(DAY(Date), ''00'') = ''' + @Day + '''
        ORDER BY Date, FromCurrency, ToCurrency ASC;';
    
    EXEC sp_executesql @CreateStmt;
    EXEC sp_executesql @DropStmt;
    
END
GO



EXEC Contoso.USP_Fact_CurrencyExchange '2015','01','01';

SELECT DISTINCT
    CAST(YEAR(Date) AS NVARCHAR(4)) AS Year,
    FORMAT(MONTH(Date),'00') AS Month,
    FORMAT(DAY(Date),'00') AS Day
FROM
    OPENROWSET(
        BULK 'currencyexchange.parquet',
        DATA_SOURCE = 'ContosoBronze',
        FORMAT = 'PARQUET'
    ) AS [result]
ORDER BY CAST(YEAR(Date) AS NVARCHAR(4)),
    FORMAT(MONTH(Date),'00'),
    FORMAT(DAY(Date),'00')
















