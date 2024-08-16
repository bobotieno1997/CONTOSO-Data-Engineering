USE Contoso
GO

CREATE VIEW Contoso.view_fctSales
AS
    SELECT 
        CONCAT(Year, Month, Day) AS YearMonthDay,
        Finalsales.*
    FROM (
        SELECT
            sales.filepath(1) AS Year,
            sales.filepath(2) AS Month,
            sales.filepath(3) AS Day,
            sales.*
        FROM
            OPENROWSET(
                BULK 'FactSales/Year=*/Month=*/Day=*/*.parquet',
                DATA_SOURCE = 'ContosoSilver',
                FORMAT = 'PARQUET'
            ) WITH (
                OrderKey INT,
                LineNumber TINYINT,
                OrderDate DATETIME2(7),
                DeliveryDate DATETIME2(7),
                CustomerKey INT,
                StoreKey INT,
                ProductKey SMALLINT,
                Quantity TINYINT,
                UnitPrice NUMERIC(20, 5),
                NetPrice NUMERIC(20, 5),
                UnitCost NUMERIC(20, 5),
                CurrencyCode VARCHAR(6),
                ExchangeRate NUMERIC(20, 5)
            ) AS sales
    ) AS Finalsales;

---CREATE A PROCEDURE TO ALWAYS UPDATE THE FACTS PARTITIONS
CREATE OR ALTER PROCEDURE Contoso.USP_Fact_Sales
@Year VARCHAR(4),
@Month VARCHAR(2),
@Day VARCHAR(2)
AS
    BEGIN
        DECLARE @CreateStmt NVARCHAR(MAX),
                @DropStmt NVARCHAR(MAX);

        SET @CreateStmt =
            'CREATE EXTERNAL TABLE Contoso.Fact_Sales_'+@Year+'_'+@Month+'_'+@Day+'
            WITH(
                LOCATION = ''FactSales/Year='+@Year+'/Month='+@Month+'/Day='+@Day+''',
                DATA_SOURCE = ContosoSilver,
                FILE_FORMAT = PARQUET_file_format
            )
            AS
                SELECT
                    *
                FROM
                    OPENROWSET(
                        BULK ''sales.parquet'',
                        DATA_SOURCE = ''ContosoBronze'',
                        FORMAT = ''PARQUET''
                    ) 
                    WITH(
                        OrderKey INT,
                        LineNumber	TINYINT,
                        OrderDate datetime2(7),
                        DeliveryDate datetime2(7),
                        CustomerKey	INT,
                        StoreKey int,
                        ProductKey	SMALLINT,
                        Quantity TINYINT,
                        UnitPrice numeric(20,5),
                        NetPrice numeric(20,5),
                        UnitCost numeric(20,5),
                        CurrencyCode VARCHAR(6),
                        ExchangeRate numeric(20,5)
                    )
                    AS [result]
                    WHERE YEAR(OrderDate) ='''+@Year+'''  
                    AND FORMAT(MONTH(OrderDate),''00'')='''+@Month+''' 
                    AND FORMAT(DAY(OrderDate),''00'')='''+@Day+''' 
                ORDER BY  OrderDate ASC;'
        SET @DropStmt =
            'DROP EXTERNAL TABLE Contoso.Fact_Sales_'+@Year+'_'+@Month+'_'+@Day+';'  
        
        EXEC sp_executesql @CreateStmt;
        EXEC sp_executesql @DropStmt;

    END
    GO












