-- Create Customer_Dim table
CREATE TABLE Customer_Dim (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    CustomerAddress VARCHAR(100)
);

-- Create Product_Dim table
CREATE TABLE Product_Dim (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(100),
    ProductPrice DECIMAL(10, 2)
);

-- Create SalesPerson_Dim table
CREATE TABLE SalesPerson_Dim (
    SalesPersonID INT PRIMARY KEY IDENTITY(1,1),
    SalesPerson VARCHAR(100)
);

-- Create TransactionDate_Dim table
CREATE TABLE TransactionDate_Dim (
    TransactionDateID INT PRIMARY KEY IDENTITY(1,1),
    TransactionDate DATE
);

-- Create Sales_Fact table
CREATE TABLE Sales_Fact (
    FactID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    ProductID INT,
    SalesPersonID INT,
    TransactionDateID INT,
    QuantitySold INT,
    FOREIGN KEY (CustomerID) REFERENCES Customer_Dim(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Product_Dim(ProductID),
    FOREIGN KEY (SalesPersonID) REFERENCES SalesPerson_Dim(SalesPersonID),
    FOREIGN KEY (TransactionDateID) REFERENCES TransactionDate_Dim(TransactionDateID)
);




CREATE PROCEDURE UpsertAndAppendFact
AS
BEGIN
    SET NOCOUNT ON;

    -- Upsert Customer dimension table
    MERGE INTO Customer_Dim AS target
    USING (
        SELECT DISTINCT CustomerName, CustomerEmail, CustomerAddress
        FROM SalesTransactions
    ) AS source
    ON (target.CustomerName = source.CustomerName AND target.CustomerEmail = source.CustomerEmail AND target.CustomerAddress = source.CustomerAddress)
    WHEN NOT MATCHED THEN
        INSERT (CustomerName, CustomerEmail, CustomerAddress)
        VALUES (source.CustomerName, source.CustomerEmail, source.CustomerAddress);

    -- Upsert Product dimension table
    MERGE INTO Product_Dim AS target
    USING (
        SELECT DISTINCT ProductName, ProductCategory, ProductPrice
        FROM SalesTransactions
    ) AS source
    ON (target.ProductName = source.ProductName AND target.ProductCategory = source.ProductCategory AND target.ProductPrice = source.ProductPrice)
    WHEN NOT MATCHED THEN
        INSERT (ProductName, ProductCategory, ProductPrice)
        VALUES (source.ProductName, source.ProductCategory, source.ProductPrice);

    -- Upsert SalesPerson dimension table
    MERGE INTO SalesPerson_Dim AS target
    USING (
        SELECT DISTINCT SalesPerson
        FROM SalesTransactions
    ) AS source
    ON (target.SalesPerson = source.SalesPerson)
    WHEN NOT MATCHED THEN
        INSERT (SalesPerson)
        VALUES (source.SalesPerson);

    -- Upsert TransactionDate dimension table
    MERGE INTO TransactionDate_Dim AS target
    USING (
        SELECT DISTINCT TransactionDate
        FROM SalesTransactions
    ) AS source
    ON (target.TransactionDate = source.TransactionDate)
    WHEN NOT MATCHED THEN
        INSERT (TransactionDate)
        VALUES (source.TransactionDate);

    -- Append Fact table
    INSERT INTO Sales_Fact (CustomerID, ProductID, SalesPersonID, TransactionDateID, QuantitySold)
    SELECT
        cd.CustomerID, pd.ProductID, spd.SalesPersonID, tdd.TransactionDateID, st.QuantitySold
    FROM
        SalesTransactions st
    JOIN
        Customer_Dim cd ON st.CustomerName = cd.CustomerName AND st.CustomerEmail = cd.CustomerEmail AND st.CustomerAddress = cd.CustomerAddress
    JOIN
        Product_Dim pd ON st.ProductName = pd.ProductName AND st.ProductCategory = pd.ProductCategory AND st.ProductPrice = pd.ProductPrice
    JOIN
        SalesPerson_Dim spd ON st.SalesPerson = spd.SalesPerson
    JOIN
        TransactionDate_Dim tdd ON st.TransactionDate = tdd.TransactionDate;
END;





CREATE TRIGGER trg_ExecuteProcedure
ON YourSourceTable
AFTER INSERT
AS
BEGIN
    -- Check if the current time is 14:00
    IF CONVERT(time, GETDATE()) = '14:00:00'
    BEGIN
        -- Execute your stored procedure
        EXEC UpsertAndAppendFact;
    END
END;