Use SalesDB

SELECT
    SUM(Sales) AS Total_Sales
FROM Sales.Orders;


SELECT 
    ProductID,
    SUM(Sales) AS Total_Sales
FROM Sales.Orders
GROUP BY ProductID;


SELECT
    OrderID,
    OrderDate,
    ProductID,
    Sales,
    SUM(Sales) OVER () AS Total_Sales
FROM Sales.Orders;


SELECT
    OrderID,
    OrderDate,
    ProductID,
    Sales,
    SUM(Sales) OVER () AS Total_Sales,
    SUM(Sales) OVER (PARTITION BY ProductID) AS Sales_By_Product
FROM Sales.Orders;


use SalesDB
SELECT
    OrderID,
    OrderDate,
    Sales,
    OrderStatus,
    ProductID,
    SUM(Sales) OVER () AS TotalSales,
    SUM(Sales) OVER (PARTITION BY ProductID) AS SalesByProduct,
    SUM(Sales) OVER (PARTITION BY ProductID, OrderStatus) AS SalesByProductandstatus
FROM Sales.Orders


select * from sales.Orders


SELECT
    OrderID,
    OrderDate,
    Sales,
    Dense_rank() OVER (ORDER BY Sales ASC) AS Rank_Sales
FROM Sales.Orders;

Use SalesDB
SELECT
    OrderID,
    OrderDate,
    ProductID,
    OrderStatus,
    Sales,
    SUM(Sales) OVER (
        PARTITION BY OrderStatus 
        ORDER BY OrderDate 
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) AS Total_Sales
FROM Sales.Orders;




SELECT
    OrderID,
    OrderDate,
    ProductID,
    OrderStatus,
    Sales,
    SUM(Sales) OVER (
        PARTITION BY OrderStatus 
        ORDER BY OrderDate 
        ROWS 1 FOLLOWING
    ) AS Total_Sales
FROM Sales.Orders;

use SalesDB
SELECT
    CustomerID,
    SUM(Sales) AS Total_Sales,
    RANK() OVER (ORDER BY SUM(Sales) DESC) AS Rank_Customers
FROM Sales.Orders
GROUP BY CustomerID;


SELECT
    OrderID,
    OrderDate,
    CustomerID,
    COUNT(*) OVER() AS TotalOrders,
    COUNT(*) OVER(PARTITION BY CustomerID) AS OrdersByCustomers
FROM Sales.Orders



SELECT
    *,
    COUNT(*) OVER () AS TotalCustomersStar,
    COUNT(1) OVER () AS TotalCustomersOne,
    COUNT(Score) OVER() AS TotalScores,
    COUNT(Country) OVER() AS TotalCountries
FROM Sales.Customers


select * from (

SELECT 
        OrderID,
        COUNT(*) OVER(PARTITION BY OrderID) AS CheckDuplicates
    FROM Sales.OrdersArchive)t
where CheckDuplicates>1



SELECT
    OrderID,
    OrderDate,
    Sales,
    productID,
    SUM(Sales) OVER () AS TotalSales,
        SUM(Sales) OVER (PARTITION BY ProductID) AS SalesByProduct

FROM Sales.Orders



SELECT
    OrderID,
    ProductID,
    Sales,
    SUM(Sales) OVER () AS TotalSales,
    ROUND(CAST(sales AS FLOAT)/sum(Sales) OVER () *100,2)
FROM Sales.Orders




SELECT
    OrderID,
    ProductID,
    Sales,
    AVG(Sales) OVER () AS AVGSales,
    AVG(Sales) OVER (PARTITION BY productID) AS AvgSalesByProduct
FROM Sales.Orders


SELECT
    CustomerID,
    LastName,
    Score,
    COALESCE(Score, 0) AS CustomerScore,
    AVG(Score) OVER () AS AvgScore,
    AVG(COALESCE(Score, 0)) OVER () AS AvgScoreWithoutNull
FROM Sales.Customers


SELECT 
    c.name AS COLUMN_NAME,
    t.name AS DATA_TYPE,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('Sales.Orders');


select * from (
SELECT
        OrderID,
        ProductID,
        Sales,
        AVG(Sales) OVER () AS Avg_Sales
    FROM Sales.Orders
)t 
where Avg_Sales<Sales


SELECT 
    OrderID,
    ProductID,
    OrderDate,
    Sales,
    MIN(Sales) OVER () AS LowestSales,
    MAX(Sales) OVER () AS HighestSales,
    MIN(Sales) OVER (PARTITION BY ProductID) AS LowestSalesByProduct,
    MAX(Sales) OVER (PARTITION BY ProductID) AS highestSalesByProduct
FROM Sales.Orders

SELECT
    OrderID,
    OrderDate,
    ProductID,
    Sales,
    MAX(Sales) OVER () AS HighestSales,
    MIN(Sales) OVER () AS LowestSales,
    Sales - MIN(Sales) OVER () AS DeviationFromMin,
    MAX(Sales) OVER () - Sales AS DeviationFromMax
FROM Sales.Orders




SELECT
    OrderID,
    ProductID,
    OrderDate,
    Sales,
    AVG(Sales) OVER (PARTITION BY ProductID ORDER BY OrderDate ) AS RollingAvg
FROM Sales.Orders


--  10 hrs 48 min 41 sec

-- 123

-- 789
-- 459

-- 10 11 13
