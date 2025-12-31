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
        ROWS unbounded FOLLOWING
    ) AS Total_Sales
FROM Sales.Orders;