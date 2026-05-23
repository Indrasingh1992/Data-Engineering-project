
use SalesDB
/* TASK 9:
   Find Products that Fall Within the Highest 40% of the Prices
*/
SELECT 
    Product,
    Price,
    DistRank,
    CONCAT(DistRank * 100, '%') AS DistRankPerc
FROM (
    SELECT
        Product,
        Price,
        CUME_DIST() OVER (ORDER BY Price DESC) AS DistRank
    FROM Sales.Products
) AS PriceDistribution
WHERE DistRank <= 1;

SELECT 
    Product,
    Price,
    DistRank,
    CONCAT(DistRank * 100, '%') AS DistRankPerc
FROM (
    SELECT
        Product,
        Price,
        PERCENT_RANK() OVER (ORDER BY Price DESC) AS DistRank
    FROM Sales.Products
) AS PriceDistribution
WHERE DistRank <= 1;



use SalesDB

select *, CurrentMonthSales - PreviousMonthSales as MOM_change,
ROUND(cast((CurrentMonthSales - PreviousMonthSales) AS Float)/previousMonthSales*100,1)  as MOM_perc
from
(select Month(orderdate) OrderMonth, 
sum(Sales) CurrentMonthSales,
lag(sum(sales)) over (ORDER BY MONTH(OrderDate)) previousMonthSales
from sales.Orders
group by Month(orderdate))t



select
CustomerID,
AVG(DaysUnitnxtOrder) AvgDays,
rank() over (ORDER BY COALESCE(AVG(DaysUnitnxtOrder), 999999)) Rankavg from
(
SELECT
        OrderID,
        CustomerID,
        OrderDate,
        LEAD(OrderDate) OVER(PARTITION BY CustomerID ORDER BY OrderDate) next_order,
        DATEDIFF(day,OrderDate,LEAD(OrderDate) OVER(PARTITION BY CustomerID ORDER BY OrderDate)) DaysUnitnxtOrder
from Sales.Orders)t
GROUP BY CustomerID



SELECT
    OrderID,
    ProductID,
    Sales,
    FIRST_VALUE(Sales) OVER (PARTITION BY ProductID ORDER BY Sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LowestSales,
    LAST_VALUE(Sales) OVER (PARTITION BY ProductID ORDER BY Sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS HighestSales,
    Sales - FIRST_VALUE(Sales) OVER (PARTITION BY ProductID ORDER BY Sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) As SalesDifference
    FROM Sales.Orders;


   

use SalesDB
    select product, Category from Sales.Products where price > (select AVG(price) from Sales.Products)


SELECT
*
FROM (
    -- Subquery
    SELECT
        ProductID,
        Price,
        AVG(Price) OVER () AS AvgPrice
    FROM Sales.Products
) AS t
WHERE Price > AvgPrice;




select *,
RANK() OVER (ORDER BY TotalSales DESC) AS Customerrank 
from 

 (SELECT
        CustomerID,
        SUM(Sales) AS TotalSales
    FROM Sales.Orders
    GROUP BY CustomerID
) AS t;




-- main Query
Select 
PRODUCTID,
PRODUCT,
Price,
--subquery
(select count(*) from Sales.orders) as TotalOrders
from 
Sales.Products



-- main query

select c.*, o.total_orders  from Sales.Customers c
LEFT JOIN(

    select count(*) total_orders, customerID from Sales.orders GROUP BY CustomerID) o
     on c.CustomerID=o.CustomerID



SELECT c.*, 
       (SELECT COUNT(*) FROM Sales.Orders o 
        WHERE o.CustomerID = c.CustomerID) AS total_orders
FROM Sales.Customers c;


WITH CustomerOrders AS (
    SELECT customerID, COUNT(*) AS total_orders 
    FROM Sales.Orders 
    GROUP BY CustomerID
)
SELECT c.*, CO.total_orders
FROM Sales.Customers c
LEFT JOIN CustomerOrders CO ON c.CustomerID = CO.customerID;



use SalesDB
-- write sql query to get the product list with price less than 20
SELECT Product, Price FROM Sales.Products WHERE Price < 20

 -- start  13:31:37 for advance sql Subqueries sql


SELECT *,
RANK() OVER (ORDER BY TotalSales DESC) AS Customerrank
 from (
SELECT
        CustomerID,
        SUM(Sales) AS TotalSales
    FROM Sales.Orders GROUP BY CustomerID) AS t


/*
====
   SUBQUERY | COMPARISON OPERATORS
===============================================================================*/
use SalesDB
/* TASK 6:
   Find the products that have a price higher than the average price of all products.
*/
-- Main Query
SELECT
    ProductID,
    Price,
    (SELECT AVG(Price) FROM Sales.Products) AS AvgPrice -- Subquery
FROM Sales.Products
WHERE Price > (SELECT AVG(Price) FROM Sales.Products); -- Subquery



SELECT
    *
FROM Sales.Orders
WHERE CustomerID NOT IN (
    -- Subquery
    SELECT
        CustomerID
    FROM Sales.Customers
    WHERE Country = 'Germany');
    
    use SalesDB
/* TASK 9:
   Find female employees whose salaries are greater than the salaries of any male employees.
*/
SELECT
    EmployeeID, 
    FirstName,
    Salary
FROM Sales.Employees
WHERE Gender = 'F'
  AND Salary > ALL (
      SELECT Salary
      FROM Sales.Employees
      WHERE Gender = 'M'
  );




  use SalesDB
/* TASK 10:
   Show all customer details and the total orders for each customer using a correlated subquery.
*/
SELECT * , (Select count(*) from sales.Orders o where o.CustomerID = c.CustomerID) as TotalSales FROM Sales.customers c;
