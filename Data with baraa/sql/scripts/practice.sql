
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


    -- start  12:40:37 for advance sql Subqueries sql