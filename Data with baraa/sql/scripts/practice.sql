use MyDatabase;
UPDATE customers
SET first_name = 'USA', country = 'Max'
WHERE id = 8

use SalesDB;

/* last 08:07:00 seen video*/ 

select Category, SUM(Sales) as TotalSales
FROM (
    SELECT
        OrderID,
        Sales,
        CASE
            WHEN Sales > 50 THEN 'High'
            WHEN Sales > 20 THEN 'Medium'
            ELSE 'Low'
        END AS Category FROM Sales.Orders
) AS t
GROUP BY Category;
ORDER BY TotalSales DESC;

use SalesDB


SELECT
    CustomerID,
    LastName,
    Score,
    CASE
            WHEN Score IS NULL THEN 0
            ELSE Score
    END ScoreClean,
    AVG(CASE
            WHEN Score IS NULL THEN 0
            ELSE Score
        END) over() as AvgCustomerClean,

    AVG(Score) over() as AvgCustomer
    from Sales.Customers



    select * from customers

    