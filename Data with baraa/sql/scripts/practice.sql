use MyDatabase;
UPDATE customers
SET first_name = 'USA', country = 'Max'
WHERE id = 8

use SalesDB;

/* last 4:53:07 seen video*/ 


SELECT first_name, country,
       CONCAT(first_name, '-', country) AS full_info
FROM customers


SELECT 
    LOWER(first_name) AS lower_case_name,
    UPPER(first_name) AS upper_case_name
FROM customers



SELECT 
    first_name,
	LEN(first_name) len_name,
	LEN(TRIM(first_name)) len_trim_name,
	LEN(first_name) - LEN(TRIM(first_name)) flag
FROM customers
WHERE LEN(first_name)  != LEN(TRIM(first_name))



SELECT
'123-456-7890' AS phone,
REPLACE('123-456-7890', '-', '') AS clean_phone



SELECT 
    first_name,
    LEFT(trim(first_name), 2) AS first_2_chars
FROM customers




select first_name,
SUBSTRING(trim(first_name), 2, len(trim(first_name))) AS subname
from customers




SELECT 
    3.516 AS original_number,
    ROUND(3.516, 2) AS round_2,
    ROUND(3.516, 1) AS round_1,
    ROUND(3.516, 0) AS round_0



SELECT 
    -10 AS original_number,
    ABS(-10) AS absolute_value_negative,
    ABS(10) AS absolute_value_positive


use SalesDB;

select creationtime from Sales.Orders



select OrderID, CreationTime, '2025-08-20' AS HardCoded, GETDATE() AS Today
from Sales.Orders


select orderID, CreationTime,YEAR(CreationTime) AS Year,
MONTH(CreationTime) AS Month,
DAY(CreationTime) AS Day
from Sales.Orders


SELECT orderId,
DATEPART(YEAR, creationTime) as Year_dp,
DATEPART(MONTH, creationTime) as Month_dp,
DATEPART(DAY, creationTime) as Day_dp,
DATEPART(HOUR, creationTime) as Hour_dp
, DATEPART(QUARTER, creationTime) as Quarter_dp,
DATEPART(WEEK, creationTime) as Week_dp
from Sales.Orders


SELECT orderId,
DATENAME(MONTH, creationTime) as Month_dn,
DATENAME(YEAR, creationTime) as Year_dn,
DATENAME(DAY, creationTime) as Day_dn,
DATENAME(WEEKDAY, creationTime) as Weekday_dn
from Sales.Orders


SELECT orderId,
DATETRUNC(YEAR, creationTime) as Year_dt,
DATETRUNC(DAY, creationTime) as Day_dt,
DATETRUNC(MONTH, creationTime) as Month_dt,
DATETRUNC(MINUTE, creationTime) as Minute_dt,
DATETRUNC(QUARTER, creationTime) as Quarter_dt,
DATETRUNC(WEEK, creationTime) as Week_dt
from Sales.Orders


SELECT
    DATETRUNC(MONTH, CreationTime) AS Creation,
    COUNT(*) AS OrderCount
FROM Sales.Orders
GROUP BY DATETRUNC(MONTH, CreationTime);



SELECT
    OrderID,
    CreationTime,
    EOMONTH(CreationTime) AS EndOfMonth,
    cast(DATETRUNC(MONTH, creationTime) AS DATE) AS StartOfMonth
FROM Sales.Orders;
