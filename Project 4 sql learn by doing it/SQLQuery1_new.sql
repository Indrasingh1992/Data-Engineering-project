---1. How to create new database
CREATE DATABASE EMPLOYEE;

--2. how to use database (go to specific database)
USE EMPLOYEE;
use master;

--3. how to delete database 
DROP DATABASE EMPLOYEE;

--USE master;
--IF EXISTS (SELECT * FROM sys.databases WHERE name = 'EMPLOYEE')
--BEGIN
--    ALTER DATABASE [EMPLOYEE] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
--    DROP DATABASE [EMPLOYEE];
--END


--4. how to create table
CREATE TABLE EMP (
ID INT,
NAME VARCHAR(30),
ADDRESS VARCHAR(30),
SALARY INT
)

---create and Insert statement


--5. create table and insert vales in the tables
create table employee (
id int,
name varchar(28),
address varchar(28),
salary int,
bonus float
)

-- INSERT 

INSERT INTO employee values(1,'manish','INDIA',10000,3.5)
INSERT INTO employee values(2,'RANI','INDIA',20000,3.5)

--6. how to read any tables
select * from employee

--7. create table and insert only few colmn values in the table

INSERT INTO employee values(3,'rahu',4.5)

insert into employee(id,name,bonus) values(3,'rahu',4.5)


--8. SQL constraints 

-- Definition:
--SQL constraints are rules applied to columns in a database table
--to enforce data integrity and ensure the accuracy and reliability of the data.
1. NOT NULL
Definition: Ensures that a column cannot have a NULL value.
2. UNIQUE
Definition: Ensures that all values in a column are unique across the table,
preventing duplicate entries.
3.PRIMARY KEY
Definition: A combination of NOT NULL and UNIQUE.
It uniquely identifies each row in a table and cannot accept NULL values.
4.FOREIGN KEY
Definition: Establishes a relationship between two tables by ensuring that a value in one table
matches a value in another, maintaining referential integrity.
5.CHECK
Definition: Ensures that all values in a column satisfy a specific condition, 
helping to enforce business rules.
6.DEFAULT
definition: Provides a default value for a column when no value is specified during the insertion of a new record.


-- 9. Not Null and UNIQUE CONSTRAINTS

-- table -  email cant be blank 
-- table -  Unique phone nmber 

create table emp1(
id int,
email varchar(128) NOT NULL,
phone varchar(28) UNIQUE
)

SELECT * FROM EMP1

INSERT INTO EMP1 VALUES(1,'ABC@GMAIL.COM',123)

INSERT INTO EMP1(ID,PHONE) VALUES(2,234)  -- give error for email column does not allow nulls.

INSERT INTO EMP1(ID,EMAIL,PHONE) VALUES(2,'DEF',234)
INSERT INTO EMP1(ID,EMAIL,PHONE) VALUES(3,'DEF',234)  --phone number column is a unique key. Cannot insert duplicate key in object 'dbo.emp1'. The duplicate key value is (234).





--10. check and default constraints 

--1 . create table emp ehere age column should contain value only greater than 18 
--2.  create table emp where country value should automatic take default INDIA.

CREATE TABLE EMP2 (
ID INT,
AGE INT CHECK (AGE>18) ,
COUNTRY VARCHAR(28) DEFAULT 'india'
);

insert into emp2 values(1,17,'india');  --The INSERT statement conflicted with the CHECK constraint. The conflict occurred in database "EMPLOYEE", table "dbo.EMP2", column 'AGE'.

insert into emp2 values(1,19,'india');
select * from emp2;

insert into emp2 (id,age) values(2,20)



--WITH CTE AS (
--    SELECT *,
--           ROW_NUMBER() OVER (PARTITION BY ID, AGE, COUNTRY ORDER BY (SELECT NULL)) AS rn
--    FROM emp2
--)
--DELETE FROM CTE WHERE rn > 1;



---11. PRIMARY KEY & UNIQUE KEY

-- PRIMARY KEY - 
-- A PRIMARY KEY is a column (or a set of columns) that uniquely identifies each row in a table.
-- No two rows can have the same primary key value, and it can’t be NULL.

-- IT DOESNT CONTAIN DUPLICATE VALUE   
-- IT CANT BE NULL 
-- WE CAN HAVE ONLY 1 PRIMARY KEY IN THE TABLE

-- DIFFERNCE BETWEEN UNIQUE KEY VS PRIMARY KEY 


CREATE TABLE EMP3 (
ID INT PRIMARY KEY,
EMAIL VARCHAR(28)
)

INSERT INTO EMP3 VALUES(1,'ABC')
INSERT INTO EMP3 VALUES(1,'DEF')  -- Violation of PRIMARY KEY constraint 'PK__EMP3__3214EC278E2C1446'. Cannot insert duplicate key in object 'dbo.EMP3'. The duplicate key value is (1).

INSERT INTO EMP3 VALUES(NULL,'DEF') --Because of primary key Cannot insert the value NULL into column 'ID', table 'EMPLOYEE.dbo.EMP3'; column does not allow nulls. INSERT fails.

select * from EMP3




--12. FOREIGN KEY CONSTRAINTS


CREATE TABLE DEPARTMENT1 (
DEPT_ID INT PRIMARY KEY,
DEPT_NAME VARCHAR(28)
)

CREATE TABLE STUDENT1(
ID INT PRIMARY KEY,
NAME VARCHAR(28),
DEPT_ID INT
FOREIGN KEY (DEPT_ID) REFERENCES DEPARTMENT1(DEPT_ID)
)




--FILTERING AND SORTING 

CREATE TABLE EMPL (
ID INT,
NAME VARCHAR (17),
SALARY INT,
LOCATION VARCHAR(17),
DEPT VARCHAR(17)
)

INSERT INTO EMPL VALUES (1,'MANISH', 900000,'INDIA','SDE')
INSERT INTO EMPL VALUES (2,'RANI', 1000000,'INDIA','SDE2')
INSERT INTO EMPL (ID ,NAME,SALARY,DEPT) VALUES (3,'RAJ', 700000,'SALES')
INSERT INTO EMPL(ID ,NAME,SALARY,DEPT) VALUES (4,'RAHUL', 600000,'HR')

SELECT * FROM EMPL


-- 13. HOW TO FILTER DATA IN SQL 
--FILTER A DATA FROM EMPLOYEE TABLE WHERE SALARY GRETAER THAN 700000
 SELECT * FROM EMPL WHERE SALARY>700000

--FILTER A DATA FROM EMPLOYEE WHERE LOCATION IS NULL
SELECT * FROM EMPL WHERE LOCATION IS NULL

--14 HOW TO SORT THE DATA IN SQL 
--SORT THE DATA BASED ON HIGHEST TO LOWEST SALARY
SELECT * FROM EMPL ORDER BY SALARY DESC

--SORT THE DATA BASED ON LOWEST TO HIGHEST SALARY
SELECT * FROM EMPL ORDER BY SALARY ASC


SELECT * FROM EMPL 
-- 14. DELETE VS DROP VS TRUNCATE IN SQL 

--DELETE - WHEN YOU WANT TO DELETE ANY SPECIOFIC ROW OR COMPLETE DATA FROM TABLE 
DELETE FROM EMPL WHERE ID=4
DELETE FROM EMPL   -- it will delete all the rows as there is not contition present

--TRUNCATE  
--WE USE WHENEVER WE HAVE TO DELETE COMPLETE DATA FROM TABLE 
TRUNCATE TABLE EMPL 

-- DROP 
DROP TABLE EMPL  -- As whole table got deleted after dropping of table there after select statement we got error as  invalid object name






-- 15. update in sql

CREATE TABLE EMPL (
ID INT,
NAME VARCHAR (17),
SALARY INT,
LOCATION VARCHAR(17),
DEPT VARCHAR(17)
)

INSERT INTO EMPL VALUES (1,'MANISH', 900000,'INDIA','SDE')
INSERT INTO EMPL VALUES (2,'RANI', 1000000,'INDIA','SDE2')
INSERT INTO EMPL (ID ,NAME,SALARY,DEPT) VALUES (3,'RAJ', 700000,'SALES')
INSERT INTO EMPL(ID ,NAME,SALARY,DEPT) VALUES (4,'RAHUL', 600000,'HR')
INSERT INTO EMPL(ID ,NAME,DEPT) VALUES (5,'RAj','HR')
INSERT INTO EMPL(ID ,NAME,DEPT) VALUES (6,'shyam','HR')

select * from empl

 

-- update emplyoee location as INDIA where location is null
UPDATE empl set location ='INDIA'
where location is null
select * from empl

--- update salary as 100000 for shyam 
update empl set salary=100000
where name='shyam'
select * from empl

--  update salary as 100000 if ist null 
update empl set salary=100000
where salary is null
select * from empl





--16. AND /OR/NOT IN IN SQL 

--AND - IF WE HAVE 2 CONDITION THEN BOTH CONDITION IF ITS MATCHING THEN WE WILL GET RESULT
--OR/IN--IFWE HAVE 2 CONDITION AND IF ANY OF THE CONDITION SATISFY THEN WE WILL GET RESULT
--NOT -- IF CONDITION WE ARE CHECKING WITH NOT CLAUSE 

--AND
-- GIVE THE EMPLOYEE DETAILS WHO ARE FROM HR DEPARTMENT AND ALSO SALARY IS 100000
select * from empl
SELECT * FROM EMPL WHERE DEPT ='HR' AND SALARY=100000

-- GIVE THE EMPLOYEE DETAILS WHO ARE FROM HR DEPARTMENT OR FROM LOCATION USA
select * from empl
SELECT * FROM EMPL WHERE DEPT='HR' OR LOCATION='USA'

--OR/IN

-- GIVE THE EMPLOYEE DETAILS OF EMPLOYEE WHERE ID IS 2,3,4,6
select * from empl
SELECT * FROM EMPL WHERE ID=2OR ID =3 OR ID=4 OR ID=6
SELECT * FROM EMPL WHERE ID IN (2,4,3,6)

--NOT IN 
-- GIVE THE EMPLOYEE DETAILS WHERE ID NOT (2,3,4,5,6)
SELECT * FROM EMPL WHERE ID NOT IN (2,3,4,5,6)




--17.aggregate in sql 
DROP TABLE EMPL
CREATE TABLE EMPL (
ID INT,
NAME VARCHAR (17),
SALARY INT,
LOCATION VARCHAR(17),
DEPT VARCHAR(17)
)

INSERT INTO EMPL VALUES (1,'MANISH', 900000,'INDIA','SDE')
INSERT INTO EMPL VALUES (2,'RANI', 1000000,'INDIA','SDE2')
INSERT INTO EMPL (ID ,NAME,SALARY,DEPT) VALUES (3,'RAJ', 700000,'SALES')
INSERT INTO EMPL(ID ,NAME,SALARY,DEPT) VALUES (4,'RAHUL', 600000,'HR')

select * from empl

--17. how to find maximum salary of employee from empl table 
select max(salary) from empl
--18. how to find minimum salary of employee
select min(salary) from empl
--19. total salary of employee 
select sum(salary) from empl
--20. find the total n8umber of employee we have 
select count(id) from empl
-- 21. find avg salary of employee
select avg(salary) from empl

--update empl set location ='USA' where location is null



-- group by in sql 
-- we use the GROUP BY clause to group rows based on the value of columns.
--find number of customers in each country".

select * from empl
update empl set location ='USA' where location is null

--22. find the number of employee present in each location 
select count(id) , location  from empl group by location

--23. find the highest salary employee from each location 
select max(salary) ,location from empl group by location

--24. find the lowest earning salary from each location 
select min(salary),location from empl group by location

--25. find the total salary from each location 
select sum(salary),location from empl group by location


--25. find the total salary from each department 
select sum(salary),dept from empl group by dept








---------LIKE OPERATOR----------
-- the like operator used in where clause  search for specified  pattern in a column

select * from retail_dataset;

BULK INSERT retail_dataset
FROM 'D:\Data-Engineering-project\Project 4 sql learn by doing it\online-retail-dataset.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,            -- Skip header row
    FIELDTERMINATOR = ',',   -- Specify delimiter
    ROWTERMINATOR = '0x0A',    -- Specify end of row
    TABLOCK                 -- Optimizes performance
);

--  query 1   search all the records where description start -white
SELECT * FROM RETAIL_DATASET WHERE DESCRIPTION LIKE 'WHITE%'

--  query 2   search all the records where description end -holder
SELECT * FROM RETAIL_DATASET WHERE DESCRIPTION LIKE '%HOLDER'


--  query 3   search all the records where description is 'Playhouse'
SELECT * FROM RETAIL_DATASET WHERE DESCRIPTION LIKE '%Playhouse%'




--QUERY 26. 
-- HAVING CLASE IN SQL 

USE EMPLOYEE;

CREATE TABLE CUSTOMERS (
   ID INT NOT NULL,
   NAME VARCHAR (20) NOT NULL,
   AGE INT NOT NULL,
   ADDRESS CHAR (25),
   SALARY DECIMAL (18, 2),
   PRIMARY KEY (ID)
);



INSERT INTO CUSTOMERS VALUES
(1, 'Ramesh', 32, 'Ahmedabad', 2000.00),
(2, 'Khilan', 25, 'Delhi', 1500.00),
(3, 'Kaushik', 23, 'Kota', 2000.00),
(4, 'Chaitali', 25, 'Mumbai', 6500.00),
(5, 'Hardik', 27, 'Bhopal', 8500.00),
(6, 'Komal', 22, 'Hyderabad', 4500.00),
(7, 'Muffy', 24, 'Indore', 10000.00);


SELECT * FROM CUSTOMERS;

 
--------------------------
-- The SQL HAVING clause is similar to the WHERE clause;
-- are used to filter rows in a table based on specified criteria

--  HAVING clause was added to SQL because the WHERE keyword cannot be used with aggregate functions
SELECT SUM(SALARY) FROM CUSTOMERS WHERE SUM(SALARY)>10000


-- FIND THE ADDRESS OF CUSTOMER WHERE TOTAL SALARY IS GREATER THAN 5000
SELECT SUM(SALARY),ADDRESS FROM CUSTOMERS GROUP BY ADDRESS HAVING SUM(SALARY)>5000

-- FIND THE ADDRESS OF CUSTOMER WHERE AVERAGE SALARY GREATER THAT 5000
SELECT AVG(SALARY),ADDRESS FROM CUSTOMERS GROUP BY ADDRESS HAVING AVG(SALARY)>5000





-- TOP/Limit in sql
use EMPLOYEE;

CREATE TABLE employees (
ID INT,
NAME VARCHAR (17),
SALARY INT,
LOCATION VARCHAR(17),
POST VARCHAR(17)
);


INSERT INTO employees VALUES (1,'SAI', 900000,'UK','SSE');
INSERT INTO employees VALUES (2,'SREE', 1000000,'INDIA','SE');
INSERT INTO employees VALUES (3,'MANISH', 800000,'INDIA','SDE');
INSERT INTO employees VALUES (4,'AKKI', 700000,'USA','SE');
INSERT INTO employees VALUES (5,'ANI', 1000000,'USA','SE');
INSERT INTO employees VALUES (6,'RAHUL', 20000,'INDIA','SDE');
INSERT INTO employees VALUES (7,'NEGHA', 30000,'UK','SSE');
INSERT INTO employees (ID ,NAME,SALARY,LOCATION) VALUES (8,'AA', 10000,'UK');

select * from employees;

--- query 1. How to get only  first 3 record from the table
SELECT TOP 3 * FROM EMPLOYEES;
select * from employees limit 3;  -- used in MYSQL


--- query 2. how to get 50% records from table
SELECT TOP 50 PERCENT * FROM EMPLOYEES;

--- query 3. how to get top 2 highest salary of employee
select top 2 * from employees order by salary desc; 

--- query 4. how to get bottom 2 salary of employee
select top 2 * from employees order by salary asc;

--- query 5. how to get top 2 records from india based on salary
select top 2 * from employees where location='INDIA' order by salary desc;





-- Unique records (distict)

DROP TABLE IF EXISTS EMPL;

CREATE TABLE EMPL (
    ID INT,
    NAME VARCHAR(17),
    SALARY INT,
    LOCATION VARCHAR(17),
    DEPT VARCHAR(17)
);

-- Insert data
INSERT INTO EMPL VALUES (1, 'MANISH', 900000, 'INDIA', 'SDE');
INSERT INTO EMPL VALUES (2, 'RANI', 1000000, 'INDIA', 'SDE2');
INSERT INTO EMPL VALUES (3, 'RAJ', 700000, 'USA', 'SALES');
INSERT INTO EMPL VALUES (4, 'RAHUL', 600000, 'USA', 'HR');
INSERT INTO EMPL VALUES (1, 'MANISH', 900000, 'INDIA', 'SDE'); -- Duplicate Entry
INSERT INTO EMPL VALUES (2, 'RANI', 1000000, 'INDIA', 'SDE2'); -- Duplicate Entry


select * from empl 


select distinct id,name from empl

select distinct name,id,salary,location,dept from empl




-- Coalesce in SQL

use employee;
CREATE TABLE employees1 (
    employee_id INT PRIMARY KEY,
    name VARCHAR(50),
    phone VARCHAR(15),
    email VARCHAR(50),
    alternate_email VARCHAR(50),
    salary DECIMAL(10, 2),
    bonus DECIMAL(10, 2)
);
INSERT INTO employees1 (employee_id, name, phone, email, alternate_email, salary, bonus) 
VALUES 
(1, 'Alice', NULL, 'alice@example.com', 'alice.alt@example.com', 50000.00, 1000.00),
(2, 'Bob', '123-456-7890', NULL, 'bob.alt@example.com', 45000.00, NULL),
(3, 'Charlie', NULL, NULL, NULL, 48000.00, NULL);


-- COALESCE IN SQL 

select * from employees1

-- Replace NULL Phone Numbers with unknown
SELECT NAME, COALESCE(PHONE,'Unknown') from employees1

-- Select the First Available Email: (first not null values)
select name, coalesce(email,alternate_email,'Unknwon') from employees1

---Calculate Total Compensation (Salary + Bonus):

select salary + coalesce(bonus,0) from employees1









-- joining in sql --
✅ What is a JOIN in SQL?
The JOIN clause is used to combine  two or more tables based on a common column
between them. This helps retrieve related data efficiently. --

-- Types of SQL JOINs

--1. INNER JOIN: Returns only matching rows from both tables.
--2.LEFT JOIN (or LEFT OUTER JOIN): Returns all rows from the left table 
and the matching rows from the right table.
--3. RIGHT JOIN (or RIGHT OUTER JOIN): Returns all rows from the right table and the matching rows
from the left table.
--4. FULL OUTER JOIN: Returns all rows when there is a match in either table,
with NULL for non-matching rows.



CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(50),
    department_id INT,
    salary DECIMAL(10, 2)
);

-- Create departments table
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50)
);
drop table employees

-- Insert data into employees table
INSERT INTO employees (employee_id, name, department_id, salary) VALUES
(1, 'Alice', 1, 50000.00),
(2, 'Bob', 2, 45000.00),
(3, 'Charlie', 4, 48000.00),
(4, 'David', 5, 52000.00);  -- No department assigned

-- Insert data into departments table
INSERT INTO departments (department_id, department_name) VALUES
(1, 'Human Resources'),
(2, 'Finance'),
(3, 'IT');


select * from employees
select * from departments


--INNER JOIN
select * from employees inner join departments on employees.department_id=departments.department_id
--left join 
select * from employees left join departments on employees.department_id=departments.department_id
--right join 
select * from employees right join departments on employees.department_id=departments.department_id
--full join 
select * from employees full join departments on employees.department_id=departments.department_id




---union vs union all

CREATE TABLE employees_2023 (
    employee_id INT,
    name VARCHAR(50),
    department_id INT
);
INSERT INTO employees_2023 (employee_id, name, department_id) 
VALUES 
(1, 'Alice', 101),
(2, 'Bob', 102),
(3, 'Charlie', 103);

CREATE TABLE employees_2024 (
    employee_id INT,
    name VARCHAR(50),
    department_id INT
);

INSERT INTO employees_2024 (employee_id, name, department_id) 
VALUES 
(2, 'Bob', 102),
(3, 'Charlie', 103),
(4, 'David', 104);


--UNION VS UNION ALL

Key Differences between UNION and UNION ALL
UNION: Removes duplicate rows from the result set.
UNION ALL: Keeps all rows, including duplicates.


SELECT * FROM employees_2023

SELECT * FROM employees_2024

--UNION 
SELECT * FROM employees_2023
UNION 
SELECT * FROM employees_2024

--UNION ALL

SELECT * FROM employees_2023
UNION ALL
SELECT * FROM employees_2024


SELECT * FROM employees_2023

SELECT * FROM employees_2024


SELECT EMPLOYEE_ID,NAME FROM EMPLOYEES_2023
UNION 
SELECT NAME,department_id FROM employees_2024



---ALTER IN SQL 
--The ALTER statement is used to change the structure of a table after it has been created. 
--This helps when you need to update the schema without losing existing data.

SELECT * FROM EMPLOYEES_2023

-- ADD NEW COLUMN SALARY INT THE EXISTING TABLE WITHOUT DROPPING IT
ALTER TABLE EMPLOYEES_2023 ADD SALARY INT 

-- CHANGE THE SCHEMA OF SALARY COLUMN FROM INT TO VARCHAR 
ALTER TABLE EMPLOYEES_2023 ALTER COLUMN SALARY INT

-- DELETE COLUMN SALARY FROM THE EXISTING TABLE.
ALTER TABLE EMPLOYEES_2023 DROP COLUMN SALARY


--- WINDOWS FUNCTION 
-- OVER CLAUSE AND PARTITION BY 

-- OVER 
--The OVER clause in SQL is used with window functions (like SUM, COUNT, ROW_NUMBER, etc.) 
--to perform calculations across a specific set of rows related to the current row.

--The OVER clause allows for partitioning and ordering rows
--without collapsing them into a single result like aggregate


--In SQL, window functions perform calculations across a specified range of rows 
--that are related to the current row, without collapsing 
--them into a single row as aggregate functions do. 

--This allows for advanced analytical queries, such as ranking, running totals, moving averages, and more.

--Types of Window Functions
-- Aggregate Functions: SUM(), AVG(), MIN(), MAX(), COUNT()
-- Ranking Functions: ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE()
-- Value Functions: LEAD(), LAG(), FIRST_VALUE(), LAST_VALUE()

--TABLE 


CREATE TABLE sales (
    sales_id INT,
    salesperson VARCHAR(50),
    region VARCHAR(50),
    amount DECIMAL(10, 2)
);

INSERT INTO sales (sales_id, salesperson, region, amount) VALUES 
(1, 'Alice', 'North', 500),
(2, 'Bob', 'South', 300),
(3, 'Alice', 'North', 200),
(4, 'Charlie', 'South', 700),
(5, 'Bob', 'South', 400),
(6, 'Alice', 'North', 800);

---- Aggregate Functions: SUM(), AVG(), MIN(), MAX(), COUNT()

SELECT * FROM SALES

-- FIND OUT THE TOTAL AMOUNT BASED ON EACH REGION 
SELECT SUM(AMOUNT),REGION FROM SALES GROUP BY REGION 

SELECT *,SUM(AMOUNT) OVER(PARTITION BY REGION) FROM SALES


-- FIND OUT THE MIN AMOUNT BASED ON EACH REGION
SELECT *,MIN(AMOUNT)OVER(PARTITION BY REGION ) FROM SALES

-- FIND OUT THE MAX AMOUNT BASED ON EACH REGION
SELECT *,MAX(AMOUNT)OVER(PARTITION BY REGION ) FROM SALES

-- FIND OUT THE AVG AMOUNT BASED ON EACH REGION
SELECT *,AVG(AMOUNT)OVER(PARTITION BY REGION ) FROM SALES


SELECT *,AVG(AMOUNT)OVER(PARTITION BY REGION ) 'AVG_SALES',
MIN(AMOUNT)OVER(PARTITION BY REGION ) 'MIN_SALES',
MAX(AMOUNT)OVER(PARTITION BY REGION ) 'MAX_SALES'
FROM SALES

---------------------------------
-- windows function 
--ROW NUMBER , RANK , DENSE RANK 

--assign ranks or numbers to rows based on the order defined in the 
--ORDER BY clause of the OVER() window function

CREATE TABLE employees1 (
    employee_id INT,
    name VARCHAR(50),
    department VARCHAR(50),
    salary INT
);

INSERT INTO employees1 (employee_id, name, department, salary) VALUES 
(1, 'Alice', 'Sales', 5000),
(2, 'Bob', 'Sales', 3000),
(3, 'Charlie', 'Sales', 3000),
(4, 'David', 'Sales', 2000),
(5, 'Eve', 'Sales', 2000);


SELECT * FROM EMPLOYEES1

--ROW_NUMBER(): Unique numbering for each row; no duplicates.
--RANK(): Same rank for ties, but gaps in ranking after ties.
-- DENSE_RANK(): Same rank for ties, but no gaps in ranking sequence.

SELECT * FROM EMPLOYEES1


SELECT *, ROW_NUMBER() OVER(ORDER BY SALARY DESC) AS RN FROM EMPLOYEES1

SELECT *, DENSE_RANK() OVER(ORDER BY SALARY DESC) AS RN FROM EMPLOYEES1

SELECT *, RANK() OVER(ORDER BY SALARY DESC) AS RN FROM EMPLOYEES1


SELECT *, ROW_NUMBER() OVER(ORDER BY SALARY DESC) AS RN,
DENSE_RANK() OVER(ORDER BY SALARY DESC) AS RN1,
RANK() OVER(ORDER BY SALARY DESC) AS RN3 FROM EMPLOYEES1

-----------

lead and lag

CREATE TABLE sales1 (
    transaction_id INT,
    salesperson VARCHAR(50),
    transaction_date DATE,
    amount DECIMAL(10, 2)
);

INSERT INTO sales1 (transaction_id, salesperson, transaction_date, amount) VALUES 
(1, 'Alice', '2023-01-01', 500.00),
(2, 'Alice', '2023-01-03', 300.00),
(3, 'Alice', '2023-01-05', 450.00),
(4, 'Bob', '2023-01-02', 200.00),
(5, 'Bob', '2023-01-06', 700.00),
(6, 'Bob', '2023-01-08', 300.00);



--lead and lag


--LEAD() and LAG() are window functions that allow you to access data 
--from the next row (LEAD()) or previous row (LAG()) in a result set 


select * from sales1



lag

select *,lag(amount,1,0) over(order by transaction_date ) prev_sal from sales1

select * ,lead(amount,1,0) over(order by transaction_date) next_Sal from sales1


select *,lag(amount,1,0) over(order by transaction_date ) prev_sal,
lead(amount,1,0) over(order by transaction_date) next_Sal from sales1






--CTE 


----A Common Table Expression (CTE) in SQL is a temporary result set that you can
--reference within a SELECT, INSERT, UPDATE, or DELETE statement.

--CTEs are often used to simplify complex queries,
--make queries more readable, and allow for recursive queries.

--CTEs are powerful for organizing SQL code, 
--especially in multi-step analyses or when calculating intermediate results.

--Reusable Logic: Once defined, you can refer to a CTE multiple times within the main query.


--SYNTAX 

WITH CTE AS(
SELECT * FROM EMPLOYEES
)
SELECT * FROM CTE


-- FIND THE 3RD HIGHEST SALARY FROM EMPLOYEE TABLE
WITH CTE AS(
SELECT * , ROW_NUMBER() OVER(ORDER BY SALARY DESC ) RN FROM EMPLOYEES
)
SELECT * FROM CTE WHERE RN =3

-- FIND THE SECOND LOWEST SALARY OF EMPLOYEE

SELECT * FROM EMPLOYEES

WITH MANISH AS (
SELECT * , ROW_NUMBER() OVER(ORDER BY SALARY ASC) RN FROM EMPLOYEES
)
SELECT * FROM MANISH WHERE RN=2



----VIEW IN SQL

A View in SQL is a virtual table created by a query.
views do not store data physically but instead 
store a query that generates the data on demand.



SELECT * FROM EMPLOYEES

CREATE VIEW EMP_INFO AS 
SELECT EMPLOYEE_ID,DEPARTMENT_ID,SALARY FROM EMPLOYEES


SELECT * FROM EMP_INFO

DROP VIEW EMP_INFO



--What is a Stored Procedure?

--1. A stored procedure is a prepared SQL code that you can save, so the code can be reused over and over again.

--2. if you have an SQL query that you write over and over again, save it as a stored procedure, and then just call it to execute it.

--3.You can also pass parameters to a stored procedure, so that the stored procedure can act based on the parameter value(s) that is passed.

--SYNTAX

CREATE PROCEDURE procedure_name
AS
sql_statement
GO;

EXEC procedure_name;


SELECT * FROM EMPLOYEES

-- STORE PROCEDRE EXAMPLE
CREATE PROCEDURE EMP_SP AS 
SELECT * FROM EMPLOYEES
GO;

EXEC EMP_SP


--Stored Procedure With One Parameter
CREATE PROCEDURE EMP_SP1 @DEPT_ID INT AS
SELECT * FROM EMPLOYEES WHERE DEPARTMENT_ID=@DEPT_ID

EXEC EMP_SP1 2

--Stored Procedure With Multiple Parameters

SELECT * FROM EMPLOYEES

CREATE PROCEDURE EMP_SP2 @DEPT_ID INT , @SALARY INT AS
SELECT * FROM EMPLOYEES WHERE DEPARTMENT_ID=@DEPT_ID AND SALARY >@SALARY

EXEC EMP_SP2 @DEPT_ID=1 , @SALARY=10000


------------------
SUB QUERY IN SQL 
----------------------

--A SQL subquery, also known as a nested query or inner query, 
-- a query within another SQL statement

--It allows you to use the result of one query as input in another query. 


SELECT * FROM employee



CREATE TABLE Departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50),
    location VARCHAR(50)
);

CREATE TABLE Employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(50),
    department_id INT,
    salary DECIMAL(10, 2),
    FOREIGN KEY (department_id) REFERENCES Departments(department_id)
);

INSERT INTO Departments (department_id, department_name, location)
VALUES 
(101, 'Sales', 'New York'),
(102, 'IT', 'Chicago'),
(103, 'HR', 'San Francisco');


INSERT INTO Employees (employee_id, employee_name, department_id, salary)
VALUES 
(1, 'Alice', 101, 5000),
(2, 'Bob', 102, 6000),
(3, 'Carol', 101, 5500),
(4, 'Dave', 103, 7000),
(5, 'Eve', 102, 6500);


SELECT * FROM Employees
SELECT * FROM DEPARTMENTS


--QUERY 1. Find employees who earn more than the average salary across all employees.
SELECT AVG(SALARY) FROM EMPLOYEES

SELECT * FROM EMPLOYEES WHERE SALARY > (SELECT AVG(SALARY) FROM EMPLOYEES)

--QUERY2. List employees who work in departments located in 'Chicago'.


SELECT department_id from departments where location='Chicago'

select * from employees where department_id in (SELECT department_id from departments where location='Chicago')


---- triggers in sql

A trigger in SQL is a set of instructions that automatically execute in response to specific events on a particular table or view. Triggers are typically used to enforce business rules, maintain audit trails, and automate tasks such as calculations or updates when changes are made to a database.

Trigger Types
BEFORE Trigger: Executes before an operation (INSERT, UPDATE, or DELETE) is completed.
AFTER Trigger: Executes after an operation is completed.
INSTEAD OF Trigger: Typically used on views to handle what happens instead of a default INSERT, UPDATE, or DELETE action.


CREATE TABLE Employee (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(50),
    department_id INT,
    salary DECIMAL(10, 2)
);


CREATE TABLE Backup_Employee (
    backup_id INT IDENTITY(1,1) PRIMARY KEY,
    employee_id INT,
    employee_name VARCHAR(50),
    department_id INT,
    salary DECIMAL(10, 2),
    deleted_at DATETIME DEFAULT GETDATE()
);



CREATE TRIGGER employee_delete
ON Employee
AFTER DELETE
AS
BEGIN
    INSERT INTO Backup_Employee (employee_id, employee_name, department_id, salary, deleted_at)
    SELECT 
        employee_id, 
        employee_name, 
        department_id, 
        salary, 
        GETDATE() 
    FROM DELETED;
END;





Testing the Trigger

INSERT INTO Employee(employee_id, employee_name, department_id, salary)
VALUES 
(1, 'Alice', 101, 5000),
(2, 'Bob', 102, 6000),
(3, 'Carol', 101, 5500);

select * from Employee
select * from Backup_Employee


DELETE FROM Employee WHERE employee_id = 3



SELECT * FROM Backup_Employee;















