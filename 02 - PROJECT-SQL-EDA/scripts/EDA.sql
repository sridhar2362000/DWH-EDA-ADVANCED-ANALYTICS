
/*************************DATA EXPLORATION******************************************/
SELECT * FROM INFORMATION_SCHEMA.TABLES

SELECT * FROM INFORMATION_SCHEMA.COLUMNS

SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dim_customers'

/**************************DIMENSION EXPLORATION********************************************************/

 SELECT DISTINCT country FROM gold.dim_customers

 SELECT DISTINCT category,subcategory FROM gold.dim_products

  SELECT DISTINCT category,subcategory,product_name FROM gold.dim_products

/****************************DATE EXPLORATION *******************************************************************/

SELECT MIN(order_date) as starting_year,
MAX(order_date)as ending_year,
DATEDIFF(YEAR,MIN(order_date), MAX(order_date)) as year_range,
DATEDIFF(MONTH,MIN(order_date), MAX(order_date)) as MONTH_range
from gold.fact_sales

SELECT MIN(birthdate) AS OLDEST_bdy,
MAX(birthdate) AS YOUNGEST_bdy,
DATEDIFF(YEAR,MIN(birthdate),GETDATE()) as old_cust_age,
DATEDIFF(YEAR,MAX(birthdate),GETDATE()) as young_cust_age
FROM gold.dim_customers

/***********************************MEASURE EXPLORATION*************************************************/
SELECT * FROM gold.fact_sales
SELECT SUM(sales_amount)AS TOTAL_SALES FROM gold.fact_sales;

SELECT SUM(quantity)AS TOTAL_QUANTITY_SALES FROM gold.fact_sales;

SELECT AVG(sales_amount)AS AVERAGE_PRICE FROM gold.fact_sales;

SELECT COUNT(order_number)AS TOTAL_ORDERS FROM gold.fact_sales;
SELECT COUNT(DISTINCT order_number)AS TOTAL_ORDERS FROM gold.fact_sales; --ACCURATE NO.ORDERS

SELECT COUNT(product_key)AS TOTAL_PRODUCT FROM gold.dim_products;
SELECT COUNT(DISTINCT product_key)AS TOTAL_PRODUCT FROM gold.dim_products;

SELECT COUNT(customer_key)AS TOTAL_CUSTOMERS FROM gold.dim_customers;
SELECT COUNT(DISTINCT customer_key)AS TOTAL_CUSTOMERS FROM gold.dim_customers;
SELECT COUNT(DISTINCT customer_key)AS TOTAL_CUSTOMERS FROM gold.fact_sales;

SELECT COUNT(DISTINCT customer_key)AS TOTAL_CUSTOMERS FROM gold.dim_customers;
SELECT COUNT(DISTINCT customer_key)AS TOTAL_CUSTOMERS FROM gold.dim_customers;


SELECT 'TOTAL_SALES' AS MEASURE_NAME, SUM(sales_amount)AS MEASURE_VALUE FROM gold.fact_sales
UNION ALL
SELECT 'TOTAL_QUANTITY' AS MEASURE_NAME,SUM(quantity)AS MEASURE_VALUE FROM gold.fact_sales
UNION ALL
SELECT 'TOTAL_PRICE' AS MEASURE_NAME,AVG(sales_amount)AS MEASURE_VALUE  FROM gold.fact_sales
UNION ALL
SELECT 'TOTAL_NO.ORDERS' AS MEASURE_NAME,COUNT(DISTINCT order_number)AS MEASURE_VALUE FROM gold.fact_sales --ACCURATE NO.ORDERS
UNION ALL
SELECT 'TOTAL_NO.ORDERS' AS MEASURE_NAME,COUNT(DISTINCT product_key)AS MEASURE_VALUE FROM gold.dim_products
UNION ALL
SELECT 'TOTAL_NO.ORDERS' AS MEASURE_NAME,COUNT(DISTINCT customer_key)AS MEASURE_VALUE FROM gold.dim_customers;


/********************************************MAGNITUDE ANALYSIS********************************************/
SELECT country, COUNT(customer_key)AS TOTAL_CUSTOMERS FROM gold.dim_customers
GROUP BY country
ORDER BY TOTAL_CUSTOMERS DESC

SELECT gender, COUNT(customer_key)AS TOTAL_CUSTOMERS FROM gold.dim_customers
GROUP BY gender
ORDER BY TOTAL_CUSTOMERS DESC

SELECT category, COUNT(product_key)AS TOTAL_PRODUCTS FROM gold.dim_products
GROUP BY category
ORDER BY TOTAL_PRODUCTS DESC

SELECT category, AVG(cost)AS AVG_COSTS FROM gold.dim_products
GROUP BY category
ORDER BY AVG_COSTS DESC


SELECT P.category, SUM(F.sales_amount)
FROM gold.fact_sales F
LEFT JOIN gold.dim_products P 
ON P.product_key = F.product_key
GROUP BY P.category;

SELECT 
c.customer_key,
c.first_name,
c.last_name
FROM gold.fact_sales F
LEFT JOIN gold.dim_customers C 
ON F.customer_key = C.customer_key
GROUP BY c.customer_key,
c.first_name,
c.last_name;


SELECT 
c.country,
SUM(f.quantity) as total_items_sold
FROM gold.fact_sales F
LEFT JOIN gold.dim_customers C 
ON F.customer_key = C.customer_key
GROUP BY c.country

/********************************************RANKING DIMENSION**************************************************/
SELECT TOP 5 P.product_name, SUM(F.sales_amount) AS TOTAL_REVENUE
FROM gold.fact_sales F
LEFT JOIN gold.dim_products P 
ON P.product_key = F.product_key
GROUP BY P.product_name
ORDER BY TOTAL_REVENUE DESC;



SELECT * FROM
(SELECT P.product_name, SUM(F.sales_amount) AS TOTAL_REVENUE,
ROW_NUMBER() OVER (ORDER BY SUM(F.sales_amount) DESC) AS RANK_PRODUCT
FROM gold.fact_sales F
LEFT JOIN gold.dim_products P 
ON P.product_key = F.product_key
GROUP BY P.product_name)t
WHERE RANK_PRODUCT <= 5

SELECT TOP 5 P.product_name, SUM(F.sales_amount) AS TOTAL_REVENUE
FROM gold.fact_sales F
LEFT JOIN gold.dim_products P 
ON P.product_key = F.product_key
GROUP BY P.product_name
ORDER BY TOTAL_REVENUE ASC;


SELECT TOP 10
c.customer_key,
c.first_name,
c.last_name,
SUM(F.sales_amount) AS TOTAL_REVENUE
FROM gold.fact_sales F
LEFT JOIN gold.dim_customers C 
ON F.customer_key = C.customer_key
GROUP BY c.customer_key,
c.first_name,
c.last_name
ORDER BY TOTAL_REVENUE DESC;

SELECT TOP 3
c.customer_key,
c.first_name,
c.last_name,
COUNT(DISTINCT F.order_number) AS LOWEST_ORDERS
FROM gold.fact_sales F
LEFT JOIN gold.dim_customers C 
ON F.customer_key = C.customer_key
GROUP BY c.customer_key,
c.first_name,
c.last_name
ORDER BY LOWEST_ORDERS ASC;
