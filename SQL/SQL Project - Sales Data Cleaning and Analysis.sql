/*
SQL Project_Sales Data Cleaning & Analysis
Data source: https://www.kaggle.com/datasets/kyanyoga/sample-sales-data/data
Created by: Maggie Huang
Created time: 2024/07/15
Last modified: 2024/07/24
*/


/* PART 1. IMPORT DATA FROM CSV */

/*
Create the base table `sales_data` to store imported data from the csv file.
This table will later be dropped once new tables are created with cleaned data.
*/

-- Create the `sales_data` table
DROP TABLE IF EXISTS sales_data;
CREATE TABLE sales_data (
	order_number smallint,
	quantity_ordered smallint,
	price_each numeric,
	sales numeric,
	order_line_number smallint,
	deal_size varchar(10),
	status varchar(10),
	order_date varchar(20),
	date_quarter smallint,
	date_month smallint,
	date_year smallint,
	product_code varchar(10),
	product_line varchar(20),
	msrp smallint,
	customer_name varchar(50),
	phone varchar(20),
	address_line1 varchar(50),
	address_line2 varchar(50),
	city varchar(20),
	state varchar(20),
	postal_code varchar(10),
	country varchar(20),
	territory varchar(5),
	contact_lastname varchar(20),
	contact_firstname varchar(20)
);

-- Copy data from the csv file into the `sales_data` table
COPY sales_data
FROM 'C:\Sales Data Sample.csv'
DELIMITER ','
ENCODING 'Latin1'
CSV HEADER;

-- Verify the imported data
SELECT *
FROM sales_data;
-- Expected result: 2823 rows in total


/* PART 2. DATA PREPARATION & CLEANING */

/* 2-1: Create the `customers` table containing customer-related into and clean the data */

-- Verify if every distinct <customer_name> only matches one set of customer info
SELECT DISTINCT customer_name
FROM sales_data;
-- Returns 92 rows

SELECT DISTINCT
	customer_name,
	phone,
	address_line1,
	address_line2,
	city,
	state,
	postal_code,
	country,
	territory,
	contact_lastname,
	contact_firstname
FROM sales_data;
-- Returns 92 rows

-- Create the `customers` table
DROP TABLE IF EXISTS customers;
CREATE TABLE customers AS
WITH customer_info AS (
	SELECT DISTINCT
		customer_name,
		phone,
		address_line1,
		address_line2,
		city,
		state,
		postal_code,
		country,
		territory,
		contact_lastname,
		contact_firstname
	FROM sales_data
)
SELECT
	ROW_NUMBER() OVER() AS customer_id,
	*
FROM customer_info;

-- Examine the <country>, <territory> pairs
SELECT DISTINCT
	country,
	territory
FROM customers;
-- <territory> 'Japan' for <country> Singapore, Japan and Philippines need to be replaced with 'APAC'

-- Update the <territory> values
UPDATE customers
SET territory = 'APAC'
WHERE country IN ('Singapore', 'Philippines', 'Japan');

-- Examine null values for each column of the `customers` table
SELECT
	SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS customer_id_null,
	SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS customer_name_null,
	SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) AS phone_null,
	SUM(CASE WHEN address_line1 IS NULL THEN 1 ELSE 0 END) AS address_line1_null,
	SUM(CASE WHEN address_line2 IS NULL THEN 1 ELSE 0 END) AS address_line2_null,
	SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS city_null,
	SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS state_null,
	SUM(CASE WHEN postal_code IS NULL THEN 1 ELSE 0 END) AS postal_code_null,
	SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS country_null,
	SUM(CASE WHEN territory IS NULL THEN 1 ELSE 0 END) AS territory_null,
	SUM(CASE WHEN contact_lastname IS NULL THEN 1 ELSE 0 END) AS contact_lastname_null,
	SUM(CASE WHEN contact_firstname IS NULL THEN 1 ELSE 0 END) AS contact_firstname_null
FROM customers;
-- null values in address_line2, state, and postal_code are valid and do not affect further analysis

SELECT * FROM customers;
-- 92 customers in total

/* 2-2: Create the `products` table containing product-related info */

-- Verify if every distinct <product_code> only matches one set of product info
SELECT DISTINCT product_code
FROM sales_data;
-- Returns 109 rows

SELECT DISTINCT
	product_code,
	product_line,
	msrp
FROM sales_data;
-- Returns 109 rows

-- Create the `products` table
DROP TABLE IF EXISTS products;
CREATE TABLE products AS
SELECT DISTINCT
	product_code,
	product_line,
	msrp
FROM sales_data;

-- Examine null values for each column of the `products` table
SELECT
	SUM(CASE WHEN product_code IS NULL THEN 1 ELSE 0 END) AS product_code_null,
	SUM(CASE WHEN product_line IS NULL THEN 1 ELSE 0 END) AS product_line_null,
	SUM(CASE WHEN msrp IS NULL THEN 1 ELSE 0 END) AS msrp_null
FROM products;
-- no null values

SELECT * FROM products;
-- 109 products in total

/* 2-3: Create `order_details` table containing order line details */

-- Examine if there are duplicated order details
SELECT DISTINCT
	order_number,
	product_code,
	quantity_ordered,
	price_each,
	sales,
	order_line_number,
	deal_size
FROM sales_data;
-- Returns 2823 rows

-- Create `order_details` table
DROP TABLE IF EXISTS order_details;
CREATE TABLE order_details AS
SELECT DISTINCT
	order_number,
	product_code,
	quantity_ordered,
	price_each,
	sales,
	order_line_number,
	deal_size
FROM sales_data;

-- Examine null values for each column of the `order_details` table
SELECT
	SUM(CASE WHEN order_number IS NULL THEN 1 ELSE 0 END) AS order_number_null,
	SUM(CASE WHEN product_code IS NULL THEN 1 ELSE 0 END) AS product_code_null,
	SUM(CASE WHEN quantity_ordered IS NULL THEN 1 ELSE 0 END) AS quantity_ordered_null,
	SUM(CASE WHEN price_each IS NULL THEN 1 ELSE 0 END) AS price_each_null,
	SUM(CASE WHEN sales IS NULL THEN 1 ELSE 0 END) AS sales_null,
	SUM(CASE WHEN order_line_number IS NULL THEN 1 ELSE 0 END) AS order_line_number_null,
	SUM(CASE WHEN deal_size IS NULL THEN 1 ELSE 0 END) AS deal_size_null
FROM order_details;

SELECT * FROM order_details;
-- 2823 order data entries in total

/* 2-4: Create `orders` table containing order-related info and clean the data */

-- Create the `orders` table
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
	order_number smallint,
	order_date date,
	customer_id smallint,
	status varchar(10)
);

-- Examine <order_date> values
SELECT DISTINCT order_date
FROM sales_data;
-- Returns 252 rows
-- Dates need to be reformatted and standardised

-- Add a new column <new_order_date> to store reformmatted dates
ALTER TABLE sales_data
ADD COLUMN new_order_date date;

-- Extract year, month and day as integers to form standardised dates
UPDATE sales_data
SET new_order_date = MAKE_DATE(
	CAST(SUBSTRING(order_date, '/([0-9]+) ') AS integer),
	CAST(SUBSTRING(order_date, '([0-9]+)/') AS integer),
	CAST(SUBSTRING(order_date, '/([0-9]+)/') AS integer)
);

-- Insert order data from `sales_data` and `customers` tables to `orders` table
INSERT INTO orders
SELECT DISTINCT
	s.order_number,
	s.new_order_date,
	c.customer_id,
	s.status
FROM sales_data AS s
JOIN customers AS c
	ON s.customer_name = c.customer_name
ORDER BY order_number;

-- Examine null values for each column of the `orders` table
SELECT
	SUM(CASE WHEN order_number IS NULL THEN 1 ELSE 0 END) AS order_number_null,
	SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS order_date_null,
	SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS customer_id_null,
	SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS status_null
FROM orders;

SELECT * FROM orders;
-- 307 orders in total

/* 2-5: Drop sales_data table */
DROP TABLE IF EXISTS sales_data;


/* PART 3. DATA ANALYSIS */

-- Q1: Calculate total sales for each month and find the peak season
WITH this_year AS (
	SELECT
		DATE_PART('year', o.order_date) AS year,
		DATE_PART('month', o.order_date) AS month,
		SUM(od.sales) AS monthly_sales
	FROM orders AS o
	JOIN order_details AS od
		ON o.order_number = od.order_number
	GROUP BY year, month
	ORDER BY year, month
)
SELECT
	year,
	month,
	monthly_sales,
	SUM(monthly_sales) OVER (PARTITION BY year) AS annual_sales,
	ROUND(monthly_sales / SUM(monthly_sales) OVER (PARTITION BY year) * 100, 2) AS "proportion (%)"
FROM this_year;
/* October and November are the peak season, with October sales generally accounts for more than 10 percent
and November sales accounts for more than 20 percent of the annual sales. */

-- Q2. Compare monthly sales over different years
WITH this_year AS (
	SELECT
		DATE_PART('year', o.order_date) AS year,
		DATE_PART('month', o.order_date) AS month,
		SUM(od.sales) AS monthly_sales
	FROM orders AS o
	JOIN order_details AS od
		ON o.order_number = od.order_number
	GROUP BY year, month
	ORDER BY year, month
)
SELECT
	year,
	month,
	monthly_sales AS this_year_sales,
	LAG(monthly_sales, 12) OVER (ORDER BY year, month) AS last_year_sales,
	monthly_sales - LAG(monthly_sales, 12) OVER (ORDER BY year, month) AS difference,
	ROUND((monthly_sales - LAG(monthly_sales, 12) OVER (ORDER BY year, month)) /
		 LAG(monthly_sales, 12) OVER (ORDER BY year, month) * 100, 2) AS "difference_percentage (%)"
FROM this_year;
/* A continuous growth is found when comparing monthly sales over different years
	except for one month, October 2004, with a drop of 2.7 percent from October 2003. */

-- Q3. Calculate total sales for each territory
SELECT
	c.territory,
	SUM(od.sales) AS territory_sales,
	ROUND(SUM(od.sales) / SUM(SUM(od.sales)) OVER() * 100, 2) AS "proportion (%)"
FROM orders AS o
JOIN customers AS c
	ON o.customer_id = c.customer_id
JOIN order_details AS od
	ON o.order_number = od.order_number
GROUP BY c.territory
ORDER BY territory_sales DESC;
/* EMEA is the company's biggest market, contributing around 50 percent of total sales.
APAC is the smallest market, accounting for only 12 percent of total sales. */

-- Q4. Calculate total sales for each country
SELECT
	c.country,
	c.territory,
	SUM(od.sales) AS country_sales
FROM orders AS o
JOIN customers AS c
	ON o.customer_id = c.customer_id
JOIN order_details AS od
	ON o.order_number = od.order_number
GROUP BY c.country, c.territory
ORDER BY country_sales DESC;
/* Despite that EMEA accounts for approximately 50% of total sales,
	USA in NA ranks 1st when calculating sales by country,
	and Australia ranks 4th even when APAC only accounts for 12 percent of total sales.
This may indicate that sales in NA and APAC are extremely concentrated,
	while sales in EMEA are more evenly distributed over various countries. */

-- Q5. Calculate total sales for each country and the proportion each country contributes within its respective territory
SELECT
	c.country,
	c.territory,
	SUM(od.sales) AS country_sales,
	ROUND(SUM(od.sales) / SUM(SUM(od.sales)) OVER (PARTITION BY c.territory) * 100, 2) AS "proportion_in_territory (%)",
	RANK() OVER (PARTITION BY territory ORDER BY SUM(od.sales) DESC) AS rank_in_territory
FROM orders AS o
JOIN customers AS c
	ON o.customer_id = c.customer_id
JOIN order_details AS od
	ON o.order_number = od.order_number
GROUP BY c.country, c.territory
ORDER BY territory ASC, country_sales DESC;
/* Australia accounts for over 50% of all sales in APAC, while 94% of total sales in NA come from the USA.
In EMEA, sales are concentrated in Spain and France, but with a much lower percentage. */

-- Q6. Identify primary customers based on total sales
SELECT
	c.customer_name,
	SUM(od.sales) AS sales,
	SUM(SUM(od.sales)) OVER() AS total_sales,
	ROUND(SUM(od.sales) / SUM(SUM(od.sales)) OVER() * 100, 2) AS "proportion (%)"
FROM orders AS o
JOIN customers AS c
	ON o.customer_id = c.customer_id
JOIN order_details AS od
	ON o.order_number = od.order_number
GROUP BY c.customer_name
ORDER BY sales DESC
LIMIT 10;
/* Euro Shopping Channel and Mini Gifts Distribution Ltd. are the two main customers,
	accounting for approximately 9 and 7 percent of total sales */

-- Q7. Identify primary customers based on the number of orders
SELECT
	c.customer_name,
	COUNT(*) AS number_of_orders,
	SUM(COUNT(*)) OVER() AS total_orders,
	ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS "proportion (%)"
FROM orders AS o
JOIN customers AS c
	ON o.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY number_of_orders DESC
LIMIT 10;
/* Euro Shopping Channel and Mini Gifts Distribution Ltd. are the two main customers,
accounting for approximately 8 and 6 percent of the total number of orders */

-- Q8. Identify popular product types based on sales and quantities ordered
SELECT
	p.product_line,
	SUM(od.sales) AS sales,
	ROUND(SUM(od.sales) / SUM(SUM(od.sales)) OVER() * 100, 2) AS "sales_proportion (%)",
	RANK() OVER(ORDER BY SUM(od.sales) DESC) AS sales_rank,
	SUM(od.quantity_ordered) AS quantities,
	ROUND(SUM(od.quantity_ordered) / SUM(SUM(od.quantity_ordered)) OVER() * 100, 2) AS "quantities_proportion (%)",
	RANK() OVER(ORDER BY SUM(od.quantity_ordered) DESC) as quantities_rank
FROM order_details AS od
JOIN products AS p
	ON od.product_code = p.product_code
GROUP BY p.product_line
ORDER BY sales_rank;
/* Product line 'Classic Cars' generated the highest sales and quantities ordered. */

-- Q9. Determine the days of the week with the most orders
SELECT
	TO_CHAR(order_date, 'Dy') AS name_of_day,
	COUNT(*) AS number_of_orders,
	ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS "proportion (%)"
FROM orders
GROUP BY DATE_PART('isodow', order_date), name_of_day
ORDER BY DATE_PART('isodow', order_date);
/* The number of orders is significantly higher during weekdays than weekends. */