/*
======================================================================================
Customer Report
======================================================================================
Purpose:
	- This report consolidates key customer metrics and behaviors

Highlights:
	1. Gathers essential fields such as names, ages, and transactions details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
=======================================================================================
*/

WITH base_query AS
(
-- BASE QUERY FOR CORE RECORDS
	SELECT
		f.order_number,
		f.product_key,
		f.order_date,
		f.quantity,
		f.sales_amount,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
		DATEDIFF(YEAR, c.birthdate, GETDATE()) AS age
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c.customer_key = f.customer_key
	WHERE f.order_date IS NOT NULL
)
, customer_aggregations AS
(
	SELECT
		customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT product_key) AS total_products,
		MAX(order_date) AS last_order,
		SUM(quantity) AS total_quantity,
		SUM(sales_amount) AS total_sales,
		CONCAT(DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) , ' months') AS lifespan
	FROM base_query
	GROUP BY 
		customer_key,
		customer_number,
		customer_name,
		age
)
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE 
		WHEN age < 20 THEN 'Under 20'
		WHEN age BETWEEN 20 AND 40 THEN '20-40'
		WHEN age BETWEEN 40 AND 60 THEN '40-60'
		WHEN age BETWEEN 60 AND 80 THEN '60-80'
		ELSE 'Above 80'
	END AS age_group,
	CASE 
		WHEN SUM(total_sales) > 5000 AND lifespan >= 12
			THEN 'VIP'
		WHEN SUM(total_sales) <= 5000 AND lifespan >= 12
			THEN 'Regular'
		ELSE 'New'
	END Type_of_customer,
	total_orders,
	total_products,
	last_order,
	total_quantity,
	total_sales,
	lifespan
FROM customer_aggregations
GROUP B
