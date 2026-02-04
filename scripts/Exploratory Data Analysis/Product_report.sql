/*
=======================================================================
Product Report
=======================================================================
Purpose:
	This report consolidates key product metrics and behaviours.
Highlights:
	1. Gathers essential fields such as product name, category, subcategory, and cost
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low_Performers.
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last sales)
		- average order revenue (AOR)
		- average monthly revenue
============================================================================
*/


CREATE OR ALTER VIEW gold.product_report AS
WITH base_query AS
(
	SELECT
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.product_cost,
	f.order_number,
	f.customer_key,
	f.quantity,
	f.sales_amount,
	f.order_date
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
		ON p.product_key = f.product_key
	WHERE f.order_date IS NOT NULL
),
products_aggregations AS
(
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		product_cost,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
		MAX(order_date) AS last_order_date,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_qnty_sold,
		COUNT(DISTINCT customer_key) AS total_customers,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
	FROM base_query
	GROUP BY 
		product_key,
		product_name,
		category,
		subcategory,
		product_cost
)
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	product_cost,
	last_order_date,
	CASE
		WHEN total_sales > 50000 THEN 'High Performers'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performers'
	END AS product_segment,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency,
	total_orders,
	total_sales,
	total_qnty_sold,
	total_customers,
	lifespan AS lifespan_in_months,
	avg_selling_price,
	CASE 
		WHEN total_sales = 0 THEN 0
		WHEN total_orders = 0 THEN 0
		ELSE total_sales/total_orders 
	END AS avg_order_revenue,
	CASE 
		WHEN total_sales = 0 THEN 0
		WHEN lifespan = 0 THEN lifespan
		ELSE total_sales/lifespan
	END AS avg_monthly_revenue
FROM products_aggregations
