
-- ANALYZE SALES PERFORMANCE OVER TIME
SELECT
YEAR(f.order_date) AS order_year,
COUNT(DISTINCT f.customer_key) AS total_customers,
SUM(f.quantity) AS total_quantity,
SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
WHERE f.order_date IS NOT NULL
GROUP BY YEAR(f.order_date)
ORDER BY YEAR(f.order_date)

-- OR

SELECT
DATETRUNC(month, f.order_date) AS order_date,
COUNT(DISTINCT f.customer_key) AS total_customers,
SUM(f.quantity) AS total_quantity,
SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
WHERE f.order_date IS NOT NULL
GROUP BY DATETRUNC(month, f.order_date)
ORDER BY DATETRUNC(month, f.order_date)


-- CALCULATE THE TOTAL SALES PER MONTH AND THE RUNNING TOTAL OF SALES OVER TIME
SELECT
FORMAT(order_month, 'yyy') AS order_year,
FORMAT(order_month, 'MMM') AS order_month,
total_sales,
SUM(total_sales) OVER (ORDER BY order_month) AS running_total_sales,
AVG(avg_price) OVER (ORDER BY order_month) AS moving_average
FROM
(
	SELECT
	DATETRUNC(month, order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(month, order_date)
) t


-- ANALYZE THE YEARLY PERFORMANCE OF PRODUCTS BY COMPARING EACH PRODUCT'S SALES TO BOTH
-- ITS AVERAGE SALES PERFORMANCE AND THE PREVIOUS YEAR'S SALES
WITH yearly_products_sales AS
(
	SELECT
	YEAR(f.order_date) AS order_year,
	p.product_name,
	SUM(f.sales_amount) AS current_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
	WHERE YEAR(f.order_date) IS NOT NULL
	GROUP BY 
		YEAR(f.order_date),
		p.product_name
)
SELECT
order_year,
product_name,
current_sales,
AVG(current_sales) OVER(PARTITION BY product_name) AS avg_sales,
(current_sales - AVG(current_sales) OVER(PARTITION BY product_name)) AS diff_avg,
CASE
	WHEN (current_sales - AVG(current_sales) OVER(PARTITION BY product_name)) > 0
		THEN 'Above Avg'
	WHEN (current_sales - AVG(current_sales) OVER(PARTITION BY product_name)) < 0
		THEN 'Below Avg'
	ELSE 'Avg'
END AS avg_change,
LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
(current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) AS diff_prev_yr,
CASE
	WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) > 0
		THEN 'Increase'
	WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) < 0
		THEN 'Decrease'
	ELSE 'No Change'
END AS prev_yr_change
FROM yearly_products_sales
ORDER BY product_name, order_year


-- WHICH CATEGORIES CONTRIBUTES THE MOST TO OVERALL SALES ?
WITH categories_sales AS
(
	SELECT
	p.category,
	SUM(f.sales_amount) AS total_cat_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
	GROUP BY p.category
) 
SELECT
category,
total_cat_sales,
SUM(total_cat_sales) OVER() AS total_sales,
CONCAT( ROUND((CAST(total_cat_sales AS FLOAT)/SUM(total_cat_sales) OVER())*100, 2), '%') AS perc_sales
FROM categories_sales
ORDER BY perc_sales DESC



