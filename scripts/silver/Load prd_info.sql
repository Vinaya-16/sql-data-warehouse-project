/*

	================================================================
	CLEAN AND LOAD crm_cust_info INTO SILVER LAYER
	================================================================

	Remove Unwanted Spaces
	Removes unnecessary spaces to ensure data consistnecy, 
	and uniformity accross all records

	Data Normalization & Standardization
	Maps coded values to meaningful, user-friendly descriptions

	Handling Missing Data
	Fills in the blanks by adding a default value

	Remove Duplicates
	Ensure only one record per entity by identifying and retaining the most relevant row

*/

/* 
	SELECT
	*
	FROM silver.crm_prd_info
*/


-- CHECK FOR NULL OR DUPLICATES IN PRIMARY KEY
-- EXPECTATIONS : NO RESULT
/*
	SELECT
	prd_id,
	COUNT(*) 
	FROM silver.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) > 1 OR prd_id IS NULL
*/

-- INSERT CATEGORY ID , PRODUCT KEY
/*
	SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	FROM silver.crm_prd_info
*/

-- CHECK FOR UNWANTED SPACES
-- EXPECTATIONS : NO RESULT

/*
	-- prd_nm = 0
	SELECT
	prd_nm
	FROM silver.crm_prd_info
	WHERE TRIM(prd_nm) != prd_nm
*/

/*
	-- CHECK FOR NULLS AND NEGATIVE NUMBERS
	-- EXPECTATIONS : NO RESULTS
	SELECT
	prd_cost
	FROM silver.crm_prd_info
	WHERE prd_cost < 0 OR prd_cost IS NULL

	-- HANDLE NULL COST
	SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	FROM silver.crm_prd_info
*/

-- Data Normalization & Standardization
-- Maps coded values to meaningful, user-friendly descriptions
/* 
	SELECT DISTINCT
	prd_line
	FROM silver.crm_prd_info

	-- ===============================================================
	SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	prd_end_dt
	FROM silver.crm_prd_info
*/

-- CHECK FOR INVALID DATES
-- EXPECTATIONS : NO RESULTS
/*
	SELECT
	*
	FROM silver.crm_prd_info
	WHERE prd_end_dt < prd_start_dt

	-- ===========================================================
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm, 
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)AS prd_end_dt
	FROM silver.crm_prd_info
*/

-- INSERT CLEAN DATA INTO crm_prd_info
INSERT INTO silver.crm_prd_info
(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm, 
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)AS prd_end_dt
	FROM silver.crm_prd_info

