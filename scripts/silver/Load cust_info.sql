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

-- CHECK FOR NULLS AND DUPLICATES IN PRIMARY KEY
-- EXPECTATION : NO RESULT

-- DUPLICATE ROWS  
/*
	SELECT
	cust_id,
	COUNT(*) 
	FROM bronze.crm_cust_info
	GROUP BY cust_id 
	HAVING COUNT(*) > 1 OR cust_id IS NULL
*/


-- INTEGRAL ROW FOR EACH PRIMARY KEY 
/*
	SELECT
	*
	FROM
	(
		SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC) AS flags
		FROM bronze.crm_cust_info
	) t 
	WHERE flags = 1
*/


-- CHECK FOR UNWANTED SPACES
-- EXPECTATION : NO RESULT
/*
-- cst_firstname = 15
	SELECT 
	cst_firstname
	FROM bronze.crm_cust_info
	WHERE cst_firstname != TRIM(cst_firstname)

-- cst_lastname = 17
    SELECT 
	cst_lastname
	FROM bronze.crm_cust_info
	WHERE cst_lastname != TRIM(cst_lastname)

-- cst_maritalstatus = 0
	SELECT 
	cst_maritalstatus
	FROM bronze.crm_cust_info
	WHERE cst_maritalstatus != TRIM(cst_maritalstatus)

-- cst_gndr = 0
	SELECT 
	cst_gndr
	FROM bronze.crm_cust_info
	WHERE cst_gndr != TRIM(cst_gndr)

*/

-- REMOVE THE TRAILING SPACES AND MAKE THE DATA MEANINGFUL
/*
	SELECT
	cust_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_maritalstatus)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_maritalstatus)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_maritalstatus,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM
	(
		SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC) AS flags
		FROM bronze.crm_cust_info
	) t 
	WHERE flags = 1
*/


-- INSERT THE CLEAN DATA INTO SILVER SCHEMA

INSERT INTO silver.crm_cust_info (
	cust_id,
	cst_key,
	cst_firstname,
	cst_lastname, 
	cst_maritalstatus,
	cst_gndr,
	cst_create_date
)
SELECT
cust_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE 
	WHEN UPPER(TRIM(cst_maritalstatus)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_maritalstatus)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_maritalstatus, -- Normalize marital status values to readable format
CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr, -- Normalize gender values to readable format
cst_create_date
FROM
(
	SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC) AS flags
	FROM bronze.crm_cust_info
) t 
WHERE flags = 1 AND cust_id IS NOT NULL

