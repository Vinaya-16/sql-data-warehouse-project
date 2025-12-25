/*
==============================================================================
CLEAN AND LOAD THE DATA INTO SILVER.ERP_LOC_A101
==============================================================================
*/

-- TRUNCATING TABLLE
TRUNCATE TABLE silver.erp_loc_a101;

PRINT('Inserting Data Into Table silver.erp_loc_a101');

-- inserting clean data into table silver.erp_loc_a101

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT
REPLACE(cid, '-', '') AS cid,
CASE
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	WHEN TRIM(cntry) IN ('USA', 'US', 'United Sates') THEN 'USA'
	WHEN TRIM(cntry) IN ('DE', 'Germany') THEN 'Germany'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
