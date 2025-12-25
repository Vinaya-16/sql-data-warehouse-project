/*
==============================================================================
CLEAN AND LOAD THE DATA INTO SILVER.ERP_PX_CAT_G1V2
==============================================================================
*/
-- TRUNCATING TABLLE
TRUNCATE TABLE silver.erp_px_cat_g1v2;

PRINT('Inserting Data Into Table silver.erp_px_cat_g1v2');

-- inserting clean data into table silver.erp_px_cat_g1v2

INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2
