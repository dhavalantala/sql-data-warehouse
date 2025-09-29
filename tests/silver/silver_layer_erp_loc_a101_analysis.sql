SELECT 
	REPLACE(cid, '-', ''),
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
         ELSE TRIM(cntry)
	END AS cntry
FROM DataWarehouse_bronze.erp_loc_a101;

-- Find unnecessary character in cid columns.
SELECT 
	cid,
    REPLACE(cid, '-', '') 
FROM DataWarehouse_bronze.erp_loc_a101;

-- Find the values which not in crm_cust_info.....
-- WHERE REPLACE(cid, '-', '') NOT IN (
SELECT 
	cst_key
FROM DataWarehouse_silver.crm_cust_info;

-- Data Standardization & Consistency 
SELECT DISTINCT
	cntry,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
         ELSE TRIM(cntry)
	END AS cntry
FROM DataWarehouse_bronze.erp_loc_a101;