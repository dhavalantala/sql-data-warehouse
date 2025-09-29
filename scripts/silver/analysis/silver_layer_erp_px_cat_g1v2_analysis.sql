SELECT 
	id,
    cat,
    subcat,
    maintenance
FROM DataWarehouse_bronze.erp_px_cat_g1v2;


-- Check for Unwanted Spaces 
SELECT 
	id,
    cat,
    subcat,
    maintenance
FROM DataWarehouse_bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
	-- cat
    -- subcat
    maintenance
FROM DataWarehouse_bronze.erp_px_cat_g1v2;