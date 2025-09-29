TRUNCATE TABLE DataWarehouse_silver.erp_px_cat_g1v2;
INSERT INTO DataWarehouse_silver.erp_px_cat_g1v2(
	id,
    cat,
    subcat,
    maintenance)
SELECT 
	id,
    cat,
    subcat,
    maintenance
FROM DataWarehouse_bronze.erp_px_cat_g1v2;