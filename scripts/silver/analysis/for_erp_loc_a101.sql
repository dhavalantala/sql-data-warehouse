TRUNCATE TABLE DataWarehouse_silver.erp_loc_a101;
INSERT INTO DataWarehouse_silver.erp_loc_a101(
	cid,
    cntry
)
SELECT 
	REPLACE(cid, '-', ''),
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
         ELSE TRIM(cntry)
	END AS cntry												-- Normalize and Handle missing or blank country codes.
FROM DataWarehouse_bronze.erp_loc_a101;

SELECT `erp_loc_a101`.`cid`,
    `erp_loc_a101`.`cntry`,
    `erp_loc_a101`.`dwh_create_date`
FROM `DataWarehouse_silver`.`erp_loc_a101`;
