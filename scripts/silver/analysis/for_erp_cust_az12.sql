TRUNCATE TABLE DataWarehouse_silver.erp_cust_az12;
INSERT INTO DataWarehouse_silver.erp_cust_az12 (
	cid,
    bdate,
    gen
)
SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))		-- Remove 'NAS' prefix if present
		 ELSE cid
    END AS cid,
    CASE WHEN bdate < '1925-01-01' OR bdate > NOW() THEN NULL 
		 ELSE bdate
	END AS bdate,														-- Set Future birthdates to NULL
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'n/a'
	END AS gen															-- Normlize gender values and handle unknown cases
FROM DataWarehouse_bronze.erp_cust_az12;

SELECT `erp_cust_az12`.`cid`,
    `erp_cust_az12`.`bdate`,
    `erp_cust_az12`.`gen`,
    `erp_cust_az12`.`dwh_create_date`
FROM `DataWarehouse_silver`.`erp_cust_az12`;
