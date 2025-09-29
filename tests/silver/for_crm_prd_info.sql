USE DataWarehouse_silver;
TRUNCATE TABLE DataWarehouse_silver.crm_prd_info ;
INSERT INTO DataWarehouse_silver.crm_prd_info (
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
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE WHEN UPPER(TRIM(prd_line)) = "M" THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = "R" THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = "S" THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = "T" THEN 'Touring'
         ELSE 'n/a'
	END AS prd_line, 
    DATE(prd_start_dt) prd_start_dt,
    DATE(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY) AS prd_end_dt
FROM
	DataWarehouse_bronze.crm_prd_info;


SELECT *
FROM DataWarehouse_silver.crm_prd_info;