/* 
	Quality Check on BRONZE LAYER. 
*/
SELECT `crm_prd_info`.`prd_id`,
    `crm_prd_info`.`prd_key`,
    `crm_prd_info`.`prd_nm`,
    `crm_prd_info`.`prd_cost`,
    `crm_prd_info`.`prd_line`,
    `crm_prd_info`.`prd_start_dt`,
    `crm_prd_info`.`prd_end_dt`
FROM `DataWarehouse_bronze`.`crm_prd_info`;

-- Check for nulls or Duplicates in primary Key
-- Expectation: No Result
SELECT 
	prd_id,
    COUNT(*)
FROM
	DataWarehouse_bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or NOT NULL;

-- Extracting the information from prd_key to match a available information in sales details 
-- I extract the cat_id from prd_key because erp_px_cat_g1v2 consist of category id.

SELECT 
	prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    prd_line,
    CASE WHEN UPPER(TRIM(prd_line)) = "M" THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = "R" THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = "S" THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = "T" THEN 'Touring'
         ELSE 'n/a'
	END AS prd_line, 
    DATE(prd_start_dt) prd_start_dt,
    DATE(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY
	) AS prd_end_dt
FROM
	DataWarehouse_bronze.crm_prd_info;

-- cat_id not in -->> filter our unmatched data after applying transformation
-- Where REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (......)
SELECT 
	distinct id
FROM
	DataWarehouse_bronze.erp_px_cat_g1v2;
    
-- sls_prd_key is align with crm_prd_info but we need to extract a value from prd_key, also we get a inromation about how many product never get a order 
-- where SUBSTRING(prd_key, 7, LENGTH(prd_key)) NOT IN (.......) 
SELECT 
	sls_prd_key
FROM
	DataWarehouse_bronze.crm_sales_details
WHERE 
	sls_prd_key LIKE 'FK%';
    
-- Check for unwanted space  in prd_nm
-- Expectation: No Result
SELECT 
	prd_nm
FROM
	DataWarehouse_bronze.crm_prd_info
WHERE 
	prd_nm != TRIM(prd_nm);


-- Based on Business (In our business case study)
-- Checks for NULLs or Negative Numbers
-- Expectation: No Results
-- If we found that and our business alow us to change according to requirement we have do.....
-- Because also better for letter value avg or sum....
SELECT
	prd_cost
FROM
	DataWarehouse_bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Data Standardization & Consistency 
-- usually we need to ask expertise about abbreviations.
SELECT
	count(DISTINCT prd_line)
FROM
	DataWarehouse_bronze.crm_prd_info;

-- Check for Invalid Date Orders
-- I noticed that prd_end_dt is less than prd_start_dt
SELECT
	*
FROM
	DataWarehouse_bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Logic for Date
SELECT
	prd_id,
    prd_key,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY AS prd_end_dt_test
FROM
	DataWarehouse_bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');






















































