SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		 ELSE cid
    END AS cid,
    CASE WHEN bdate < '1925-01-01' OR bdate > NOW() THEN NULL 
		 ELSE bdate
	END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'n/a'
	END AS gen
FROM DataWarehouse_bronze.erp_cust_az12;


-- Check that anycustomer not in our crm_cust_info.........
-- where CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
-- 		 ELSE cid
--     END NOT IN (....);
select 
	cst_key
from
	DataWarehouse_silver.crm_cust_info;
    
-- Identify Out-of_range Dates
-- We need to Inform the Amdin or We can replace the values by our self.  
SELECT  
	bdate
FROM 
	DataWarehouse_bronze.erp_cust_az12
WHERE 
	bdate < '1925-01-01' OR bdate > NOW();
    
-- Data Standardization & Consistency 
SELECT DISTINCT
	gen,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'n/a'
	END AS gen
FROM 
	DataWarehouse_bronze.erp_cust_az12;
    
    
    
    
    
