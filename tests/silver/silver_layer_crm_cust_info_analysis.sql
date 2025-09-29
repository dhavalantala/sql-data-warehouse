-- Quality Check on BRONZE LAYER. 
-- A primary Key must be unique and not null.
-- INSERT INTO SILCER LAYER

SELECT 
    CASE 
        WHEN cst_id IS NULL THEN 'NULL'
        WHEN cst_id = '' THEN 'EMPTY'
        ELSE CAST(cst_id AS CHAR)
    END AS cst_id_display,
	-- cst_id,
    COUNT(*) AS cnt
FROM DataWarehouse_bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
   OR cst_id IS NULL
   OR cst_id = '';
   
-- Check for Unwanted Spaces and Check for all string values
-- Expectation: No Results
SELECT 	
	-- cst_firstname,
    -- cst_gndr, 								-- No White space 
    cst_lastname
FROM
	DataWarehouse_bronze.crm_cust_info
WHERE 
 	cst_lastname != TRIM(cst_lastname); 
    
-- Data Standardization & Consistence
select distinct cst_gndr
from DataWarehouse_bronze.crm_cust_info;

select distinct cst_marital_status
from DataWarehouse_bronze.crm_cust_info;




-- lates check everything on SILVER LAYER `crm_cust_info`
SELECT 
    CASE 
        WHEN cst_id IS NULL THEN 'NULL'
        WHEN cst_id = '' THEN 'EMPTY'
        ELSE CAST(cst_id AS CHAR)
    END AS cst_id_display,
	-- cst_id,
    COUNT(*) AS cnt
FROM DataWarehouse_silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
   OR cst_id IS NULL
   OR cst_id = '';

SELECT 	
	-- cst_firstname,
    cst_gndr 								-- No White space 
    -- cst_lastname
FROM
	DataWarehouse_silver.crm_cust_info
WHERE 
 	cst_gndr != TRIM(cst_gndr); 

-- Data Standardization & Consistence
select distinct cst_gndr
from DataWarehouse_silver.crm_cust_info;

select distinct cst_marital_status
from DataWarehouse_silver.crm_cust_info;


select *
from DataWarehouse_silver.crm_cust_info;













