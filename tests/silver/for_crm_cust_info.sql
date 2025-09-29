-- 1. Remove a duplicate
-- 2. Remove an unwanted space from all string columns
-- 3. Data Standardization & Consistence
-- INSERT INTO SILCER LAYER
USE DataWarehouse_silver;
TRUNCATE TABLE DataWarehouse_silver.crm_cust_info;
INSERT INTO DataWarehouse_silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)

select
	cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname, 														-- Problem Solved 2. 				
    TRIM(cst_lastname) AS cst_lastname, 														-- Problem Solved 2.
    
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'								-- Problem Solved 3.
		 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'								-- Problem Solved 3.
         ELSE 'n/a'
	END cst_marital_status,
    
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'											-- Problem Solved 3.
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'											-- Problem Solved 3.
         ELSE 'n/a'
	END cst_gndr,
    cst_create_date
from (
	SELECT 
		*,
		row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
	FROM
		DataWarehouse_bronze.crm_cust_info) t
WHERE flag_last = 1; 																			-- Problem Solved 1. 












