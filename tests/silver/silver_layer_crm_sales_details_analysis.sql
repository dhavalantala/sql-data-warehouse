-- crm-sales-details Analysis for the silver layer
-- Checks Unwanted Space, 
-- relationship between DataWarehouse_bronze.crm_sales_details with ==> DataWarehouse_silver.crm_prd_info, 
-- Invalid DatesConsistency: Between Sales, Quantity, and Price 

SELECT 
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    CASE WHEN sls_price <= 0  THEN sls_sales
		 WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price <= 0 THEN sls_sales / IFNULL(sls_quantity, 0)
		 ELSE sls_price
	END As sls_price 
FROM 
	DataWarehouse_bronze.crm_sales_details;


-- Unwanted Space 
SELECT 
	*
FROM 
	DataWarehouse_bronze.crm_sales_details
WHERE 
	sls_ord_num!= TRIM(sls_ord_num);


-- ACCORDING TO OUR DATAMODEL CHECKING THE RELATIONSHIP BETWEEN DataWarehouse_bronze.crm_sales_details with ==> DataWarehouse_silver.crm_prd_info.....
-- sls_prd_key ==> prd_key
-- Expected Result: NULL
SELECT 
	*
FROM 
	DataWarehouse_bronze.crm_sales_details
where sls_prd_key NOT IN (
	SELECT prd_key
    FROM DataWarehouse_silver.crm_prd_info
    ); 
    

-- Check for Invalid Dates
-- sls_order_dt, sls_ship_dt, and sls_due_dt are in integer nots in Date formatt. 
SELECT 
	IFNULL(sls_order_dt, 0) sls_order_dt
FROM 
	DataWarehouse_bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR sls_order_dt > 20500101;


-- Check for Invalid Date Orders
SELECT
	* 
FROM 
	DataWarehouse_bronze.crm_sales_details
WHERE 
	sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantity, and Price 
-- => Sales = Quantiity * Price 
-- => Values must not be NULL, Zero, or negative.
-- Before transmitting a these column information we need to talk with someone from business
-- Solution 1. Data Issues will be fixed direct in source system. 
-- Solution 2. Data Issues has to be fixed in data warehouse (We don't have a budget and those data are relly old.)
-- Rules ==> If sales is negative, zero, or null, derive it using Quantity and Price.
-- 		 ==> If Price is Zero or null, calculate it using Sales and quantity. 
-- 		 ==> If price is negative, convert it to a positive value.

SELECT DISTINCT
	sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE WHEN sls_price <= 0  THEN sls_sales
		 WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
    CASE WHEN sls_price <= 0 THEN sls_sales / IFNULL(sls_quantity, 0)
		 ELSE sls_price
	END As sls_price
FROM 
	DataWarehouse_bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
-- OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


SELECT 
	sls_order_dt
FROM 
	DataWarehouse_bronze.crm_sales_details
WHERE 
	sls_order_dt = 0000-00-00










