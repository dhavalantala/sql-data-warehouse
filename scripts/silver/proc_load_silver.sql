/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    SHOW PROCEDURE STATUS WHERE Db = 'DataWarehouse_silver' AND Name = 'load_silver';
	CALL DataWarehouse_silver.load_silver();
===============================================================================
*/



DELIMITER //

-- Drop the existing procedure if it exists
DROP PROCEDURE IF EXISTS DataWarehouse_silver.load_silver;

CREATE PROCEDURE DataWarehouse_silver.load_silver()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    DECLARE error_msg VARCHAR(512);
    DECLARE error_code INT;

    -- Declare handler for SQL exceptions
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        -- Capture error details using GET DIAGNOSTICS
        GET DIAGNOSTICS CONDITION 1
            error_code = MYSQL_ERRNO,
            error_msg = MESSAGE_TEXT;
        
        SELECT '==========================================' AS message;
        SELECT 'ERROR OCCURRED DURING LOADING SILVER LAYER' AS message;
        SELECT CONCAT('Error Message: ', error_msg) AS message;
        SELECT CONCAT('Error Code: ', error_code) AS message;
        SELECT '==========================================' AS message;
    END;

    SET batch_start_time = NOW();
    SELECT '================================================' AS message;
    SELECT 'Loading Silver Layer' AS message;
    SELECT '================================================' AS message;

    SELECT '------------------------------------------------' AS message;
    SELECT 'Loading CRM Tables' AS message;
    SELECT '------------------------------------------------' AS message;

    -- Loading DataWarehouse_silver.crm_cust_info
    SET start_time = NOW();
    SELECT '>> Truncating Table: DataWarehouse_silver.crm_cust_info' AS message;
    TRUNCATE TABLE DataWarehouse_silver.crm_cust_info;
    SELECT '>> Inserting Data Into: DataWarehouse_silver.crm_cust_info' AS message;
    INSERT INTO DataWarehouse_silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM DataWarehouse_bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Loading DataWarehouse_silver.crm_prd_info
    SET start_time = NOW();
    SELECT '>> Truncating Table: DataWarehouse_silver.crm_prd_info' AS message;
    TRUNCATE TABLE DataWarehouse_silver.crm_prd_info;
    SELECT '>> Inserting Data Into: DataWarehouse_silver.crm_prd_info' AS message;
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
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        (SELECT 
            CAST(DATE_SUB(MIN(t2.prd_start_dt), INTERVAL 1 DAY) AS DATE)
         FROM DataWarehouse_bronze.crm_prd_info t2 
         WHERE t2.prd_key = t1.prd_key 
         AND t2.prd_start_dt > t1.prd_start_dt
         LIMIT 1) AS prd_end_dt
    FROM DataWarehouse_bronze.crm_prd_info t1;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Loading DataWarehouse_silver.crm_sales_details
    SET start_time = NOW();
    SELECT '>> Truncating Table: DataWarehouse_silver.crm_sales_details' AS message;
    TRUNCATE TABLE DataWarehouse_silver.crm_sales_details;
    SELECT '>> Inserting Data Into: DataWarehouse_silver.crm_sales_details' AS message;
    INSERT INTO DataWarehouse_silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        CASE 
            WHEN sls_price <= 0 THEN sls_sales
            WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM DataWarehouse_bronze.crm_sales_details
    WHERE sls_order_dt != '0000-00-00'
    AND sls_ship_dt != '0000-00-00'
    AND sls_due_dt != '0000-00-00';
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Loading DataWarehouse_silver.erp_cust_az12
    SET start_time = NOW();
    SELECT '>> Truncating Table: DataWarehouse_silver.erp_cust_az12' AS message;
    TRUNCATE TABLE DataWarehouse_silver.erp_cust_az12;
    SELECT '>> Inserting Data Into: DataWarehouse_silver.erp_cust_az12' AS message;
    INSERT INTO DataWarehouse_silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate < '1925-01-01' OR bdate > NOW() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM DataWarehouse_bronze.erp_cust_az12;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    SELECT '------------------------------------------------' AS message;
    SELECT 'Loading ERP Tables' AS message;
    SELECT '------------------------------------------------' AS message;

    -- Loading DataWarehouse_silver.erp_loc_a101
    SET start_time = NOW();
    SELECT '>> Truncating Table: DataWarehouse_silver.erp_loc_a101' AS message;
    TRUNCATE TABLE DataWarehouse_silver.erp_loc_a101;
    SELECT '>> Inserting Data Into: DataWarehouse_silver.erp_loc_a101' AS message;
    INSERT INTO DataWarehouse_silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM DataWarehouse_bronze.erp_loc_a101;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    -- Loading DataWarehouse_silver.erp_px_cat_g1v2
    SET start_time = NOW();
    SELECT '>> Truncating Table: DataWarehouse_silver.erp_px_cat_g1v2' AS message;
    TRUNCATE TABLE DataWarehouse_silver.erp_px_cat_g1v2;
    SELECT '>> Inserting Data Into: DataWarehouse_silver.erp_px_cat_g1v2' AS message;
    INSERT INTO DataWarehouse_silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM DataWarehouse_bronze.erp_px_cat_g1v2;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS message;
    SELECT '>> -------------' AS message;

    SET batch_end_time = NOW();
    SELECT '==========================================' AS message;
    SELECT 'Loading Silver Layer is Completed' AS message;
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS message;
    SELECT '==========================================' AS message;

END //

DELIMITER ;
