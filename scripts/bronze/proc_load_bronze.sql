/* 
SHOW VARIABLES LIKE 'secure_file_priv';
SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;
 */


/*
===============================================================================
Bronze Loader Procedure (Commented)
===============================================================================
Purpose:
    Loads data into the Bronze layer (DataWarehouse_bronze) from CSV files.

Notes:
1. Original plan: Use LOAD DATA INFILE to import CSVs directly from local paths.
   - On macOS, LOAD DATA INFILE requires the CSV to be in the MySQL server's
     secure_file_priv folder.
   - Using LOAD DATA LOCAL INFILE is an option but requires enabling LOCAL
     on both the server and client, which can be tricky.
2. Issue encountered:
   - CSVs in /Users/dhavalantala/Documents/... could not be loaded.
   - Error: "The MySQL server is running with the --secure-file-priv option"
   - LOAD DATA LOCAL INFILE also rejected in some cases.
3. Temporary solution:
   - Files were imported manually via MySQL Workbench:
       - Open Workbench → Select table → Table Data Import Wizard
       - This works for any CSV location and handles UTF-8 automatically.

Future:
    Once CSVs are in a secure folder or server/local infile is enabled,
    LOAD DATA INFILE statements can be uncommented for automation.
===============================================================================
*/

-- Example: Truncate Bronze tables before loading
TRUNCATE TABLE DataWarehouse_bronze.crm_cust_info;
TRUNCATE TABLE DataWarehouse_bronze.crm_prd_info;
TRUNCATE TABLE DataWarehouse_bronze.crm_sales_details;
TRUNCATE TABLE DataWarehouse_bronze.erp_loc_a101;
TRUNCATE TABLE DataWarehouse_bronze.erp_cust_az12;
TRUNCATE TABLE DataWarehouse_bronze.erp_px_cat_g1v2;

-- -----------------------------
-- Manual import section
-- -----------------------------
-- The CSVs were imported manually via Workbench:
-- 1. Open MySQL Workbench
-- 2. Right-click the target table → Table Data Import Wizard
-- 3. Select CSV file (e.g., cust_info.csv)
-- 4. Map columns if necessary and finish import

-- -----------------------------
-- Uncomment the following for automated loading once paths and permissions are ready
-- -----------------------------
/*
LOAD DATA LOCAL INFILE '/path/to/cust_info.csv'
INTO TABLE DataWarehouse_bronze.crm_cust_info
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/path/to/prd_info.csv'
INTO TABLE DataWarehouse_bronze.crm_prd_info
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/path/to/sales_details.csv'
INTO TABLE DataWarehouse_bronze.crm_sales_details
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

-- Repeat for ERP tables...
*/

select * from DataWarehouse_bronze.crm_cust_info;
select * from DataWarehouse_bronze.crm_prd_info;
select * from DataWarehouse_bronze.crm_sales_details;
select * from DataWarehouse_bronze.erp_cust_az12;
select * from DataWarehouse_bronze.erp_loc_a101;
select * from DataWarehouse_bronze.erp_px_cat_g1v2;

