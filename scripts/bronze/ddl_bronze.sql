/*
===============================================================================
DDL Script: Create Bronze Tables (MySQL Version)
===============================================================================
Script Purpose:
    This script creates tables in the 'DataWarehouse_bronze' database, 
    dropping existing tables if they already exist.
    Run this script to re-define the DDL structure of Bronze Tables.
===============================================================================
*/

-- Switch to Bronze Database
USE DataWarehouse_bronze;

DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
	cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE
);

-- CRM: Product Info
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

-- CRM: Sales Details
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    DECIMAL(10,2),
    sls_quantity INT,
    sls_price    DECIMAL(10,2)
);

-- ERP: Location
DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
    cid   VARCHAR(50),
    cntry VARCHAR(50)
);

-- ERP: Customer
DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
    cid   VARCHAR(50),
    bdate DATE,
    gen   VARCHAR(50)
);

-- ERP: Product Category
DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
    id          VARCHAR(50),
    cat         VARCHAR(50),
    subcat      VARCHAR(50),
    maintenance VARCHAR(50)
);
