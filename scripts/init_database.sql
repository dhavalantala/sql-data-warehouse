/*
=============================================================
Create Data Warehouse Databases
=============================================================
Script Purpose:
    This script drops and recreates three databases for the Data Warehouse:
    - DataWarehouse_bronze (raw ingested data)
    - DataWarehouse_silver (cleaned/standardized data)
    - DataWarehouse_gold   (business-ready data for reporting)

WARNING:
    Running this script will drop these databases if they already exist. 
    All data will be permanently deleted. Proceed with caution and ensure
    you have proper backups before running this script.
*/

USE mysql;

-- Drop existing Data Warehouse layer databases
DROP DATABASE IF EXISTS DataWarehouse_bronze;
DROP DATABASE IF EXISTS DataWarehouse_silver;
DROP DATABASE IF EXISTS DataWarehouse_gold;

-- Create new databases (layers)
CREATE DATABASE DataWarehouse_bronze;
CREATE DATABASE DataWarehouse_silver;
CREATE DATABASE DataWarehouse_gold;
