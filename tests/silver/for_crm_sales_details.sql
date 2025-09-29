TRUNCATE TABLE DataWarehouse_silver.crm_sales_details;
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
    CASE WHEN sls_price <= 0  THEN sls_sales
		 WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price <= 0 THEN sls_sales / IFNULL(sls_quantity, 0)
		 ELSE sls_price
	END As sls_price 
FROM 
	DataWarehouse_bronze.crm_sales_details
WHERE
	sls_order_dt != 0000-00-00
    AND sls_ship_dt != 0000-00-00
    AND sls_due_dt != 0000-00-00;

