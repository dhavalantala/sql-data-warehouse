DROP VIEW IF EXISTS DataWarehouse_gold.dim_products;
CREATE VIEW DataWarehouse_gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date 
FROM DataWarehouse_silver.crm_prd_info pn
LEFT JOIN DataWarehouse_silver.erp_px_cat_g1v2 pc
ON 		  pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;		-- If the END Date is NULL then It is Current into of the Product! and Filter out all historical data.

-- Checking duplicates
-- OUTPUT is null then it's good. I don't have any duplicates.
-- SELECT 	prd_key, 
-- 		count(*)
-- FROM (
-- 	SELECT 
-- 		pn.prd_id,
-- 		pn.cat_id,
-- 		pn.prd_key,
-- 		pn.prd_nm,
-- 		pn.prd_cost,
-- 		pn.prd_line,
-- 		pn.prd_start_dt,
-- 		pc.cat,
-- 		pc.subcat,
-- 		pc.maintenance
-- 	FROM DataWarehouse_silver.crm_prd_info pn
-- 	LEFT JOIN DataWarehouse_silver.erp_px_cat_g1v2 pc
-- 	ON 		  pn.cat_id = pc.id
-- 	WHERE pn.prd_end_dt IS NULL)t GROUP BY prd_key
-- HAVING COUNT(*) >1;	