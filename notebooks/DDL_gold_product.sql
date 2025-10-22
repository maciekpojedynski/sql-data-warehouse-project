CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY p.prd_start_dt, p.prd_key) AS product_key,
	p.prd_id AS product_id,
	p.prd_key AS production_key,
	p.prd_nm AS production_name,
	p.cat_id AS category_id,
	g1v2.cat AS category,
	g1v2.subcat AS subcategory,
	g1v2.maintenance,
	p.prd_cost AS cost,
	p.prd_line AS production_line,
	p.prd_start_dt AS start_date
FROM
	silver.crm_prd_info AS p
LEFT JOIN silver.erp_px_cat_g1v2 as g1v2
ON p.cat_id = g1v2.id
WHERE p.prd_end_dt IS NULL --Filter out all historical data


SELECT * FROM gold.dim_products;
