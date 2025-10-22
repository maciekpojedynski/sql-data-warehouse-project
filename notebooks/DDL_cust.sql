CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	a101.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gndr = 'unknown' and az12.gen IS NOT NULL THEN az12.gen
		WHEN ci.cst_gndr IS NULL and az12.gen is NULL THEN 'unknown'
		ELSE ci.cst_gndr
	END AS gender,
	az12.bdate AS birthdate
FROM 
	silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 as az12
ON ci.cst_key = az12.cid
LEFT JOIN silver.erp_loc_a101 as a101
ON ci.cst_key = a101.cid
