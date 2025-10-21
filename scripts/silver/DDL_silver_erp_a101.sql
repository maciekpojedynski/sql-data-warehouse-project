IF OBJECT_ID ('silver.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
)

INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-','') as cid,
	CASE 
		WHEN TRIM(cntry) IN('USA','US') THEN 'United States'
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN NULLIF(cntry, '') IS NULL THEN 'unknown'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101
