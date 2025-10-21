-- DROP TABLE 
IF OBJECT_ID ('silver.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;

-- CREATE TABLE
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

WITH 
-- cid Normalisation
	cte_cid AS (
		SELECT 
			CASE 
				WHEN LEN(cid) > 10 THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid 
			END AS cid,
			bdate,
			gen
	FROM bronze.erp_cust_az12
	), 
cte_transform AS (
	SELECT
		c.cid,
		CASE 
			WHEN c.bdate > GETDATE() THEN NULL
			ELSE c.bdate
		END AS bdate,
		CASE 
			WHEN NULLIF(c.gen, '') IS NULL THEN cu.cst_gndr
			WHEN c.gen = 'F' THEN 'Female'
			WHEN c.gen = 'M' THEN 'Male'
			ELSE c.gen
		END AS gen
	FROM cte_cid as c
	LEFT JOIN silver.crm_cust_info as cu
	ON c.cid = cu.cst_key
)



-- INSERT FROM CTE
INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)

SELECT * FROM cte_transform;
