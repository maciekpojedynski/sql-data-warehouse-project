INSERT INTO silver.crm_prd_info(
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
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') as cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
TRIM(prd_nm) as prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE TRIM(UPPER(prd_line))
	WHEN 'T' THEN 'Touring'
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	ELSE 'unknown'
END as prd_line,
prd_start_dt,
DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) as prd_end_dt
FROM 
bronze.crm_prd_info;
