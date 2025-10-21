EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATE, @batch_end_time DATE;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=======================================';
		PRINT 'Loading Silver Layer';
		PRINT '=======================================';

		PRINT '---------------------------------------';
		PRINT 'Loading for the CRM Tables';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr
		)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'unknown'
		END as cst_marital_status, -- Normalize marital status values to readable format
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'unknown'
		END as cst_gndr -- Normalize gender values to readable format
		FROM(
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE(); 
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
			PRINT '---------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
			PRINT '---------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
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
			CASE 
				WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
				END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
				END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
				END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price <= 0 or sls_price IS NULL 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
				END AS sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
			PRINT '---------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
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

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
			PRINT '---------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
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
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
			PRINT '---------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)

		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
				SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
			PRINT '---------------------------------';
				SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Silver layer is Completed';
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERORR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END
