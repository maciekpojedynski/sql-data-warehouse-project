--Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT
cst_id,
COUNT(*) as Number_of_ids
FROM 
silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
--Expectation: No Result

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

--Data Standarization & Consistancy

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;
