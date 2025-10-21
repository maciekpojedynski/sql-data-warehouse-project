CREATE NONCLUSTERED INDEX IX_cst_dedup
ON bronze.crm_cust_info (cst_id, cst_create_date DESC)
WITH (DROP_EXISTING = OFF);

/* The Index is urgent due to timeout while executing customer table in silver layer. Nesting query with subquery with RANK_NUMBER and window function created bottleneck of query's performance. */ 
