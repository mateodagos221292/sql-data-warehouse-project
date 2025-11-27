
--- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Results

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id is NULL

-- Check for unwanted spaces
-- Expectatio: No results

SELECT cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)


-- Data Standardization & Consistency

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Nulls or Negative Numbers

SELECT prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is NULL


-- Check for Invalid Date Orders

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales 
END  AS sls_sales ,  
CASE WHEN sls_price IS NULL OR sls_price = 0
THEN sls_sales /  NULLIF(sls_quantity,0)
ELSE sls_price 
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL 


