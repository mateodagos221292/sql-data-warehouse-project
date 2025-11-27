/*
======================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================================

Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to 
populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load silver;

======================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc_start DATETIME2(7) = SYSUTCDATETIME();
    DECLARE @step_start DATETIME2(7);
    DECLARE @step_end DATETIME2(7);
    DECLARE @duration_ms BIGINT;
    DECLARE @proc_end DATETIME2(7);
    DECLARE @full_duration_ms BIGINT;
    DECLARE @current_section INT = 0;

    -- Helper: use RAISERROR WITH NOWAIT so messages stream immediately.

    BEGIN TRY

    -- SECTION 1: crm_cust_info
    SET @current_section = 1;
    SET @step_start = SYSUTCDATETIME();
    RAISERROR('Section 1/6: Starting - silver.crm_cust_info (truncate + insert)',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_cust_info;
        RAISERROR('Section 1: Truncate completed',0,1) WITH NOWAIT;
        RAISERROR('Section 1: Starting insert',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date    
    )
    SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Marriage'
        ELSE 'N/A'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END AS cst_gndr,
    cst_create_date
    FROM(
    SELECT
    *,
    ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    from bronze.crm_cust_info ) t 
    where flag_last = 1;
    SET @step_end = SYSUTCDATETIME();
    SET @duration_ms = DATEDIFF(MILLISECOND,@step_start,@step_end);
    RAISERROR('Section 1 completed in %d ms',0,1,@duration_ms) WITH NOWAIT;

    -- SECTION 2: crm_prd_info
    SET @current_section = 2;
    SET @step_start = SYSUTCDATETIME();
    RAISERROR('Section 2/6: Starting - silver.crm_prd_info (truncate + insert)',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_prd_info;
        RAISERROR('Section 2: Truncate completed',0,1) WITH NOWAIT;
        RAISERROR('Section 2: Starting insert',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt    
    )
    SELECT prd_id
        ,REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id
        ,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
        ,prd_nm
        ,ISNULL(prd_cost,0) as prd_cost
        ,CASE UPPER(TRIM(prd_line))
                WHEN  'M' THEN 'Mountain'
                WHEN  'R' THEN 'Road'
                WHEN  'S' THEN 'Other Sales'
                WHEN  'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line
        ,CAST(prd_start_dt AS DATE) AS prd_start_dt
        ,CAST(DATEADD(DAY, -1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))AS DATE) AS prd_end_dt
    FROM DataWarehouse.bronze.crm_prd_info;
    SET @step_end = SYSUTCDATETIME();
    SET @duration_ms = DATEDIFF(MILLISECOND,@step_start,@step_end);
    RAISERROR('Section 2 completed in %d ms',0,1,@duration_ms) WITH NOWAIT;

    -- SECTION 3: crm_sales_details
    SET @current_section = 3;
    SET @step_start = SYSUTCDATETIME();
    RAISERROR('Section 3/6: Starting - silver.crm_sales_details (truncate + insert)',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_sales_details;
        RAISERROR('Section 3: Truncate completed',0,1) WITH NOWAIT;
        RAISERROR('Section 3: Starting insert',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_sales_details (
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
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales 
        END AS sls_sales ,
        sls_quantity,  
        CASE 
            WHEN sls_price IS NULL OR sls_price = 0
            THEN sls_sales /  NULLIF(sls_quantity,0)
            ELSE sls_price 
        END AS sls_price
    from bronze.crm_sales_details;
    SET @step_end = SYSUTCDATETIME();
    SET @duration_ms = DATEDIFF(MILLISECOND,@step_start,@step_end);
    RAISERROR('Section 3 completed in %d ms',0,1,@duration_ms) WITH NOWAIT;

    -- SECTION 4: erp_cust_az12
    SET @current_section = 4;
    SET @step_start = SYSUTCDATETIME();
    RAISERROR('Section 4/6: Starting - silver.erp_cust_az12 (truncate + insert)',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_cust_az12;
        RAISERROR('Section 4: Truncate completed',0,1) WITH NOWAIT;
        RAISERROR('Section 4: Starting insert',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
    SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid
    END AS cid ,
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate, 
    CASE
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'Female') THEN 'Female'
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'Male') THEN 'Male'
        ELSE NULL
    END AS gen
    FROM bronze.erp_cust_az12;
    SET @step_end = SYSUTCDATETIME();
    SET @duration_ms = DATEDIFF(MILLISECOND,@step_start,@step_end);
    RAISERROR('Section 4 completed in %d ms',0,1,@duration_ms) WITH NOWAIT;

    -- SECTION 5: erp_loc_a101
    SET @current_section = 5;
    SET @step_start = SYSUTCDATETIME();
    RAISERROR('Section 5/6: Starting - silver.erp_loc_a101 (truncate + insert)',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_loc_a101;
        RAISERROR('Section 5: Truncate completed',0,1) WITH NOWAIT;
        RAISERROR('Section 5: Starting insert',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_loc_a101 (cid,cntry)
    SELECT
    REPLACE(cid , '-','') as cid,
    CASE 
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' OR  TRIM(REPLACE(cntry, CHAR(13), '')) IS NULL THEN 'N/A'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
    END AS cntry
    FROM bronze.erp_loc_a101;
    SET @step_end = SYSUTCDATETIME();
    SET @duration_ms = DATEDIFF(MILLISECOND,@step_start,@step_end);
    RAISERROR('Section 5 completed in %d ms',0,1,@duration_ms) WITH NOWAIT;

    -- SECTION 6: erp_px_cat_g1v2
    SET @current_section = 6;
    SET @step_start = SYSUTCDATETIME();
    RAISERROR('Section 6/6: Starting - silver.erp_px_cat_g1v2 (truncate + insert)',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        RAISERROR('Section 6: Truncate completed',0,1) WITH NOWAIT;
        RAISERROR('Section 6: Starting insert',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
    SELECT
    id,
    cat,
    subcat,
    REPLACE(maintenance, CHAR(13), '') AS maintenance
    FROM bronze.erp_px_cat_g1v2;
    SET @step_end = SYSUTCDATETIME();
    SET @duration_ms = DATEDIFF(MILLISECOND,@step_start,@step_end);
    RAISERROR('Section 6 completed in %d ms',0,1,@duration_ms) WITH NOWAIT;

    END TRY
    BEGIN CATCH
        -- Capture step end and report which section failed
        SET @step_end = SYSUTCDATETIME();
        SET @duration_ms = DATEDIFF(MILLISECOND,@step_start,@step_end);
        DECLARE @err_num INT = ERROR_NUMBER();
        DECLARE @err_msg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(N'ERROR in Section %d: %d - %s',16,1,@current_section,@err_num,@err_msg) WITH NOWAIT;
        RAISERROR(N'Section %d duration until error: %d ms',0,1,@current_section,@duration_ms) WITH NOWAIT;
        SET @proc_end = SYSUTCDATETIME();
        SET @full_duration_ms = DATEDIFF(MILLISECOND,@proc_start,@proc_end);
        RAISERROR(N'Procedure `silver.load_silver` aborted after error, total time %d ms',16,1,@full_duration_ms) WITH NOWAIT;
        THROW;
    END CATCH

    -- Full duration (only reached when all sections succeed)
    SET @proc_end = SYSUTCDATETIME();
    SET @full_duration_ms = DATEDIFF(MILLISECOND,@proc_start,@proc_end);
    RAISERROR('Procedure `silver.load_silver` completed in %d ms',0,1,@full_duration_ms) WITH NOWAIT;

END
 



