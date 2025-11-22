/*
================================================================
DDL Script: Create Bronze Tables
================================================================
Script Purpose:
    This scriprt creates tables in the bronze schema, dropping existing tables if they already exist
    Run this script to re-fedine the DDL structure of bronze tables
================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time  DATETIME , @end_time DATETIME;
BEGIN TRY
        PRINT '========================' ;
        PRINT 'Loading Bronze Layer' ;
        PRINT '========================';

        PRINT 'Loading CRM Tables' ;

        SET @start_time = GETDATE() ;

        PRINT '>> Trucating Table: crm_cust_info' ;
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data into: crm_cust_info' ;
        BULK INSERT bronze.crm_cust_info
        FROM '/tmp/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',' ,
            TABLOCK 
        );

        SET @end_time = GETDATE() ;
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT '>> --------------'

        SET @start_time = GETDATE() ;
        PRINT '>> Trucating Table: crm_prd_info' ;
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data into: crm_prd_info' ;
        BULK INSERT bronze.crm_prd_info
        FROM '/tmp/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',' ,
            TABLOCK 
        );

        SET @end_time = GETDATE() ;
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT '>> --------------'

        SET @start_time = GETDATE() ;
        PRINT '>> Trucating Table: crm_sales_details' ;   
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data into: crm_sales_details' ;
        BULK INSERT bronze.crm_sales_details
        FROM '/tmp/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',' ,
            TABLOCK 
        );

        SET @end_time = GETDATE() ;
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT '>> --------------'

        PRINT 'Loading ERP Tables';

        SET @start_time = GETDATE() ;

        PRINT '>> Trucating Table: erp_cust_az12' ;
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data into: erp_cust_az12' ;
        BULK INSERT bronze.erp_cust_az12
        FROM '/tmp/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',' ,
            TABLOCK 
        );

        SET @end_time = GETDATE() ;
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT '>> --------------'

        SET @start_time = GETDATE() ;

        PRINT '>> Trucating Table: erp_loc_a101' ;
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data into: erp_loc_a101' ;
        BULK INSERT bronze.erp_loc_a101
        FROM '/tmp/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',' ,
            TABLOCK 
        );

        SET @end_time = GETDATE() ;
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT '>> --------------'

        SET @start_time = GETDATE() ;

        PRINT '>> Trucating Table: bronze.erp_px_cat_g1v2' ;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data into: erp_px_cat_g1v2' ;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/tmp/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',' ,
            TABLOCK 
        );

        SET @end_time = GETDATE() ;
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + 'seconds'
        PRINT '>> --------------'

    END TRY
    BEGIN CATCH
        PRINT '============================================'
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR) ;
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR) ;
        PRINT '============================================'
    END CATCH

END

