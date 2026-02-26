/*
========================================================
Stored Procedure: Load Bronze Layer
========================================================
Purpose:
    Loads data from external CSV files into the bronze schema.
    - Truncates existing bronze tables.
    - Uses BULK INSERT to reload fresh data.

Usage:
    EXEC bronze.load_bronze;
========================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
DECLARE @batch_start_time datetime,@batch_end_time DATETIME;
BEGIN TRY
set @batch_start_time=getdate();
     

    PRINT '===================================='
    PRINT 'LOADING BRONZE LAYER'
    PRINT '===================================='

    PRINT '------------------------------------'
    PRINT 'LOADING CRM TABLES'
    PRINT '------------------------------------'

    PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info'
    TRUNCATE TABLE bronze.crm_cust_info

    PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info'
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\dwij7\Downloads\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info'
    TRUNCATE TABLE bronze.crm_prd_info

    PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info'
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\dwij7\Downloads\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details'
    TRUNCATE TABLE bronze.crm_sales_details

    PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details'
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\dwij7\Downloads\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    PRINT '------------------------------------'
    PRINT 'LOADING ERP TABLES'
    PRINT '------------------------------------'

    PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12'
    TRUNCATE TABLE bronze.erp_cust_az12

    PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12'
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\dwij7\Downloads\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101'
    TRUNCATE TABLE bronze.erp_loc_a101

    PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101'
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\dwij7\Downloads\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2'
    TRUNCATE TABLE bronze.erp_px_cat_g1v2

    PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2'
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\dwij7\Downloads\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    PRINT'-----------------------------------------------'
    
    PRINT'==============================================='
    PRINT'LOADING BRONZE LAYER IS COMPLETED'
    set @batch_end_time=GETDATE();
    print  '>>TOTAL LOAD DURATION:'+CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)+'SECONDS';
    PRINT'==============================================='
    END TRY
    BEGIN CATCH
    PRINT'==================================================='
    PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
    PRINT'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT'ERROR MESSAGE'+CAST(ERROR_STATE() AS NVARCHAR);
    PRINT'=================================================='
    END CATCH

END


