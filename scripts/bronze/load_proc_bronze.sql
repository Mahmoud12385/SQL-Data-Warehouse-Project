/*=====================================================
  Procedure: bronze.load_bronze
=====================================================

Purpose:
Automates the loading of raw CSV data from CRM and ERP source 
systems into the Bronze layer tables. Ensures each table is 
truncated before loading, logs the duration of each load, and 
provides structured console output for monitoring.
parameters:
  NONE
Return :
  NONE
Best Practices Implemented:
- Uses TRY...CATCH for robust error handling with detailed messages.
- Measures load time for each table and the overall batch.
- Separates processing by source system for clarity.
- Uses BULK INSERT with TABLOCK and KEEPNULLS for efficient raw data ingestion.
- Maintains a clean Bronze layer ready for downstream Silver/Gold processing.

=====================================================*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @duration VARCHAR(20),
    @batch_duration VARCHAR(20) ,@batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        -- =========================
        -- CRM Source System
        -- =========================
        PRINT '====================================================';
        PRINT '             STARTING CRM DATA LOAD                 ';
        PRINT '====================================================';

        -- CRM Customer Info
        PRINT '>>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        SET @batch_start_time = GETDATE();
        SET @start_time = GETDATE();
        PRINT '>>> Loading data into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'F:\learn sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK,
            KEEPNULLS
        );
        SET @end_time = GETDATE();
        SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + 's';
        PRINT '>>> Completed: bronze.crm_cust_info | Duration: ' + @duration;
        PRINT '----------------------------------------------------';

        -- CRM Sales Details
        PRINT '>>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        SET @start_time = GETDATE();
        PRINT '>>> Loading data into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'F:\learn sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK,
            KEEPNULLS
        );
        SET @end_time = GETDATE();
        SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + 's';
        PRINT '>>> Completed: bronze.crm_sales_details | Duration: ' + @duration;

        -- CRM Product info
        PRINT '>>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        SET @start_time = GETDATE();
        BULK INSERT bronze.crm_prd_info
        FROM 'F:\learn sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + 's';
        PRINT '>>> Completed: bronze.crm_prd_info | Duration: ' + @duration;
        PRINT '====================================================';
        PRINT '             CRM DATA LOAD COMPLETED               ';
        PRINT '====================================================';
        PRINT '';

        -- =========================
        -- ERP Source System
        -- =========================
        PRINT '====================================================';
        PRINT '             STARTING ERP DATA LOAD                 ';
        PRINT '====================================================';

        -- ERP Location Data
        PRINT '>>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        SET @start_time = GETDATE();
        PRINT '>>> Loading data into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'F:\learn sql\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK,
            KEEPNULLS
        );
        SET @end_time = GETDATE();
        SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + 's';
        PRINT '>>> Completed: bronze.erp_loc_a101 | Duration: ' + @duration;
        PRINT '----------------------------------------------------';

        -- ERP Customer Extended Data
        PRINT '>>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        SET @start_time = GETDATE();
        PRINT '>>> Loading data into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'F:\learn sql\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK,
            KEEPNULLS
        );
        SET @end_time = GETDATE();
        SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + 's';
        PRINT '>>> Completed: bronze.erp_cust_az12 | Duration: ' + @duration;
        PRINT '----------------------------------------------------';

        -- ERP Product Category Mapping
        PRINT '>>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        SET @start_time = GETDATE();
        PRINT '>>> Loading data into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'F:\learn sql\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK,
            KEEPNULLS
        );
        SET @end_time = GETDATE();
        SET @batch_end_time = GETDATE();

        SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + 's';
        SET @batch_duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @batch_start_time, @batch_end_time)) + 's';

        PRINT '>>> Completed: bronze.erp_px_cat_g1v2 | Duration: ' + @duration;
        PRINT '';
        PRINT '====================================================';
        PRINT '             ERP DATA LOAD COMPLETED               ';
        PRINT '====================================================';
        PRINT 'Total Loading Duration: ' + @batch_duration;

    END TRY
    BEGIN CATCH
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
        PRINT 'ERROR OCCURRED DURING BRONZE DATA LOAD!';
        PRINT 'Table load failed. See error details below:';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
    END CATCH
END;
GO

