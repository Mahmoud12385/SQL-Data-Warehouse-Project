/*
============================================================
Procedure Name : silver.load_silver
Purpose        : Load and transform data from bronze schema 
                 into silver schema.
                 
Description    :
- Cleanses, standardizes, and deduplicates CRM & ERP data.
- Applies business rules and data normalization.
- Measures and prints duration for each step.
- Implements structured error handling.
============================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN



    DECLARE 
        @step_start_time   DATETIME,
        @step_end_time     DATETIME,
        @total_start_time  DATETIME,
        @total_end_time    DATETIME;

    BEGIN TRY

        SET @total_start_time = GETDATE();

        PRINT '====================================================';
        PRINT '               STARTING SILVER LOAD                 ';
        PRINT '====================================================';


        /*====================================================
          1) Load silver.crm_cust_info
        ====================================================*/
        PRINT '----------------------------------------------------';
        PRINT 'Step 1: Loading silver.crm_cust_info';
        PRINT '----------------------------------------------------';

        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        SET @step_start_time = GETDATE();

        INSERT INTO silver.crm_cust_info
        (
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
            TRIM(cst_firstname)      AS cst_firstname,
            TRIM(cst_lastname)       AS cst_lastname,

            -- Normalize coded marital status
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END                      AS cst_marital_status,

            -- Normalize gender values
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'n/a'
            END                      AS cst_gndr,

            cst_create_date
        FROM
        (
            -- Keep only latest record per customer
            SELECT *,
                   ROW_NUMBER() OVER 
                   (
                       PARTITION BY cst_id 
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1
          AND cst_id IS NOT NULL;

        SET @step_end_time = GETDATE();

        PRINT 'Step 1 Completed Successfully';
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND, @step_start_time, @step_end_time) AS VARCHAR(20)) + ' seconds';


        /*====================================================
          2) Load silver.crm_prd_info
        ====================================================*/
        PRINT '----------------------------------------------------';
        PRINT 'Step 2: Loading silver.crm_prd_info';
        PRINT '----------------------------------------------------';

        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        SET @step_start_time = GETDATE();

        INSERT INTO silver.crm_prd_info
        (
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

            -- Extract category id
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

            -- Extract product key
            SUBSTRING(prd_key, 7, LEN(prd_key))         AS prd_key,

            prd_nm,
            ISNULL(prd_cost, 0)                         AS prd_cost,

            -- Map product line codes
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Sport'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END                                         AS prd_line,

            CAST(prd_start_dt AS DATE)                  AS prd_start_dt,

            -- SCD logic: derive end date using LEAD
            CAST(
                LEAD(prd_start_dt) OVER
                (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) AS DATE
            )                                           AS prd_end_dt

        FROM bronze.crm_prd_info;

        SET @step_end_time = GETDATE();

        PRINT 'Step 2 Completed Successfully';
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND, @step_start_time, @step_end_time) AS VARCHAR(20)) + ' seconds';


        /*====================================================
          3) Load silver.crm_sales_details
        ====================================================*/
        PRINT '----------------------------------------------------';
        PRINT 'Step 3: Loading silver.crm_sales_details';
        PRINT '----------------------------------------------------';

        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        SET @step_start_time = GETDATE();

        INSERT INTO silver.crm_sales_details
        (
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

            -- Convert YYYYMMDD integer to DATE
            TRY_CONVERT(DATE, CAST(sls_order_dt AS CHAR(8)), 112),
            TRY_CONVERT(DATE, CAST(sls_ship_dt  AS CHAR(8)), 112),
            TRY_CONVERT(DATE, CAST(sls_due_dt   AS CHAR(8)), 112),

            -- Recalculate invalid sales values
            CASE 
                WHEN sls_sales IS NULL 
                     OR sls_sales <= 0
                     OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN ABS(sls_quantity * sls_price)
                ELSE sls_sales
            END,

            sls_quantity,

            -- Recalculate invalid price values
            CASE 
                WHEN sls_price IS NULL 
                     OR sls_price <= 0
                THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
                ELSE sls_price
            END

        FROM bronze.crm_sales_details;

        SET @step_end_time = GETDATE();

        PRINT 'Step 3 Completed Successfully';
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND, @step_start_time, @step_end_time) AS VARCHAR(20)) + ' seconds';


        /*====================================================
          4) Load ERP Tables
        ====================================================*/
        PRINT '----------------------------------------------------';
        PRINT 'Step 4: Loading ERP Tables';
        PRINT '----------------------------------------------------';


        -- 4.1 silver.erp_cust_az12
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        SET @step_start_time = GETDATE();

        INSERT INTO silver.erp_cust_az12
        (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' 
                THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END,

            CASE 
                WHEN bdate > GETDATE() 
                THEN NULL
                ELSE bdate
            END,

            CASE
                WHEN UPPER(TRIM(gen)) IN ('M','MALE')   THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @step_end_time = GETDATE();

        PRINT 'ERP Customer Load Completed';
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND, @step_start_time, @step_end_time) AS VARCHAR(20)) + ' seconds';


        -- 4.2 silver.erp_loc_a101
        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101
        (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', ''),
            CASE 
                WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) = 'DE'          THEN 'Germany'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;


        -- 4.3 silver.erp_px_cat_g1v2
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2
        (
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


        SET @total_end_time = GETDATE();

        PRINT '====================================================';
        PRINT '        SILVER LOAD COMPLETED SUCCESSFULLY         ';
        PRINT 'Total Duration: ' + 
              CAST(DATEDIFF(SECOND, @total_start_time, @total_end_time) AS VARCHAR(20)) + ' seconds';
        PRINT '====================================================';

    END TRY
    BEGIN CATCH

        PRINT '====================================================';
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS VARCHAR(20));
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Line    : ' + CAST(ERROR_LINE() AS VARCHAR(20));
        PRINT '====================================================';

        THROW;

    END CATCH

END;


