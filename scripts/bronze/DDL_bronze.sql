/*=====================================================
  Bronze Layer - SQL Server (Raw Landing Tables)
=====================================================

Purpose:
This script defines the Bronze layer tables for the SQL Server data warehouse. 
The Bronze layer serves as the raw landing zone where data from various source systems 
(CRM and ERP) is ingested in its original form, without transformations. 

It includes:
- CRM tables: customer information, product details, and sales transactions.
- ERP tables: location data, extended customer information, and product category mappings.

These tables provide a structured foundation for subsequent processing and 
transformation into Silver (cleansed) and Gold (aggregated/analytics-ready) layers.

Note:
- All tables are dropped and recreated to ensure a clean slate for data ingestion.
- Data types are chosen to accommodate raw source data as-is.
=====================================================*/
/*=====================================================
  Bronze Layer - SQL Server (Raw Landing Tables)
=====================================================*/

-- CRM Customer Info
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info
(
    cst_id          INT,
    cst_key         NVARCHAR(50),
    cst_firstname   NVARCHAR(50),
    cst_lastname    NVARCHAR(50),
    cst_gndr        NVARCHAR(50),
    cst_matrial     NVARCHAR(50),
    cst_create_date DATE
);
GO

-- CRM Product Info
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info
(
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_line     NVARCHAR(50),
    prd_cost     INT,
    prd_start_dt DATE,
    prd_end_dt   DATE
);
GO

-- Sales Details
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details
(
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,   -- YYYYMMDD
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_quantity  INT,
    sls_price     INT,
    sls_sales     INT
);
GO

-- ERP Location Data
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101
(
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

-- ERP Customer Extended Data
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO
CREATE TABLE bronze.erp_cust_az12
(
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);
GO

-- ERP Product Category Mapping
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2
(
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO
