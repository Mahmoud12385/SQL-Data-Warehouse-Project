/******************************************************************************************
 Layer      : GOLD
 Purpose:
 This script creates the analytical (Gold Layer) views for the data warehouse.
 
 The Gold layer represents the business-ready dimensional model following 
 a Star Schema design. It includes:

    1. dim_products   → Product dimension (current active products only)
    2. dim_customers  → Customer dimension (enriched with demographic data)
    3. fact_sales     → Sales fact table referencing product and customer dimensions

 These views are designed to support reporting, BI tools, dashboards,
 and analytical queries.

 Notes:
 - Surrogate keys are generated using ROW_NUMBER().
 - Historical product records are excluded.
 - Views are recreated safely using DROP IF EXISTS.
******************************************************************************************/
GO


/*========================================================================================
    VIEW: gold.dim_products
    Description: Product dimension containing only active products.
========================================================================================*/

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    -- Surrogate key generated for dimensional modeling
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
       ON pn.cat_id = pc.id

-- Keep only current active products (exclude historical versions)
WHERE pn.prd_end_dt IS NULL;
GO



/*========================================================================================
    VIEW: gold.dim_customers
    Description: Customer dimension enriched with gender, location, and birth data.
========================================================================================*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 

    -- Surrogate key for dimensional modeling
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,

    ci.cst_id         AS customer_id,
    ci.cst_key        AS customer_number,
    ci.cst_firstname  AS first_name,
    ci.cst_lastname   AS last_name,
    lo.cntry          AS country,
    ci.cst_marital_status AS marital_status,

    -- Fallback logic when CRM gender is missing
    CASE 
        WHEN ci.cst_gndr = 'n/a' 
            THEN COALESCE(ca.gen, 'n/a')
        ELSE ci.cst_gndr 
    END AS gender,

    ca.bdate           AS birth_date,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
       ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 lo
       ON ci.cst_key = lo.cid;
GO



/*========================================================================================
    VIEW: gold.fact_sales
    Description: Sales fact table linking customers and products.
========================================================================================*/

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 

    sl.sls_ord_num    AS order_number,

    -- Foreign keys referencing dimension surrogate keys
    dp.product_key,
    dc.customer_key,

    sl.sls_order_dt   AS order_date,
    sl.sls_ship_dt    AS shipping_date,
    sl.sls_due_dt     AS due_date,

    sl.sls_sales      AS sales_amount,
    sl.sls_quantity   AS quantity,
    sl.sls_price      AS price

FROM silver.crm_sales_details sl
LEFT JOIN gold.dim_customers dc
       ON sl.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
       ON sl.sls_prd_key = dp.product_number;
GO
