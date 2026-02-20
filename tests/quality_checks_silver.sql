-- ==================================================================================
-- Purpose: Silver Data Quality Checks
-- 
-- This script performs comprehensive data quality and consistency checks for all 
-- Silver schema tables after inserting or refreshing data from CRM and ERP sources.
-- Ensures:
--   1. No duplicate or NULL primary keys.
--   2. No negative or inconsistent numeric values.
--   3. Proper chronological order for dates.
--   4. No overlapping product periods or sales orders.
--   5. No unwanted leading/trailing spaces in text fields.
--   6. Referential integrity between tables.
--   7. Standardization and consistency across categorical fields.
--
-- Expected Outcome: All queries should return no records if data is clean.
-- ==================================================================================

-- ============================================
-- Table: crm_cust_info (CRM Customers)
-- Checks for duplicates, NULLs, unwanted spaces, and general consistency
-- ============================================

-- Duplicate or NULL customer IDs
SELECT cst_id, COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Inspect a specific repeated ID (example)
SELECT * FROM silver.crm_cust_info WHERE cst_id = 29466;

-- Keep only non-duplicate, non-null IDs (most recent creation date)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM silver.crm_cust_info
) t
WHERE flag_last != 1 OR cst_id IS NULL;

-- Check for unwanted spaces in text fields
SELECT COUNT(*) FROM silver.crm_cust_info WHERE TRIM(cst_firstname) != cst_firstname;
SELECT COUNT(*) FROM silver.crm_cust_info WHERE TRIM(cst_lastname) != cst_lastname;
SELECT COUNT(*) FROM silver.crm_cust_info WHERE TRIM(cst_gndr) != cst_gndr;
SELECT COUNT(*) FROM silver.crm_cust_info WHERE TRIM(cst_marital_status) != cst_marital_status;

-- Extra check: NULLs in critical fields
SELECT * FROM silver.crm_cust_info
WHERE cst_firstname IS NULL OR cst_lastname IS NULL;

-- ============================================
-- Table: crm_prd_info (CRM Products)
-- Checks for duplicates, negative costs, unwanted spaces, and date overlaps
-- ============================================

-- Duplicate or NULL product IDs
SELECT prd_id, COUNT(*) AS n_appearance
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check unwanted spaces
SELECT * FROM silver.crm_prd_info WHERE TRIM(prd_nm) != prd_nm;

-- Check NULLs or negative cost
SELECT * FROM silver.crm_prd_info WHERE prd_cost IS NULL OR prd_cost < 0;

-- Data standardization: distinct product lines
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Product date validity: start < end
SELECT * FROM silver.crm_prd_info WHERE prd_start_dt >= prd_end_dt;

-- Extra check: overlapping periods for same product
SELECT *
FROM (
    SELECT *,
        LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS next_start
    FROM silver.crm_prd_info
) t
WHERE prd_end_dt > next_start;

-- ============================================
-- Table: crm_sales_details (CRM Sales)
-- Checks for duplicates, invalid dates, negative or inconsistent values, overlapping orders
-- ============================================



-- Invalid chronological dates
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt >= sls_ship_dt OR sls_ship_dt >= sls_due_dt;

-- Numeric consistency: sales = quantity * price, no negatives or NULLs
SELECT * FROM silver.crm_sales_details WHERE sls_sales != sls_quantity * sls_price;
SELECT * FROM silver.crm_sales_details WHERE sls_sales <= 0 OR sls_sales IS NULL;
SELECT * FROM silver.crm_sales_details WHERE sls_quantity <= 0 OR sls_quantity IS NULL;
SELECT * FROM silver.crm_sales_details WHERE sls_price <= 0 OR sls_price IS NULL;



-- ============================================
-- Table: erp_cust_az12 (ERP Customer)
-- Checks for standardization, future dates, duplicate IDs, and gender values
-- ============================================

SELECT DISTINCT gen FROM silver.erp_cust_az12;
SELECT DISTINCT bdate FROM silver.erp_cust_az12 WHERE bdate > GETDATE();

-- Check duplicate IDs
SELECT cid, COUNT(*) AS cnt
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;

-- ============================================
-- Table: erp_loc_a101 (ERP Locations)
-- Checks for standardization, referential integrity, and duplicate IDs
-- ============================================

SELECT DISTINCT cntry FROM silver.erp_loc_a101;

-- Orphaned locations: cid not in CRM customer keys
SELECT *
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- Duplicate IDs
SELECT cid, COUNT(*) AS cnt
FROM silver.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1;

-- ============================================
-- Table: erp_px_cat_g1v2 (ERP Product Categories)
-- Checks for standardization, unwanted spaces, and duplicates
-- ============================================

-- Distinct categorical values
SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT subcat FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;

-- Unwanted spaces
SELECT cat, subcat, maintenance
FROM silver.erp_px_cat_g1v2
WHERE TRIM(cat) != cat OR TRIM(subcat) != subcat OR TRIM(maintenance) != maintenance;

-- Duplicate IDs
SELECT id, COUNT(*) AS cnt
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1;
