/******************************************************************************************
 Script Name: gold_layer_quality_checks.sql
 Layer      : GOLD
 Author     : <Your Name>
 Created On : <Date>

 Purpose:
 This script performs data quality validation tests on the Gold layer views
 (dim_products, dim_customers, fact_sales).

 The goal is to ensure:
    - Surrogate keys are unique and not null
    - No unexpected null foreign keys in fact table
    - No duplicate business keys
    - Referential integrity is preserved
    - Basic business rule consistency is maintained

 These checks are intended for validation after deployment or refresh.
******************************************************************************************/
GO


/*========================================================================================
    CHECK 1: Surrogate Key Integrity - dim_products
========================================================================================*/

-- Detect duplicate or null surrogate keys
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1
   OR product_key IS NULL;



/*========================================================================================
    CHECK 2: Business Key Uniqueness - dim_products
========================================================================================*/

-- Product number should uniquely identify active products
SELECT 
    product_number,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_number
HAVING COUNT(*) > 1;



/*========================================================================================
    CHECK 3: Surrogate Key Integrity - dim_customers
========================================================================================*/

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1
   OR customer_key IS NULL;



/*========================================================================================
    CHECK 4: Business Key Uniqueness - dim_customers
========================================================================================*/

SELECT 
    customer_id,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;



/*========================================================================================
    CHECK 5: Referential Integrity - fact_sales → dim_products
========================================================================================*/

-- Detect orphan product references
SELECT fs.*
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
       ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;



/*========================================================================================
    CHECK 6: Referential Integrity - fact_sales → dim_customers
========================================================================================*/

-- Detect orphan customer references
SELECT fs.*
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
       ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;



/*========================================================================================
    CHECK 7: Foreign Key Null Validation in fact_sales
========================================================================================*/

-- Fact table should not contain null dimension references
SELECT *
FROM gold.fact_sales
WHERE product_key IS NULL
   OR customer_key IS NULL;



/*========================================================================================
    CHECK 8: Date Consistency Rules
========================================================================================*/

-- Shipping date should not precede order date
SELECT *
FROM gold.fact_sales
WHERE shipping_date < order_date;

-- Due date should not precede order date
SELECT *
FROM gold.fact_sales
WHERE due_date < order_date;



/*========================================================================================
    CHECK 9: Sales Amount Logic Validation
========================================================================================*/

-- Validate sales amount consistency (quantity * price ≈ sales_amount)
SELECT *
FROM gold.fact_sales
WHERE ABS((quantity * price) - sales_amount) > 0.01;



/*========================================================================================
    CHECK 10: Negative or Invalid Measures
========================================================================================*/

SELECT *
FROM gold.fact_sales
WHERE quantity <= 0
   OR price <= 0
   OR sales_amount <= 0;
GO

