-- ===================================================================================
-- Stored Procedure: silver.load_silver
--
-- Purpose:
--   Orchestrates the full Silver-layer load process by transforming and loading
--   data from the Bronze layer into Silver tables.
--
-- Description:
--   - Performs a full refresh (TRUNCATE + INSERT) of all Silver tables
--   - Applies data cleansing, standardization, and business rules
--   - Covers CRM, Sales, and ERP reference datasets
--   - Uses RAISE NOTICE for execution logging and traceability
--
-- Usage:
--   CALL silver.load_silver();
-- ===================================================================================

create or replace procedure silver.load_silver()
LANGUAGE plpgsql
as $$
begin
	-- ===================================================================================
	-- SILVER LAYER LOAD SCRIPT
	-- Purpose:
	--   Load, cleanse, and standardize data from Bronze to Silver layer.
	--   This script handles CRM, Sales, and ERP reference data.
	--
	-- Notes:
	--   - Silver tables are truncated before reload (full refresh strategy)
	--   - Business rules are applied during transformation
	--   - RAISE NOTICE statements provide execution traceability
	-- ===================================================================================
	
	
	-- ===================================================================================
	-- CRM CUSTOMER DATA
	-- Purpose:
	--   Load the most recent customer record per customer ID
	--   and standardize names, gender, and marital status.
	-- ===================================================================================
	RAISE NOTICE 'Truncating table: silver.crm_cust_info';
	
	TRUNCATE TABLE silver.crm_cust_info;
	
	INSERT INTO silver.crm_cust_info (
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
	
	    -- Trim leading/trailing spaces from names
	    TRIM(cst_firstname) AS cst_firstname,
	    TRIM(cst_lastname)  AS cst_lastname,
	
	    -- Standardize marital status codes
	    CASE
	        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	        ELSE 'n/a'
	    END AS cst_marital_status,
	
	    -- Standardize gender codes
	    CASE
	        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	        ELSE 'n/a'
	    END AS cst_gndr,
	
	    cst_create_date
	FROM (
	    -- Keep only the most recent record per customer
	    SELECT *,
	           ROW_NUMBER() OVER (
	               PARTITION BY cst_id
	               ORDER BY cst_create_date DESC
	           ) AS flag_last
	    FROM bronze.crm_cust_info
	) sub
	WHERE flag_last = 1;
	
	
	-- ===================================================================================
	-- CRM PRODUCT DATA
	-- Purpose:
	--   Clean product identifiers, standardize product lines,
	--   handle missing costs, and calculate product end dates.
	-- ===================================================================================
	RAISE NOTICE 'Truncating table: silver.crm_prd_info';
	
	TRUNCATE TABLE silver.crm_prd_info;
	
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
	SELECT
	    prd_id,
	
	    -- Extract category from first 5 characters of product key
	    REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
	
	    -- Extract product key from 7th character onward
	    SUBSTRING(prd_key FROM 7) AS prd_key,
	
	    prd_nm,
	
	    -- Replace NULL cost values with 0
	    COALESCE(prd_cost::INT, 0) AS prd_cost,
	
	    -- Map product line codes to descriptive values
	    CASE
	        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	        ELSE 'n/a'
	    END AS prd_line,
	
	    CAST(prd_start_dt AS DATE) AS prd_start_dt,
	
	    -- End date is one day before the next start date per product
	    CAST(
	        LEAD(prd_start_dt)
	            OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
	        AS DATE
	    ) AS prd_end_dt
	FROM bronze.crm_prd_info;
	
	
	-- ===================================================================================
	-- CRM SALES DETAILS
	-- Purpose:
	--   Cleanse transactional sales data by fixing dates,
	--   recalculating invalid prices and sales amounts.
	-- ===================================================================================
	RAISE NOTICE 'Truncating table: silver.crm_sales_details';
	
	TRUNCATE TABLE silver.crm_sales_details;
	
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
	
	    -- Convert order date from YYYYMMDD format
	    CASE
	        WHEN sls_order_dt = 0
	          OR LENGTH(sls_order_dt::TEXT) != 8
	        THEN NULL
	        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	    END AS sls_order_dt,
	
	    -- Convert ship date
	    CASE
	        WHEN sls_ship_dt = 0
	          OR LENGTH(sls_ship_dt::TEXT) != 8
	        THEN NULL
	        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
	    END AS sls_ship_dt,
	
	    -- Convert due date
	    CASE
	        WHEN sls_due_dt = 0
	          OR LENGTH(sls_due_dt::TEXT) != 8
	        THEN NULL
	        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	    END AS sls_due_dt,
	
	    sls_quantity,
	
	    -- Fix invalid or missing price using sales ÷ quantity
	    CASE
	        WHEN sls_price IS NULL OR sls_price <= 0
	        THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
	        ELSE sls_price
	    END AS sls_price,
	
	    -- Recalculate sales if missing or inconsistent
	    CASE
	        WHEN sls_sales IS NULL
	          OR sls_sales <= 0
	          OR sls_sales != sls_quantity * ABS(sls_price)
	        THEN sls_quantity * ABS(sls_price)
	        ELSE sls_sales
	    END AS sls_sales
	FROM bronze.sales_details;
	
	
	-- ===================================================================================
	-- ERP CUSTOMER DATA
	-- ===================================================================================
	RAISE NOTICE 'Truncating table: silver.erp_cust_az12';
	
	TRUNCATE TABLE silver.erp_cust_az12;
	
	INSERT INTO silver.erp_cust_az12 (
	    cid,
	    bdate,
	    gen
	)
	SELECT
	    -- Remove NAS prefix if present
	    CASE
	        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
	        ELSE cid
	    END AS cid,
	
	    -- Remove invalid future birth dates
	    CASE
	        WHEN bdate > NOW() THEN NULL
	        ELSE bdate
	    END AS bdate,
	
	    -- Normalize gender values
	    CASE
	        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	        ELSE 'n/a'
	    END AS gen
	FROM bronze.erp_cust_az12;
	
	
	-- ===================================================================================
	-- ERP LOCATION DATA
	-- ===================================================================================
	RAISE NOTICE 'Truncating table: silver.erp_loc_a101';
	
	TRUNCATE TABLE silver.erp_loc_a101;
	
	INSERT INTO silver.erp_loc_a101 (
	    cid,
	    cntry
	)
	SELECT
	    -- Remove hyphens from customer ID
	    REPLACE(cid, '-', '') AS cid,
	
	    -- Normalize country codes
	    CASE
	        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	        ELSE TRIM(cntry)
	    END AS cntry
	FROM bronze.erp_loc_a101;
	
	
	-- ===================================================================================
	-- ERP PRODUCT CATEGORY DATA
	-- ===================================================================================
	RAISE NOTICE 'Truncating table: silver.erp_px_cat_g1v2';
	
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	
	INSERT INTO silver.erp_px_cat_g1v2 (
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
end $$

call silver.load_silver()
