/*
===================================================================================
Purpose: Load data into the Bronze layer tables in the 'bronze' schema.
         This procedure truncates each raw staging table and ingests CSV files 
         from the CRM and ERP source systems. Each table load is wrapped
         in an error-handling block so that failures in one table do not stop 
         the overall load. Progress and errors are logged using RAISE NOTICE.
===================================================================================

*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE '========================';
    RAISE NOTICE 'Starting bronze load...';
    RAISE NOTICE '========================';

    RAISE NOTICE '------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '========================';

    -- crm_cust_info
    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM 'C:/Users/unauub/Desktop/DataEng/SQL/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER true);
        RAISE NOTICE 'crm_cust_info loaded successfully!';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load crm_cust_info: %', SQLERRM;
    END;

    -- crm_prd_info
    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM 'C:/Users/unauub/Desktop/DataEng/SQL/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER true);
        RAISE NOTICE 'crm_prd_info loaded successfully!';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load crm_prd_info: %', SQLERRM;
    END;

    -- sales_details
    BEGIN
        TRUNCATE TABLE bronze.sales_details;
        COPY bronze.sales_details
        FROM 'C:/Users/unauub/Desktop/DataEng/SQL/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER true);
        RAISE NOTICE 'sales_details loaded successfully!';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load sales_details: %', SQLERRM;
    END;

    RAISE NOTICE '------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '========================';

    -- erp_cust_az12
    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM 'C:/Users/unauub/Desktop/DataEng/SQL/source_erp/cust_az12.csv'
        WITH (FORMAT csv, HEADER true);
        RAISE NOTICE 'erp_cust_az12 loaded successfully!';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load erp_cust_az12: %', SQLERRM;
    END;

    -- erp_loc_a101
    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM 'C:/Users/unauub/Desktop/DataEng/SQL/source_erp/loc_a101.csv'
        WITH (FORMAT csv, HEADER true);
        RAISE NOTICE 'erp_loc_a101 loaded successfully!';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load erp_loc_a101: %', SQLERRM;
    END;

    -- erp_px_cat_g1v2
    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM 'C:/Users/unauub/Desktop/DataEng/SQL/source_erp/px_cat_g1v2.csv'
        WITH (FORMAT csv, HEADER true);
        RAISE NOTICE 'erp_px_cat_g1v2 loaded successfully!';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Failed to load erp_px_cat_g1v2: %', SQLERRM;
    END;

    RAISE NOTICE '========================';
    RAISE NOTICE 'Bronze load completed!';
    RAISE NOTICE '========================';
END
$$;

-- Call the procedure
CALL bronze.load_bronze();
