    -- [For Testing Purposes]
    -- Truncate all tables
    TRUNCATE TABLE landing_table;

    TRUNCATE TABLE cleaned;

    TRUNCATE TABLE for_cleaning;

    TRUNCATE TABLE invalid;

    TRUNCATE TABLE cleaned_normalized;

    TRUNCATE TABLE product_dimension;

    TRUNCATE TABLE time_dimension;

    TRUNCATE TABLE location_dimension;

    TRUNCATE TABLE final_fact;

    TRUNCATE TABLE sales_data_cube;

    TRUNCATE TABLE sliced_cube;

    -- Select statements

    SELECT *
    FROM landing_table;


    SELECT *
    FROM cleaned;


    SELECT *
    FROM for_cleaning;


    SELECT *
    FROM invalid;


    SELECT *
    FROM cleaned_normalized;


    SELECT *
    FROM product_dimension;


    SELECT *
    FROM time_dimension
    ORDER BY time_level DESC;


    SELECT *
    FROM location_dimension
    ORDER BY level DESC;


    SELECT *
    FROM final_fact
    ORDER BY order_id;


    SELECT *
    FROM products;


    SELECT *
    FROM locations
    ORDER BY level DESC;


    SELECT *
    FROM time
    ORDER BY time_level,
            time_desc DESC;


    SELECT *
    FROM sales;


    SELECT *
    FROM sales_data_cube;


    SELECT *
    FROM sliced_cube;

    -- Call the procedures
    CALL clear_all_tables(); -- using delete instead of truncate

    CALL call_all_procedures();

    CALL truncate_all_tables();

    CALL data_extraction(); -- just to complete the list of procedures

    CALL data_mapping();

    CALL data_cleansing();

    CALL normalize_data();

    CALL create_product_dimension();

    CALL populate_product_dimension();

    CALL create_time_dimension();

    CALL create_location_dimension();

    CALL populate_location_dimension();

    CALL create_final_fact_table();

    CALL create_sales_data_cube();

    CALL delete_permanent_records();


    CREATE OR REPLACE PROCEDURE delete_permanent_records() LANGUAGE plpgsql AS $$
        BEGIN
            -- Delete data from products
            DELETE FROM products;

            -- Delete data from locations
            DELETE FROM locations;

            -- Delete data from time
            DELETE FROM time;

            -- Delete data from sales
            DELETE FROM sales;

            -- Delete data from sales_data_cube
            DELETE FROM sales_data_cube;

            PERFORM log_message('Permanent records deleted.');
        END;
        $$;

    -- Testing for duplicates, both complete and only ids

    SELECT *
    FROM for_cleaning
    WHERE order_id = '150925';

    -- Checking for duplicates in cleaned table

    SELECT DISTINCT(order_id),
        product,
        quantity_ordered,
        price_each,
        order_date,
        purchase_address,
        CASE
            WHEN COUNT(*) > 1 THEN 'T'
            ELSE 'F'
        END
    FROM cleaned
    GROUP BY order_id,
            product,
            quantity_ordered,
            price_each,
            order_date,
            purchase_address -- HAVING COUNT(*) > 1

    ORDER BY order_id,
            product;

    -- Checking for duplicates in cleaned_normalized table

    SELECT DISTINCT(order_id),
        product,
        quantity_ordered,
        price_each,
        order_date,
        street,
        city,
        state,
        zip_code,
        CASE
            WHEN COUNT(*) > 1 THEN 'T'
            ELSE 'F'
        END
    FROM cleaned_normalized
    GROUP BY order_id,
            product,
            quantity_ordered,
            price_each,
            order_date,
            street,
            city,
            state,
            zip_code -- HAVING COUNT(*) > 1

    ORDER BY order_id,
            product;

    -- Basic scenario (no slicing, gets all data)
    CALL slice_cube(NULL, NULL, NULL, NULL);

    -- Time-based scenarios
    -- Slice by year
    CALL slice_cube('Y2019', NULL, NULL, NULL);

    -- Slice by year, showing month level
    CALL slice_cube('Y2019', NULL, 1, NULL);

    -- Slice by year, showing quarter level
    CALL slice_cube('Y2019', NULL, 2, NULL);

    -- Slice by quarter, showing month level
    CALL slice_cube('Q1-2019', NULL, 1, NULL);

    -- Slice by single month, showing day level
    CALL slice_cube('M01-2019', NULL, 0, NULL);

    -- Location-based scenarios
    -- Slice by state
    CALL slice_cube(NULL, 'CA', NULL, NULL);

    -- Slice by state, showing city level
    CALL slice_cube(NULL, 'CA', NULL, 1);

    -- Slice by city, showing street level
    CALL slice_cube(NULL, 'San Francisco', NULL, 0);

    -- Combined time and location scenarios
    -- Sales by month for a specific state
    CALL slice_cube('Y2019', 'CA', 1, NULL);

    -- Sales by city for a specific quarter
    CALL slice_cube('Q1-2019', 'CA', NULL, 1);

    -- Sales by day for a specific city in a specific month
    CALL slice_cube('M01-2019', 'San Francisco', 0, NULL);

    -- Most granular view: Sales by day and street
    CALL slice_cube('M01-2019', 'San Francisco', 0, 0);

    -- Check the results

    SELECT *
    FROM sliced_cube
    ORDER BY time_id,
            location_id,
            product_id;

