/*
===============================================================================
Function: Load Dataset (Source -> Tables)
===============================================================================
Script Purpose:
    This function loads data into the finance transaction tables from external CSV files. 
    It performs the following actions:
    - Truncates the tables before loading data.
    - Uses the COPY command to load data from CSV files to tables.
    - Counts and reports the number of records loaded in each table.

Parameters:
    None. 
    This function does not accept any parameters.

Usage Example:
    SELECT load_dataset();
===============================================================================
*/

-- Connect to finance_db database
\c finance_db;

-- Create the load_dataset function
CREATE OR REPLACE FUNCTION load_dataset()
RETURNS TEXT AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    duration_seconds INTEGER;
    total_duration_seconds INTEGER;
    record_count INTEGER;
    total_records INTEGER := 0;
BEGIN
    batch_start_time := clock_timestamp();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Finance Transaction Dataset';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading Finance Tables';
    RAISE NOTICE '------------------------------------------------';

    -- Load Users Data
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: users';
    TRUNCATE TABLE users CASCADE; -- CASCADE to handle foreign key constraints
    
    RAISE NOTICE '>> Inserting Data Into: users';
    COPY users (
        id,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        gender,
        address,
        latitude,
        longitude,
        per_capita_income,
        yearly_income,
        total_debt,
        credit_score,
        num_credit_cards
    )
    FROM '/home/ldduc/D/f-data-platform/data/csv/users_data.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"',
        NULL ''
    );
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM users;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    -- Load Cards Data
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: cards';
    TRUNCATE TABLE cards CASCADE; -- CASCADE to handle foreign key constraints
    
    RAISE NOTICE '>> Inserting Data Into: cards';
    COPY cards (
        id,
        client_id,
        card_brand,
        card_type,
        card_number,
        expires,
        CVV,
        has_chip,
        num_cards_issued,
        credit_limit,
        acct_open_date,
        year_pin_last_changed,
        card_on_dark_web
    )
    FROM '/home/ldduc/D/f-data-platform/data/csv/cards_data.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"',
        NULL ''
    );
    
    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM cards;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    -- Load Transactions Data
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: transactions';
    TRUNCATE TABLE transactions;
    
    RAISE NOTICE '>> Inserting Data Into: transactions';
    
    -- Use temp table approach for flexible column handling and data type conversion
    DROP TABLE IF EXISTS temp_transactions;
    CREATE TEMP TABLE temp_transactions (
        id TEXT,
        date_time TEXT,
        client_id TEXT,
        card_id TEXT,
        amount TEXT,
        use_chip TEXT,
        merchant_id TEXT,
        merchant_city TEXT,
        merchant_state TEXT,
        zip TEXT,
        mcc TEXT,
        errors TEXT
    );

    COPY temp_transactions FROM '/home/ldduc/D/f-data-platform/data/csv/transactions_data.csv' 
    WITH (
        FORMAT CSV, 
        HEADER true, 
        QUOTE '"',
        DELIMITER ',',
        NULL ''
    );

    INSERT INTO transactions (
        id,
        date_time,
        date,
        client_id,
        card_id,
        amount,
        use_chip,
        merchant_id,
        merchant_city,
        merchant_state,
        zip,
        mcc,
        errors
    )
    SELECT 
        CASE WHEN id = '' OR id IS NULL THEN NULL 
             ELSE id::INTEGER END,
        CASE WHEN date_time = '' OR date_time IS NULL THEN NULL 
             ELSE date_time::TIMESTAMP END,
        -- Get date from date_time
        CASE WHEN date_time = '' OR date_time IS NULL THEN NULL 
             ELSE (date_time::TIMESTAMP)::DATE END,
        CASE WHEN client_id = '' OR client_id IS NULL THEN NULL 
             ELSE client_id::INTEGER END,
        CASE WHEN card_id = '' OR card_id IS NULL THEN NULL 
             ELSE card_id::INTEGER END,
        -- Keep amount as string - no conversion needed
        NULLIF(amount, ''),
        NULLIF(use_chip, ''),
        CASE WHEN merchant_id = '' OR merchant_id IS NULL THEN NULL 
             ELSE merchant_id::INTEGER END,
        NULLIF(merchant_city, ''),
        NULLIF(merchant_state, ''),
        NULLIF(zip, ''),
        CASE WHEN mcc = '' OR mcc IS NULL THEN NULL 
             ELSE mcc::INTEGER END,
        NULLIF(errors, '')
    FROM temp_transactions
    WHERE id IS NOT NULL AND id != '';   

    -- Count records loaded
    SELECT COUNT(*) INTO record_count FROM transactions;
    total_records := total_records + record_count;
    
    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;
    RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
    RAISE NOTICE '>> Records Loaded: %', record_count;
    RAISE NOTICE '>> -------------';

    batch_end_time := clock_timestamp();
    total_duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))::INTEGER;
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Finance Dataset is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', total_duration_seconds;
    RAISE NOTICE '   - Total Records Loaded: %', total_records;
    RAISE NOTICE '==========================================';

    -- Summary of all table record counts
    RAISE NOTICE '';
    RAISE NOTICE 'Table Record Count Summary:';
    RAISE NOTICE '   - users: % records', (SELECT COUNT(*) FROM users);
    RAISE NOTICE '   - cards: % records', (SELECT COUNT(*) FROM cards);
    RAISE NOTICE '   - transactions: % records', (SELECT COUNT(*) FROM transactions);

    RETURN 'Finance dataset loading completed successfully in ' || total_duration_seconds || ' seconds with ' || total_records || ' total records loaded';

END;
$$ LANGUAGE plpgsql;

-- Execute the function
SELECT load_dataset();


-- Create indexes separately to avoid memory/disk issues
DO $$
BEGIN
    
    -- Index
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_transactions_date') THEN
        RAISE NOTICE 'Creating index on date...';
        CREATE INDEX idx_transactions_date ON transactions(date);
        RAISE NOTICE 'Index on date created successfully';
    END IF;
    
END $$;
