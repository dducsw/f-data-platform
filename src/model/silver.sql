-- ==========================================
-- Database Silver Layer
-- ==========================================
CREATE DATABASE IF NOT EXISTS dlh_silver
COMMENT 'Silver Layer for Finance Lakehouse';

USE dlh_silver;
DROP TABLE IF EXISTS dlh_silver.users;
DROP TABLE IF EXISTS dlh_silver.cards;
DROP TABLE IF EXISTS dlh_silver.transactions;
DROP TABLE IF EXISTS dlh_silver.mcc_codes;
DROP TABLE IF EXISTS dlh_silver.train_fraud_labels;

-- ==========================================
-- Users (Dimension) - SCD2
-- ==========================================

CREATE TABLE IF NOT EXISTS dlh_silver.users (
    client_key          INT,                -- surrogate key
    client_id           INT,                -- business key
    current_age         INT,
    retirement_age      INT,
    birth_year          INT,
    birth_month         INT,
    age_group           STRING,             -- derived from birth_year
    gender              STRING,
    address             STRING,
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    per_capita_income   DECIMAL(15,2),
    yearly_income       DECIMAL(15,2),
    income_group        STRING,
    total_debt          DECIMAL(15,2),
    credit_score        INT,
    num_credit_cards    INT,
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN
)
CLUSTERED BY (client_id) INTO 10 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- ==========================================
-- Cards (Dimension) - SCD2
-- ==========================================
CREATE TABLE IF NOT EXISTS dlh_silver.cards (
    card_key            INT,                -- surrogate key
    card_id             INT,                -- business key
    client_id           INT,
    card_brand          STRING,
    card_type           STRING,
    expires_date        STRING,
    expires_year        INT,
    expires_month       INT,
    is_expires          BOOLEAN,
    has_chip            BOOLEAN,
    num_cards_issued    INT,
    credit_limit        DECIMAL(12,2),
    acct_open_date      STRING,
    acct_open_year      INT,
    acct_open_month     INT,
    card_status         STRING,
    year_pin_last_changed INT,
    card_on_dark_web    STRING,
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN
)
CLUSTERED BY (card_id) INTO 8 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- ==========================================
-- Transactions (Fact)
-- ==========================================
CREATE TABLE IF NOT EXISTS dlh_silver.transactions (
    transaction_id      INT,
    client_id           INT,
    client_key          INT,
    card_id             INT,
    card_key            INT,
    date_time           TIMESTAMP,
    year                INT,
    month               INT,
    day                 INT,
    hour                INT,
    amount              DECIMAL(18,4),

    velocity_1h         INT,
    velocity_24h        INT,
    amount_velocity_1h  DECIMAL(18,4),
    amount_velocity_24h DECIMAL(18,4),

    use_chip            BOOLEAN,
    merchant_id         INT,
    merchant_city       STRING,
    merchant_state      STRING,
    zip                 STRING,
    mcc                 INT,
    errors              STRING,
    updated_at          TIMESTAMP
)
PARTITIONED BY (year, month)
CLUSTERED BY (client_id) INTO 32 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true',
    'hive.exec.dynamic.partition'='true',
    'hive.exec.dynamic.partition.mode'='nonstrict'
);

-- ==========================================
-- MCC Codes (Dimension lookup)
-- ==========================================
CREATE TABLE IF NOT EXISTS dlh_silver.mcc_codes (
    mcc         INT,
    description STRING,
    updated_at  TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- ==========================================
-- Fraud Labels (Training data)
-- ==========================================
CREATE TABLE IF NOT EXISTS dlh_silver.train_fraud_labels (
    transaction_id  INT,
    label           TINYINT,                -- 0 or 1
    updated_at      TIMESTAMP
)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);
