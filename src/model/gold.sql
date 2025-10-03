-- Tạo database gold layer
CREATE DATABASE IF NOT EXISTS dlh_gold
COMMENT 'Gold Layer for Finance Lakehouse';

USE dlh_gold;

-- Bảng transaction_detail
CREATE TABLE IF NOT EXISTS dlh_gold.transaction_detail (
    transaction_id          INT,

    client_key              INT,
    client_id               INT,
    client_age_at_trans     INT,
    client_age_group_at_trans STRING,
    client_income_group     STRING,
    client_state            STRING,
    
    card_key                INT,
    card_id                 INT,
    card_brand              STRING,
    card_type               STRING,
    credit_limit            INT,
    amount                  DECIMAL(10,2),

    velocity_1h         INT,
    velocity_24h        INT,
    amount_velocity_1h  INT,
    amount_velocity_24h INT,

    spending_ratio          DECIMAL(5,2),
    use_chip                STRING,

    date_time               TIMESTAMP,
    year                    INT,
    month                   INT,
    day                     INT,
    hour                    INT,
    time                    STRING,

    merchant_id             INT,
    merchant_city           STRING,
    merchant_state          STRING,
    zip                     STRING,
    mcc                     INT,
    mcc_description         STRING,
    is_error                BOOLEAN,
    is_fraud                BOOLEAN,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITIONED BY (year INT, month INT, day INT)
CLUSTERED BY (client_id) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true',
    'hive.exec.dynamic.partition'='true',
    'hive.exec.dynamic.partition.mode'='nonstrict'
);

-- Bảng user_detail
CREATE TABLE IF NOT EXISTS dlh_gold.user_detail (
    client_id               INT,
    client_key              INT,
    age_current             INT,
    age_group               STRING,
    birth_year              INT,
    gender                  STRING,
    is_retired              BOOLEAN,
    income_per_capita       INT,
    income_yearly           INT,
    income_group            STRING,
    debt_total              INT,
    debt_to_income_ratio    DECIMAL(5,2),
    credit_score            INT,
    spending_total          INT,
    txn_total               INT,
    txn_online_total        INT,
    txn_fraud_total         INT,
    txn_error_total         INT,
    lifetime_first_date     DATE,
    lifetime_last_date      DATE,
    lifetime_days           INT,
    merchant_diversity_ratio DECIMAL(5,2),
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTERED BY (client_id) INTO 10 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true'
);

-- Bảng market_daily_summary
CREATE TABLE IF NOT EXISTS dlh_gold.market_daily_summary (
    period_date             DATE,
    txn_total_amt           DECIMAL(20,2),
    txn_total_cnt           INT,
    txn_avg_amt             DECIMAL(12,2)
    fraud_txn_cnt           INT,
    fraud_rate              DECIMAL(8,4),,
    error_txn_cnt           INT,
    error_rate              DECIMAL(8,4),
    growth_rate             DECIMAL(8,4),
    growth_rate_7d          DECIMAL(8,4),
    growth_rate_30d         DECIMAL(8,4),
    unique_client_cnt       INT,
    new_client_cnt          INT,
    active_merchant_cnt     INT,
    top_mcc                 INT,
    peak_hour               INT,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true'
);

-- Bảng merchant_monthly_performance
CREATE TABLE IF NOT EXISTS dlh_gold.merchant_monthly_performance (
    merchant_id             INT,
    period_month            DATE,
    active_city_cnt         INT,
    active_state_cnt        INT,
    active_mcc_cnt          INT,

    revenue_total           DECIMAL(18,2),
    revenue_growth_rate     DECIMAL(8,4),

    txn_total               INT,
    txn_total_cnt           INT,
    txn_growth_rate         DECIMAL(8,4),

    chargeback_total_amt    DECIMAL(18,2),
    chargeback_txn_cnt      INT,
    chargeback_rate         DECIMAL(8,4),

    fraud_txn_cnt           INT,
    fraud_rate              DECIMAL(8,4),
    error_txn_cnt           INT,
    error_rate              DECIMAL(8,4),

    unique_client_cnt       INT,
    new_client_cnt          INT,
    returning_client_cnt    INT,

    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITIONED BY (year INT, month INT)
CLUSTERED BY (merchant_id) INTO 8 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true'
);


