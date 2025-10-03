CREATE DATABASE IF NOT EXISTS dlh_bronze
COMMENT 'Bronze Layer for Finance Lakehouse';

USE dlh_bronze;

CREATE TABLE IF NOT EXISTS dlh_bronze.users (
    id                  INT,
    current_age         INT,
    retirement_age      INT,
    birth_year          INT,
    birth_month         INT,
    gender              STRING,
    address             STRING,
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    per_capita_income   STRING,
    yearly_income       STRING,
    total_debt          STRING,
    credit_score        INT,
    num_credit_cards    INT,
    load_at             TIMESTAMP
)
CLUSTERED BY (id) INTO 10 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

CREATE TABLE IF NOT EXISTS dlh_bronze.cards (
    id                  INT,
    client_id           INT,
    card_brand          STRING,
    card_type           STRING,
    card_number         STRING,
    expires             STRING,
    CVV                 STRING,
    has_chip            STRING,
    num_cards_issued    INT,
    credit_limit        STRING,
    acct_open_date      STRING,
    year_pin_last_changed INT,
    card_on_dark_web    STRING,
    load_at             TIMESTAMP
)
CLUSTERED BY (id) INTO 8 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

CREATE TABLE IF NOT EXISTS dlh_bronze.transactions (
    id                  INT,
    date_time           TIMESTAMP,
    client_id           INT,
    card_id             INT,
    amount              STRING,
    use_chip            STRING,
    merchant_id         INT,
    merchant_city       STRING,
    merchant_state      STRING,
    zip                 STRING,
    mcc                 INT,
    errors              STRING,
    load_at             TIMESTAMP
)
PARTITIONED BY (date_partition STRING)
CLUSTERED BY (client_id) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

CREATE TABLE IF NOT EXISTS dlh_bronze.mcc_codes (
    mcc_code    INT,
    description      STRING,
    file_name        STRING,
    load_at          TIMESTAMP
)
CLUSTERED BY (transition_id) INTO 4 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

CREATE TABLE IF NOT EXISTS dlh_bronze.train_fraud_labels (
    transition_id    INT,
    label           STRING,
    file_name       STRING,
    load_at         TIMESTAMP
)
CLUSTERED BY (transition_id) INTO 4 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

ALTER TABLE users UNSET TBLPROPERTIES ('EXTERNAL');
ALTER TABLE cards UNSET TBLPROPERTIES ('EXTERNAL');
ALTER TABLE transactions UNSET TBLPROPERTIES ('EXTERNAL');
ALTER TABLE mcc_codes UNSET TBLPROPERTIES ('EXTERNAL');
ALTER TABLE train_fraud_labels UNSET TBLPROPERTIES ('EXTERNAL');