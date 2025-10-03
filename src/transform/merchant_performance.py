

create_table = """
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
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true'
);
"""