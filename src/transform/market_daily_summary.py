

create_table = """
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

    client_unique_cnt       INT,
    client_new_cnt          INT,

    merchant_active_cnt     INT,
    merchant_top_mcc        INT,
    
    peak_hour               INT,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.enable.dictionary'='true'
);
"""