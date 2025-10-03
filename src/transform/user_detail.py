


create_table = """
CREATE TABLE IF NOT EXISTS dlh_gold.user_detail (
    client_id               INT,
    client_key              INT,

    age_current             INT,
    age_retired             INT,
    age_group               STRING,
    is_retired              BOOLEAN,
    
    birth_year              INT,
    birth_month             INT,
    gender                  STRING,
    
    income_per_capita       INT,
    income_yearly           INT,
    income_group            STRING,

    debt_total              INT,
    debt_to_income_ratio    DECIMAL(5,2),

    credit_score            INT,
    total_spending          INT,

    total_txn               INT,
    total_txn_online        INT,
    total_txn_fraud         INT,
    total_error_txn         INT,

    lifetime_first_date     DATE,
    lifetime_last_date      DATE,
    lifetime_days           INT,

    merchant_number         INT,
    merchant_diversity_ratio DECIMAL(5,2),
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""