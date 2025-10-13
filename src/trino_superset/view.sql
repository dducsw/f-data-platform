DROP VIEW IF EXISTS dlh_gold.vw_transaction_summary;

CREATE VIEW dlh_gold.vw_transaction_summary AS
SELECT 
    card_brand,
    card_type,
    txn_use_chip,
    CAST(txn_date_time AS DATE) AS txn_date,
    SUM(txn_amount) AS total_amount,
    COUNT(transaction_id) AS total_txn
FROM dlh_gold.transaction_detail
WHERE CAST(txn_date_time AS DATE) = (
    SELECT MAX(CAST(txn_date_time AS DATE)) 
    FROM dlh_gold.transaction_detail
)
GROUP BY 
    card_brand,
    card_type,
    txn_use_chip,
    CAST(txn_date_time AS DATE);


