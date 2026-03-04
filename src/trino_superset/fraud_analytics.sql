-- Fraud Analytics Metrics
-- Description: Fraud detection and monitoring queries
-- Source: dlh_gold.transaction_summary, dlh_gold.merchant_performance, dlh_gold.customer_stats
-- Update: Daily

-- Fraud Overview (Last 7 days)
SELECT 
    txn_date,
    txn_cnt_fraud as fraud_transactions,
    txn_cnt_legit as legitimate_transactions,
    ROUND(fraud_rate, 2) as fraud_rate_pct,
    txn_total_fraud as fraud_amount,
    txn_total_legit as legitimate_amount,
    ROUND(fraud_amount_rate, 2) as fraud_amount_rate_pct
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '7' DAY, '%Y-%m-%d')
ORDER BY txn_date DESC;


-- High-Risk Merchants (Current snapshot)
SELECT 
    merchant_id,
    merchant_name,
    merchant_category,
    risk_level,
    txn_cnt,
    txn_cnt_fraud,
    ROUND(fraud_rate, 2) as fraud_rate_pct,
    ROUND(txn_total, 2) as total_revenue,
    ROUND(txn_total_fraud, 2) as fraud_revenue,
    unique_customers,
    ROUND(avg_txn_per_customer, 2) as avg_txn_per_customer
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
    AND risk_level IN ('High', 'Medium')
ORDER BY fraud_rate DESC
LIMIT 50;


-- High-Risk Customers (Top 100 by fraud rate)
SELECT 
    customer_id,
    customer_city,
    customer_state,
    customer_job,
    txn_cnt,
    txn_cnt_fraud,
    ROUND(fraud_rate, 2) as fraud_rate_pct,
    ROUND(txn_total, 2) as total_spent,
    ROUND(txn_total_fraud, 2) as fraud_amount,
    unique_cards,
    unique_merchants
FROM dlh_gold.customer_stats
WHERE txn_cnt_fraud > 0
ORDER BY fraud_rate DESC, txn_cnt_fraud DESC
LIMIT 100;


-- Fraud by Category (Last 30 days aggregated)
SELECT 
    COALESCE(txn_category, 'UNKNOWN') as category,
    COUNT(*) as fraud_count,
    SUM(txn_amount) as total_fraud_amount,
    AVG(txn_amount) as avg_fraud_amount,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT merchant_id) as unique_merchants
FROM dlh_gold.transaction_details
WHERE is_fraud = 1
    AND partition >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, 'yyyyMMdd')
GROUP BY txn_category
ORDER BY fraud_count DESC;


-- Fraud by State/City (Geographic fraud hotspots)
SELECT 
    customer_state,
    customer_city,
    COUNT(*) as fraud_txn_count,
    ROUND(SUM(txn_amount), 2) as total_fraud_amount,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(AVG(txn_amount), 2) as avg_fraud_txn
FROM dlh_gold.transaction_details
WHERE is_fraud = 1
    AND partition >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, 'yyyyMMdd')
GROUP BY customer_state, customer_city
HAVING fraud_txn_count >= 5
ORDER BY fraud_txn_count DESC
LIMIT 50;


-- Hourly Fraud Pattern (Last 7 days)
SELECT 
    HOUR(txn_datetime) as hour_of_day,
    COUNT(*) as total_txns,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_txns,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN txn_amount ELSE 0 END), 2) as fraud_amount
FROM dlh_gold.transaction_details
WHERE partition >= DATE_FORMAT(CURRENT_DATE - INTERVAL '7' DAY, 'yyyyMMdd')
GROUP BY HOUR(txn_datetime)
ORDER BY hour_of_day;
