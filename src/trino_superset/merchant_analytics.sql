-- Merchant Analytics Metrics
-- Description: Merchant performance and ranking queries
-- Source: dlh_gold.merchant_performance
-- Update: Daily

-- Top Merchants by Revenue (Current snapshot)
SELECT 
    merchant_id,
    merchant_name,
    merchant_category,
    ROUND(txn_total, 2) as total_revenue,
    txn_cnt as total_transactions,
    ROUND(txn_avg, 2) as avg_transaction_value,
    unique_customers,
    ROUND(avg_txn_per_customer, 2) as avg_txn_per_customer,
    new_customers_cnt,
    ROUND(returning_customer_rate, 2) as returning_rate_pct,
    risk_level,
    ROUND(fraud_rate, 2) as fraud_rate_pct,
    active_days
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
ORDER BY txn_total DESC
LIMIT 100;


-- Merchant Category Performance
SELECT 
    merchant_category,
    COUNT(DISTINCT merchant_id) as merchant_count,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(txn_total), 2) as avg_revenue_per_merchant,
    SUM(txn_cnt) as total_transactions,
    ROUND(AVG(txn_avg), 2) as avg_transaction_value,
    SUM(unique_customers) as total_customers,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate,
    SUM(CASE WHEN risk_level = 'High' THEN 1 ELSE 0 END) as high_risk_count
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
GROUP BY merchant_category
ORDER BY total_revenue DESC;


-- Merchant Growth Leaders (Top 50 by customer acquisition)
SELECT 
    merchant_id,
    merchant_name,
    merchant_category,
    unique_customers,
    new_customers_cnt,
    ROUND((new_customers_cnt * 100.0) / NULLIF(unique_customers, 0), 2) as new_customer_pct,
    ROUND(returning_customer_rate, 2) as returning_rate_pct,
    ROUND(txn_total, 2) as total_revenue,
    txn_cnt as total_transactions
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
    AND unique_customers >= 10
ORDER BY new_customers_cnt DESC
LIMIT 50;


-- Merchant Retention Champions (High returning customer rate)
SELECT 
    merchant_id,
    merchant_name,
    merchant_category,
    unique_customers,
    ROUND(returning_customer_rate, 2) as returning_rate_pct,
    ROUND(avg_txn_per_customer, 2) as avg_txn_per_customer,
    ROUND(txn_total, 2) as total_revenue,
    active_days,
    top_customer_city
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
    AND unique_customers >= 20
    AND returning_customer_rate >= 50
ORDER BY returning_customer_rate DESC
LIMIT 50;


-- Merchant Risk Distribution
SELECT 
    risk_level,
    merchant_category,
    COUNT(*) as merchant_count,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(SUM(txn_total_fraud), 2) as total_fraud_amount,
    ROUND(AVG(txn_cnt), 2) as avg_transactions
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
GROUP BY risk_level, merchant_category
ORDER BY risk_level, merchant_category;


-- Geographic Merchant Distribution (Top cities)
SELECT 
    top_customer_city,
    COUNT(*) as merchant_count,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(txn_total), 2) as avg_revenue_per_merchant,
    SUM(unique_customers) as total_customers
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
    AND top_customer_city IS NOT NULL
GROUP BY top_customer_city
HAVING merchant_count >= 5
ORDER BY total_revenue DESC
LIMIT 50;


-- Merchant Lifetime Analysis
SELECT 
    CASE 
        WHEN active_days <= 30 THEN '0-30 days'
        WHEN active_days <= 90 THEN '31-90 days'
        WHEN active_days <= 180 THEN '91-180 days'
        WHEN active_days <= 365 THEN '181-365 days'
        ELSE '365+ days'
    END as tenure_bucket,
    COUNT(*) as merchant_count,
    ROUND(AVG(txn_total), 2) as avg_revenue,
    ROUND(AVG(unique_customers), 2) as avg_customers,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.merchant_performance
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dlh_gold.merchant_performance)
GROUP BY tenure_bucket
ORDER BY tenure_bucket;
