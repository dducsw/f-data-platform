-- Customer Analytics Metrics
-- Description: Customer behavior and segmentation queries
-- Source: dlh_gold.customer_stats, dlh_gold.transaction_details
-- Update: Daily

-- Top Customers by Revenue (Top 100)
SELECT 
    customer_id,
    customer_gender,
    customer_city,
    customer_state,
    customer_job,
    customer_age,
    txn_cnt as total_transactions,
    ROUND(txn_total, 2) as total_revenue,
    ROUND(txn_avg, 2) as avg_transaction_value,
    unique_cards,
    unique_merchants,
    unique_categories,
    ROUND(fraud_rate, 2) as fraud_rate_pct,
    DATEDIFF('day', CAST(first_txn_date AS DATE), CAST(last_txn_date AS DATE)) as customer_tenure_days
FROM dlh_gold.customer_stats
ORDER BY txn_total DESC
LIMIT 100;


-- Customer Segmentation by Spending (RFM-like)
WITH customer_segments AS (
    SELECT 
        customer_id,
        txn_total as monetary,
        txn_cnt as frequency,
        DATEDIFF('day', CAST(last_txn_date AS DATE), CURRENT_DATE) as recency_days,
        CASE 
            WHEN txn_total >= 10000 THEN 'High Value'
            WHEN txn_total >= 5000 THEN 'Medium Value'
            WHEN txn_total >= 1000 THEN 'Low Value'
            ELSE 'Very Low Value'
        END as value_segment,
        CASE 
            WHEN DATEDIFF('day', CAST(last_txn_date AS DATE), CURRENT_DATE) <= 7 THEN 'Active'
            WHEN DATEDIFF('day', CAST(last_txn_date AS DATE), CURRENT_DATE) <= 30 THEN 'Recent'
            WHEN DATEDIFF('day', CAST(last_txn_date AS DATE), CURRENT_DATE) <= 90 THEN 'Inactive'
            ELSE 'Churned'
        END as activity_segment
    FROM dlh_gold.customer_stats
)
SELECT 
    value_segment,
    activity_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(monetary), 2) as avg_spend,
    ROUND(AVG(frequency), 2) as avg_frequency,
    ROUND(AVG(recency_days), 0) as avg_recency_days
FROM customer_segments
GROUP BY value_segment, activity_segment
ORDER BY value_segment, activity_segment;


-- Customer Demographics Distribution
SELECT 
    customer_gender,
    customer_state,
    COUNT(*) as customer_count,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(txn_total), 2) as avg_customer_value,
    ROUND(AVG(txn_cnt), 2) as avg_transactions_per_customer,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.customer_stats
WHERE customer_gender IS NOT NULL AND customer_state IS NOT NULL
GROUP BY customer_gender, customer_state
HAVING customer_count >= 10
ORDER BY total_revenue DESC
LIMIT 50;


-- Customer Age Group Analysis
SELECT 
    CASE 
        WHEN customer_age < 25 THEN '18-24'
        WHEN customer_age < 35 THEN '25-34'
        WHEN customer_age < 45 THEN '35-44'
        WHEN customer_age < 55 THEN '45-54'
        WHEN customer_age < 65 THEN '55-64'
        ELSE '65+'
    END as age_group,
    COUNT(*) as customer_count,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(txn_avg), 2) as avg_transaction_value,
    ROUND(AVG(txn_cnt), 2) as avg_transactions,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.customer_stats
WHERE customer_age IS NOT NULL
GROUP BY age_group
ORDER BY age_group;


-- Customer Job Title Analysis
SELECT 
    customer_job,
    COUNT(*) as customer_count,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(txn_total), 2) as avg_customer_value,
    ROUND(AVG(txn_avg), 2) as avg_transaction_value,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.customer_stats
WHERE customer_job IS NOT NULL
GROUP BY customer_job
HAVING customer_count >= 10
ORDER BY total_revenue DESC
LIMIT 30;


-- City-wise Customer Distribution and Revenue
SELECT 
    customer_city,
    customer_state,
    COUNT(*) as customer_count,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(txn_total), 2) as avg_customer_value,
    SUM(txn_cnt) as total_transactions,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.customer_stats
WHERE customer_city IS NOT NULL AND customer_state IS NOT NULL
GROUP BY customer_city, customer_state
HAVING customer_count >= 5
ORDER BY total_revenue DESC
LIMIT 50;


-- Customer Card Usage Patterns
SELECT 
    unique_cards,
    COUNT(*) as customer_count,
    ROUND(AVG(txn_total), 2) as avg_revenue,
    ROUND(AVG(txn_cnt), 2) as avg_transactions,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.customer_stats
GROUP BY unique_cards
ORDER BY unique_cards;
