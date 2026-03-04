-- Geographic Analytics Metrics
-- Description: Location-based analysis and patterns
-- Source: dlh_gold.transaction_details, dlh_gold.customer_stats, dlh_gold.transaction_summary
-- Update: Daily

-- State-wise Revenue Distribution (Last 30 days)
SELECT 
    customer_state,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_transactions,
    ROUND(SUM(txn_amount), 2) as total_revenue,
    ROUND(AVG(txn_amount), 2) as avg_transaction_value,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct
FROM dlh_gold.transaction_details
WHERE partition >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, 'yyyyMMdd')
    AND customer_state IS NOT NULL
GROUP BY customer_state
ORDER BY total_revenue DESC;


-- City-wise Revenue Distribution (Top 100 cities)
SELECT 
    customer_city,
    customer_state,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_transactions,
    ROUND(SUM(txn_amount), 2) as total_revenue,
    ROUND(AVG(txn_amount), 2) as avg_transaction_value,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN txn_amount ELSE 0 END), 2) as fraud_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct
FROM dlh_gold.transaction_details
WHERE partition >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, 'yyyyMMdd')
    AND customer_city IS NOT NULL
    AND customer_state IS NOT NULL
GROUP BY customer_city, customer_state
HAVING total_transactions >= 10
ORDER BY total_revenue DESC
LIMIT 100;


-- Top Cities by Daily Revenue (from transaction_summary)
SELECT 
    txn_date,
    top_city,
    ROUND(top_city_revenue, 2) as revenue,
    top_category,
    top_category_cnt
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, '%Y-%m-%d')
    AND top_city IS NOT NULL
ORDER BY txn_date DESC, top_city_revenue DESC
LIMIT 100;


-- Geographic Density Map (Customer concentration)
SELECT 
    customer_state,
    customer_city,
    customer_city_pop,
    COUNT(*) as customer_count,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(customer_lat), 4) as avg_latitude,
    ROUND(AVG(customer_long), 4) as avg_longitude
FROM dlh_gold.customer_stats
WHERE customer_city IS NOT NULL 
    AND customer_state IS NOT NULL
    AND customer_lat IS NOT NULL
    AND customer_long IS NOT NULL
GROUP BY customer_state, customer_city, customer_city_pop
HAVING customer_count >= 5
ORDER BY customer_count DESC;


-- State-wise Customer Demographics
SELECT 
    customer_state,
    COUNT(*) as total_customers,
    ROUND(AVG(customer_age), 1) as avg_age,
    ROUND(SUM(txn_total), 2) as total_revenue,
    ROUND(AVG(txn_total), 2) as avg_customer_value,
    ROUND(AVG(txn_cnt), 2) as avg_transactions_per_customer,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.customer_stats
WHERE customer_state IS NOT NULL
GROUP BY customer_state
ORDER BY total_revenue DESC;


-- Distance-based Analysis (Merchant to Customer)
-- Calculate distance between customer and merchant coordinates
SELECT 
    ROUND(
        SQRT(
            POW((merch_lat - customer_lat) * 111, 2) + 
            POW((merch_long - customer_long) * 111 * COS(RADIANS(customer_lat)), 2)
        ), 
        2
    ) as distance_km,
    COUNT(*) as transaction_count,
    ROUND(AVG(txn_amount), 2) as avg_transaction_value,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct
FROM dlh_gold.transaction_details
WHERE partition >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, 'yyyyMMdd')
    AND customer_lat IS NOT NULL
    AND customer_long IS NOT NULL
    AND merch_lat IS NOT NULL
    AND merch_long IS NOT NULL
GROUP BY ROUND(
    SQRT(
        POW((merch_lat - customer_lat) * 111, 2) + 
        POW((merch_long - customer_long) * 111 * COS(RADIANS(customer_lat)), 2)
    ), 
    2
)
HAVING transaction_count >= 10
ORDER BY distance_km;


-- Regional Fraud Hotspots (Lat/Long clusters)
SELECT 
    ROUND(customer_lat, 1) as lat_bucket,
    ROUND(customer_long, 1) as long_bucket,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN txn_amount ELSE 0 END), 2) as fraud_amount
FROM dlh_gold.transaction_details
WHERE partition >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, 'yyyyMMdd')
    AND customer_lat IS NOT NULL
    AND customer_long IS NOT NULL
    AND is_fraud = 1
GROUP BY ROUND(customer_lat, 1), ROUND(customer_long, 1)
HAVING fraud_count >= 5
ORDER BY fraud_rate_pct DESC
LIMIT 100;
