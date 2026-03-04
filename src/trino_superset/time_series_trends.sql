-- Time Series Trends Metrics
-- Description: Temporal patterns and trends analysis
-- Source: dlh_gold.transaction_summary
-- Update: Daily

-- Daily Trend (Last 90 days)
SELECT 
    txn_date,
    txn_cnt as transactions,
    ROUND(txn_total, 2) as revenue,
    ROUND(txn_avg, 2) as avg_transaction_value,
    unique_customers,
    unique_merchants,
    ROUND(fraud_rate, 2) as fraud_rate_pct,
    peak_hour
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '90' DAY, '%Y-%m-%d')
ORDER BY txn_date;


-- Weekly Aggregation (Last 12 weeks)
SELECT 
    DATE_FORMAT(DATE_TRUNC('week', CAST(txn_date AS DATE)), '%Y-%m-%d') as week_start,
    SUM(txn_cnt) as weekly_transactions,
    ROUND(SUM(txn_total), 2) as weekly_revenue,
    ROUND(AVG(txn_avg), 2) as avg_transaction_value,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate,
    SUM(unique_customers) as total_customers,
    SUM(unique_merchants) as total_merchants
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '12' WEEK, '%Y-%m-%d')
GROUP BY DATE_TRUNC('week', CAST(txn_date AS DATE))
ORDER BY week_start DESC;


-- Monthly Aggregation (Last 12 months)
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', CAST(txn_date AS DATE)), '%Y-%m') as month,
    SUM(txn_cnt) as monthly_transactions,
    ROUND(SUM(txn_total), 2) as monthly_revenue,
    ROUND(AVG(txn_avg), 2) as avg_transaction_value,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate,
    SUM(unique_customers) as total_customers,
    SUM(unique_merchants) as total_merchants,
    COUNT(DISTINCT txn_date) as active_days
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '12' MONTH, '%Y-%m-%d')
GROUP BY DATE_TRUNC('month', CAST(txn_date AS DATE))
ORDER BY month DESC;


-- Day of Week Pattern (Last 8 weeks)
SELECT 
    CASE DAYOFWEEK(CAST(txn_date AS DATE))
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_of_week,
    COUNT(*) as day_count,
    ROUND(AVG(txn_cnt), 2) as avg_transactions,
    ROUND(AVG(txn_total), 2) as avg_revenue,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '8' WEEK, '%Y-%m-%d')
GROUP BY DAYOFWEEK(CAST(txn_date AS DATE))
ORDER BY DAYOFWEEK(CAST(txn_date AS DATE));


-- Hourly Pattern (Aggregated from last 30 days)
SELECT 
    peak_hour as hour,
    COUNT(*) as days_with_peak,
    ROUND(AVG(peak_hour_txn_cnt), 2) as avg_peak_txns,
    ROUND(SUM(peak_hour_txn_cnt), 0) as total_peak_txns
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, '%Y-%m-%d')
    AND peak_hour IS NOT NULL
GROUP BY peak_hour
ORDER BY hour;


-- Month-over-Month Growth
WITH monthly_stats AS (
    SELECT 
        DATE_FORMAT(DATE_TRUNC('month', CAST(txn_date AS DATE)), '%Y-%m') as month,
        SUM(txn_total) as monthly_revenue,
        SUM(txn_cnt) as monthly_transactions,
        AVG(fraud_rate) as monthly_fraud_rate
    FROM dlh_gold.transaction_summary
    WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '12' MONTH, '%Y-%m-%d')
    GROUP BY DATE_TRUNC('month', CAST(txn_date AS DATE))
),
growth_calc AS (
    SELECT 
        month,
        monthly_revenue,
        monthly_transactions,
        monthly_fraud_rate,
        LAG(monthly_revenue) OVER (ORDER BY month) as prev_month_revenue,
        LAG(monthly_transactions) OVER (ORDER BY month) as prev_month_txns
    FROM monthly_stats
)
SELECT 
    month,
    ROUND(monthly_revenue, 2) as revenue,
    monthly_transactions as transactions,
    ROUND(monthly_fraud_rate, 2) as fraud_rate,
    ROUND(((monthly_revenue - prev_month_revenue) / NULLIF(prev_month_revenue, 0)) * 100, 2) as revenue_growth_pct,
    ROUND(((monthly_transactions - prev_month_txns) / NULLIF(prev_month_txns, 0)) * 100, 2) as txn_growth_pct
FROM growth_calc
WHERE prev_month_revenue IS NOT NULL
ORDER BY month DESC;


-- Year-over-Year Comparison (Same month last year)
WITH current_month AS (
    SELECT 
        DATE_FORMAT(DATE_TRUNC('month', CURRENT_DATE), '%m') as month_num,
        SUM(txn_total) as current_revenue,
        SUM(txn_cnt) as current_txns
    FROM dlh_gold.transaction_summary
    WHERE txn_date >= DATE_FORMAT(DATE_TRUNC('month', CURRENT_DATE), '%Y-%m-%d')
),
last_year_month AS (
    SELECT 
        SUM(txn_total) as last_year_revenue,
        SUM(txn_cnt) as last_year_txns
    FROM dlh_gold.transaction_summary
    WHERE txn_date >= DATE_FORMAT(DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' YEAR, '%Y-%m-%d')
        AND txn_date < DATE_FORMAT(DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' YEAR + INTERVAL '1' MONTH, '%Y-%m-%d')
)
SELECT 
    c.current_revenue,
    c.current_txns,
    l.last_year_revenue,
    l.last_year_txns,
    ROUND(((c.current_revenue - l.last_year_revenue) / NULLIF(l.last_year_revenue, 0)) * 100, 2) as yoy_revenue_growth_pct,
    ROUND(((c.current_txns - l.last_year_txns) / NULLIF(l.last_year_txns, 0)) * 100, 2) as yoy_txn_growth_pct
FROM current_month c
CROSS JOIN last_year_month l;
