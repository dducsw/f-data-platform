-- KPI Overview Metrics
-- Description: High-level KPIs for executive dashboard
-- Source: dlh_gold.transaction_summary
-- Update: Daily

-- Overall KPIs (Last 30 days)
SELECT 
    -- Date range
    MIN(txn_date) as period_start,
    MAX(txn_date) as period_end,
    COUNT(DISTINCT txn_date) as active_days,
    
    -- Transaction metrics
    SUM(txn_cnt) as total_transactions,
    SUM(txn_total) as total_revenue,
    AVG(txn_avg) as avg_transaction_value,
    
    -- Customer metrics
    SUM(unique_customers) as total_active_customers,
    SUM(unique_merchants) as total_active_merchants,
    
    -- Fraud metrics
    SUM(txn_cnt_fraud) as total_fraud_txns,
    ROUND(AVG(fraud_rate), 2) as avg_fraud_rate,
    SUM(txn_total_fraud) as total_fraud_amount,
    
    -- Growth metrics
    ROUND(SUM(txn_total) / COUNT(DISTINCT txn_date), 2) as daily_avg_revenue,
    ROUND(SUM(txn_cnt) / COUNT(DISTINCT txn_date), 2) as daily_avg_transactions
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, '%Y-%m-%d')
    AND txn_date <= DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d');


-- Daily KPI Trend (Last 30 days)
SELECT 
    txn_date,
    txn_cnt as daily_transactions,
    txn_total as daily_revenue,
    txn_avg as avg_ticket_size,
    unique_customers,
    unique_merchants,
    fraud_rate,
    fraud_amount_rate
FROM dlh_gold.transaction_summary
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, '%Y-%m-%d')
ORDER BY txn_date DESC;


-- Week-over-Week Growth
WITH weekly_metrics AS (
    SELECT 
        DATE_TRUNC('week', CAST(txn_date AS DATE)) as week_start,
        SUM(txn_total) as weekly_revenue,
        SUM(txn_cnt) as weekly_transactions,
        AVG(fraud_rate) as weekly_fraud_rate
    FROM dlh_gold.transaction_summary
    WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '8' WEEK, '%Y-%m-%d')
    GROUP BY DATE_TRUNC('week', CAST(txn_date AS DATE))
),
growth_calc AS (
    SELECT 
        week_start,
        weekly_revenue,
        weekly_transactions,
        weekly_fraud_rate,
        LAG(weekly_revenue) OVER (ORDER BY week_start) as prev_week_revenue,
        LAG(weekly_transactions) OVER (ORDER BY week_start) as prev_week_transactions
    FROM weekly_metrics
)
SELECT 
    DATE_FORMAT(week_start, '%Y-%m-%d') as week,
    weekly_revenue,
    weekly_transactions,
    weekly_fraud_rate,
    ROUND(((weekly_revenue - prev_week_revenue) / NULLIF(prev_week_revenue, 0)) * 100, 2) as revenue_growth_pct,
    ROUND(((weekly_transactions - prev_week_transactions) / NULLIF(prev_week_transactions, 0)) * 100, 2) as txn_growth_pct
FROM growth_calc
WHERE prev_week_revenue IS NOT NULL
ORDER BY week_start DESC
LIMIT 8;
