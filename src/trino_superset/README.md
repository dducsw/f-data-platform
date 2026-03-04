# Trino + Superset SQL Metrics

SQL query files cho dashboard sử dụng **Trino** query engine và **Apache Superset** visualization.

## 📁 File Structure

```
trino_superset/
├── kpi_overview.sql              # Executive KPIs và tổng quan
├── fraud_analytics.sql           # Phân tích gian lận
├── customer_analytics.sql        # Phân tích khách hàng
├── merchant_analytics.sql        # Phân tích merchant
├── time_series_trends.sql        # Xu hướng theo thời gian
├── geographic_analytics.sql      # Phân tích địa lý
└── README.md                     # Documentation
```

## 🎯 Query Categories

### 1. KPI Overview (`kpi_overview.sql`)
**Use case:** Executive dashboard, high-level metrics

**Queries:**
- Overall KPIs (Last 30 days)
- Daily KPI Trend
- Week-over-Week Growth

**Key metrics:**
- Total revenue, transactions
- Active customers, merchants
- Fraud rate, fraud amount
- Daily/weekly averages
- Growth percentages

---

### 2. Fraud Analytics (`fraud_analytics.sql`)
**Use case:** Fraud detection, risk monitoring

**Queries:**
- Fraud Overview (Last 7 days)
- High-Risk Merchants
- High-Risk Customers
- Fraud by Category
- Fraud by State/City
- Hourly Fraud Pattern

**Key metrics:**
- Fraud rate by dimension
- Risk level classification
- Geographic fraud hotspots
- Temporal fraud patterns

---

### 3. Customer Analytics (`customer_analytics.sql`)
**Use case:** Customer segmentation, behavior analysis

**Queries:**
- Top Customers by Revenue
- Customer Segmentation (RFM-like)
- Demographics Distribution
- Age Group Analysis
- Job Title Analysis
- City-wise Distribution
- Card Usage Patterns

**Key metrics:**
- Customer lifetime value
- RFM segments (Recency, Frequency, Monetary)
- Demographic breakdowns
- Geographic distribution

---

### 4. Merchant Analytics (`merchant_analytics.sql`)
**Use case:** Merchant performance, ranking

**Queries:**
- Top Merchants by Revenue
- Category Performance
- Growth Leaders (Customer acquisition)
- Retention Champions
- Risk Distribution
- Geographic Distribution
- Lifetime Analysis

**Key metrics:**
- Revenue, transaction volume
- Customer acquisition/retention
- Risk levels
- Category performance

---

### 5. Time Series Trends (`time_series_trends.sql`)
**Use case:** Trend analysis, forecasting

**Queries:**
- Daily Trend (Last 90 days)
- Weekly Aggregation
- Monthly Aggregation
- Day of Week Pattern
- Hourly Pattern
- Month-over-Month Growth
- Year-over-Year Comparison

**Key metrics:**
- Revenue/transaction trends
- Growth rates (MoM, YoY, WoW)
- Seasonal patterns
- Peak hours/days

---

### 6. Geographic Analytics (`geographic_analytics.sql`)
**Use case:** Location-based insights

**Queries:**
- State-wise Revenue Distribution
- City-wise Revenue (Top 100)
- Top Cities by Daily Revenue
- Geographic Density Map
- State-wise Demographics
- Distance-based Analysis
- Regional Fraud Hotspots

**Key metrics:**
- Revenue by location
- Customer density
- Distance patterns
- Fraud hotspots by coordinates

---

## 🔧 Usage in Superset

### 1. Create Database Connection
```sql
-- In Superset: Database Connections
Type: Trino
SQLAlchemy URI: trino://localhost:8080/hive
```

### 2. Create Dataset
```
1. SQL Lab → Select database
2. Paste query from .sql file
3. Run query to preview
4. Save as Dataset with meaningful name
```

### 3. Build Charts
```
1. Charts → Create new chart
2. Select dataset
3. Choose visualization type:
   - Time Series: Line chart
   - Comparison: Bar chart
   - Distribution: Pie/Donut
   - Ranking: Table
   - Geographic: Map
4. Configure metrics and dimensions
```

### 4. Create Dashboard
```
1. Dashboards → Create new
2. Add charts
3. Configure filters (date range, state, category)
4. Set refresh intervals
```

---

## 📊 Recommended Dashboards

### Dashboard 1: Executive Overview
**Charts:**
- KPI Cards (revenue, transactions, customers)
- Daily trend line chart
- Week-over-week bar chart
- Top categories pie chart

**Filters:**
- Date range
- Customer state

---

### Dashboard 2: Fraud Monitoring
**Charts:**
- Fraud rate trend (last 7 days)
- High-risk merchants table
- Fraud by category bar chart
- Geographic heatmap (fraud hotspots)
- Hourly pattern line chart

**Filters:**
- Date range
- Risk level
- State

---

### Dashboard 3: Customer Insights
**Charts:**
- Customer segmentation scatter plot
- Age group distribution
- Top customers table
- City-wise revenue map
- Card usage pattern

**Filters:**
- Customer segment
- State
- Age group

---

### Dashboard 4: Merchant Performance
**Charts:**
- Top merchants table
- Category comparison bar chart
- Retention vs acquisition scatter
- Risk distribution sunburst
- Geographic merchant density

**Filters:**
- Category
- Risk level
- City

---

## ⚙️ Query Optimization Tips

### 1. Partitioning
```sql
-- Always filter by partition column
WHERE partition >= 'yyyyMMdd'  -- For transaction_details
WHERE txn_date >= 'yyyy-MM-dd'  -- For transaction_summary
WHERE snapshot_date = 'yyyy-MM-dd'  -- For merchant_performance
```

### 2. Limit Results
```sql
-- Use LIMIT for large datasets
SELECT ... LIMIT 100;

-- Filter by minimum thresholds
HAVING transaction_count >= 10
```

### 3. Date Filters
```sql
-- Use interval functions
WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, '%Y-%m-%d')

-- Avoid open-ended queries
-- BAD: WHERE txn_date >= '2020-01-01'
-- GOOD: WHERE txn_date BETWEEN '2026-01-01' AND '2026-01-31'
```

### 4. Aggregation
```sql
-- Pre-aggregate when possible
-- Use transaction_summary instead of transaction_details for daily stats

-- Push down filters before joins
WITH filtered_txns AS (
    SELECT ... FROM transactions WHERE date >= ...
)
SELECT ... FROM filtered_txns JOIN ...
```

---

## 🔄 Refresh Schedule

**Real-time dashboards:** Not supported (batch processing)

**Recommended refresh:**
- **Executive KPIs:** Every 1 hour
- **Fraud monitoring:** Every 30 minutes
- **Customer/Merchant analytics:** Every 4 hours
- **Trend analysis:** Daily

**Cache strategy:**
- Enable Superset caching for slow queries
- Set appropriate TTL based on data freshness needs

---

## 📝 Notes

1. **Date format consistency:**
   - `transaction_details.partition`: 'yyyyMMdd'
   - `transaction_summary.txn_date`: 'yyyy-MM-dd' STRING
   - `merchant_performance.snapshot_date`: DATE type

2. **Current data:**
   - `merchant_performance`: Use `MAX(snapshot_date)` for latest snapshot
   - `transaction_summary`: Latest partition for current day
   - `customer_stats`: Computed from all historical data

3. **Performance:**
   - Queries scan gold layer (optimized, aggregated)
   - Partition pruning enabled
   - Use LIMIT for exploratory analysis

4. **Customization:**
   - All queries are templates
   - Modify date ranges as needed
   - Add/remove dimensions based on requirements
   - Adjust thresholds in HAVING clauses

---

## 🚀 Getting Started

1. **Test queries in Trino CLI:**
   ```bash
   trino --server localhost:8080 --catalog hive --schema dlh_gold
   ```

2. **Run sample query:**
   ```sql
   SELECT * FROM dlh_gold.transaction_summary 
   WHERE txn_date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '7' DAY, '%Y-%m-%d')
   LIMIT 10;
   ```

3. **Import to Superset:**
   - Create database connection
   - Create datasets from queries
   - Build visualizations
   - Combine into dashboards

4. **Share dashboards:**
   - Set permissions
   - Schedule email reports
   - Embed in applications

---

## 📧 Support

For questions or improvements, refer to:
- **Trino Documentation:** https://trino.io/docs/current/
- **Superset Documentation:** https://superset.apache.org/docs/intro
- **Gold Layer Schema:** See `src/spark/transform/` directory
