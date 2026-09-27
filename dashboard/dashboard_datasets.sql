-- Sahra Retail Lakehouse | Author: Imran Sheikh | https://www.linkedin.com/in/imranazhar/
-- =====================================================================================
-- SAHRA RETAIL  |  AI/BI Dashboard datasets
-- Create a new dashboard, open the "Data" tab, add one dataset per query below
-- (use the name in the header), then build the widgets listed in each comment.
-- =====================================================================================

-- ---------- Dataset: kpi_daily
-- Widgets: 4 counters (Net revenue, Gross margin %, Orders, Avg basket)
--          + line chart: x = order_date (weekly), y = SUM(net_revenue)
--          + global filters: date range on order_date, multi-select store_emirate / store_format
SELECT
  s.order_date,
  s.store_id,
  s.store_name,
  s.store_emirate,
  s.store_format,
  d.retail_event,
  d.is_weekend,
  s.orders,
  s.units,
  s.net_revenue,
  s.gross_margin,
  s.returned_orders,
  s.cancelled_orders
FROM workspace.sahra_gold.daily_store_sales s
JOIN workspace.sahra_gold.dim_date d ON s.order_date = d.date;

-- ---------- Dataset: store_map
-- Widgets: point map (latitude/longitude, size = revenue, colour = store_format)
--          + bar chart: store_name by revenue
SELECT
  store_name,
  store_emirate,
  store_format,
  latitude,
  longitude,
  round(sum(net_revenue))                              AS revenue,
  round(sum(gross_margin) / sum(net_revenue), 3)       AS margin_pct,
  round(sum(net_revenue) / sum(orders), 1)             AS avg_basket
FROM workspace.sahra_gold.daily_store_sales
WHERE latitude IS NOT NULL
GROUP BY ALL;

-- ---------- Dataset: category_monthly
-- Widgets: stacked bar (x = month, y = net_revenue, colour = category)
--          + table with margin_pct and mom_growth_pct (conditional formatting)
SELECT month, category, net_revenue, gross_margin, margin_pct, units, orders, mom_growth_pct
FROM workspace.sahra_gold.monthly_category_sales;

-- ---------- Dataset: event_uplift
-- Widget: bar chart "Revenue per day by retail event" (Regular = baseline)
SELECT
  d.retail_event,
  round(sum(s.net_revenue) / count(DISTINCT s.order_date))  AS revenue_per_day,
  count(DISTINCT s.order_date)                              AS days
FROM workspace.sahra_gold.daily_store_sales s
JOIN workspace.sahra_gold.dim_date d ON s.order_date = d.date
GROUP BY 1;

-- ---------- Dataset: top_products
-- Widget: table, top 15 by revenue, with category and margin_pct
SELECT product_name, category, brand, units, net_revenue, margin_pct, rank_in_category
FROM workspace.sahra_gold.product_performance
ORDER BY net_revenue DESC
LIMIT 15;

-- ---------- Dataset: customer_segments
-- Widgets: bar (customers per segment) + bar (revenue per segment) + pie of current_tier
SELECT segment, current_tier, emirate, count(*) AS customers, round(sum(monetary)) AS revenue,
       round(avg(recency_days)) AS avg_recency_days, round(avg(frequency), 1) AS avg_orders
FROM workspace.sahra_gold.customer_rfm
GROUP BY ALL;

-- ---------- Dataset: data_quality
-- Widget: small table on a "Pipeline health" page
SELECT 'Duplicate orders removed' AS check_name,
       (SELECT count(*) FROM workspace.sahra_bronze.orders_raw) - (SELECT count(*) FROM workspace.sahra_silver.orders) AS rows_affected
UNION ALL
SELECT concat('Quarantined: ', reject_reason), count(*) FROM workspace.sahra_silver.order_items_quarantine GROUP BY reject_reason
UNION ALL
SELECT 'Orders with unknown customer', count(DISTINCT order_id)
FROM workspace.sahra_gold.sales_enriched WHERE loyalty_tier_at_order = 'Unknown';
