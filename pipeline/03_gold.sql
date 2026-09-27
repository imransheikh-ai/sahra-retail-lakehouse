-- =====================================================================================
-- SAHRA RETAIL LAKEHOUSE  |  Lakeflow Declarative Pipeline  |  03 GOLD
-- Author: Imran Sheikh | https://www.linkedin.com/in/imranazhar/
-- -------------------------------------------------------------------------------------
-- Gold = business-ready tables shaped for the questions people ask.
--   * sales_enriched         one wide row per order line (the "single source of truth")
--   * dim_date               calendar with UAE weekend and retail events (Ramadan, Eid, DSF...)
--   * daily_store_sales      KPIs per store per day           -> dashboard trend + map
--   * monthly_category_sales KPIs per category per month      -> category mix, MoM growth
--   * product_performance    totals + rank within category     -> top products
--   * customer_rfm           Recency / Frequency / Monetary     -> segments for marketing
-- Revenue counts COMPLETED orders only. Returned and cancelled orders are reported separately.
-- =====================================================================================

-- ---------- Calendar dimension with UAE retail events
CREATE OR REFRESH MATERIALIZED VIEW sahra_gold.dim_date
COMMENT 'Calendar 2025-2026 with UAE weekend (Sat-Sun) and retail event labels.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH days AS (
  SELECT explode(sequence(DATE'2025-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS date
)
SELECT
  date,
  year(date)                                  AS year,
  quarter(date)                               AS quarter,
  month(date)                                 AS month,
  date_format(date, 'MMM')                    AS month_name,
  date_format(date, 'EEEE')                   AS day_name,
  dayofweek(date) IN (1, 7)                   AS is_weekend,       -- Sun=1, Sat=7 (UAE weekend since 2022)
  CASE
    WHEN date BETWEEN '2025-03-01' AND '2025-03-29' THEN 'Ramadan'
    WHEN date BETWEEN '2025-03-30' AND '2025-04-02' THEN 'Eid al-Fitr'
    WHEN date BETWEEN '2025-06-05' AND '2025-06-09' THEN 'Eid al-Adha'
    WHEN date BETWEEN '2025-08-15' AND '2025-09-05' THEN 'Back to School'
    WHEN date BETWEEN '2025-11-24' AND '2025-11-30' THEN 'White Friday'
    WHEN date BETWEEN '2025-12-01' AND '2025-12-03' THEN 'National Day'
    WHEN date BETWEEN '2025-01-01' AND '2025-01-12'
      OR date BETWEEN '2025-12-05' AND '2026-01-11' THEN 'Dubai Shopping Festival'
    WHEN date BETWEEN '2026-02-18' AND '2026-03-19' THEN 'Ramadan'
    WHEN date BETWEEN '2026-03-20' AND '2026-03-22' THEN 'Eid al-Fitr'
    ELSE 'Regular'
  END                                         AS retail_event
FROM days;

-- ---------- The wide fact table: every order line with its store, product and customer context
CREATE OR REFRESH MATERIALIZED VIEW sahra_gold.sales_enriched
COMMENT 'One row per order line joined to store, product and the customer tier AT THE TIME of the order.'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  li.order_id,
  li.order_ts,
  li.order_date,
  li.line_no,
  li.status,
  li.status = 'COMPLETED'                                    AS is_revenue,
  li.channel,
  li.payment_method,
  -- store
  li.store_id,
  s.store_name,
  s.city                                                      AS store_city,
  s.emirate                                                   AS store_emirate,
  s.store_format,
  s.latitude,
  s.longitude,
  -- customer (point-in-time join on the SCD2 history)
  li.customer_id,
  CASE
    WHEN li.customer_id = 'GUEST' THEN 'Guest'
    WHEN c.customer_id IS NULL    THEN 'Unknown'
    ELSE c.loyalty_tier
  END                                                         AS loyalty_tier_at_order,
  c.emirate                                                   AS customer_emirate,
  -- product
  li.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  p.is_private_label,
  -- measures (AED)
  li.qty,
  li.unit_price,
  li.discount_pct,
  li.gross_amount,
  li.discount_amount,
  li.net_amount,
  round(li.qty * p.unit_cost_aed, 2)                          AS cost_amount,
  round(li.net_amount - li.qty * p.unit_cost_aed, 2)          AS gross_margin
FROM sahra_silver.order_items li
LEFT JOIN sahra_silver.products p
  ON li.product_id = p.product_id
LEFT JOIN sahra_silver.stores s
  ON li.store_id = s.store_id
LEFT JOIN sahra_silver.customers_history c
  ON  li.customer_id = c.customer_id
  AND li.order_ts >= c.__START_AT
  AND (c.__END_AT IS NULL OR li.order_ts < c.__END_AT);

-- ---------- Daily KPIs per store
CREATE OR REFRESH MATERIALIZED VIEW sahra_gold.daily_store_sales
COMMENT 'Per store per day: orders, units, net revenue, margin, basket size and return rate (AED).'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  order_date,
  store_id,
  store_name,
  store_emirate,
  store_format,
  latitude,
  longitude,
  count(DISTINCT CASE WHEN is_revenue THEN order_id END)              AS orders,
  count(DISTINCT CASE WHEN status = 'RETURNED' THEN order_id END)     AS returned_orders,
  count(DISTINCT CASE WHEN status = 'CANCELLED' THEN order_id END)    AS cancelled_orders,
  sum(CASE WHEN is_revenue THEN qty ELSE 0 END)                       AS units,
  round(sum(CASE WHEN is_revenue THEN net_amount ELSE 0 END), 2)      AS net_revenue,
  round(sum(CASE WHEN is_revenue THEN discount_amount ELSE 0 END), 2) AS discounts,
  round(sum(CASE WHEN is_revenue THEN gross_margin ELSE 0 END), 2)    AS gross_margin,
  round(sum(CASE WHEN is_revenue THEN net_amount ELSE 0 END)
        / nullif(count(DISTINCT CASE WHEN is_revenue THEN order_id END), 0), 2) AS avg_basket
FROM sahra_gold.sales_enriched
GROUP BY ALL;

-- ---------- Monthly KPIs per category, with month-over-month growth
CREATE OR REFRESH MATERIALIZED VIEW sahra_gold.monthly_category_sales
COMMENT 'Per category per month: revenue, margin %, units, orders and month-over-month growth.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH m AS (
  SELECT
    date_trunc('MONTH', order_date)          AS month,
    category,
    round(sum(net_amount), 2)                AS net_revenue,
    round(sum(gross_margin), 2)              AS gross_margin,
    sum(qty)                                 AS units,
    count(DISTINCT order_id)                 AS orders
  FROM sahra_gold.sales_enriched
  WHERE is_revenue
  GROUP BY ALL
)
SELECT
  CAST(month AS DATE)                                                   AS month,
  category,
  net_revenue,
  gross_margin,
  round(gross_margin / nullif(net_revenue, 0), 4)                       AS margin_pct,
  units,
  orders,
  round(net_revenue / nullif(lag(net_revenue) OVER (PARTITION BY category ORDER BY month), 0) - 1, 4)
                                                                        AS mom_growth_pct
FROM m;

-- ---------- Product leaderboard
CREATE OR REFRESH MATERIALIZED VIEW sahra_gold.product_performance
COMMENT 'Per product: units, revenue, margin, and rank by revenue within its category.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH p AS (
  SELECT
    product_id, product_name, category, subcategory, brand, is_private_label,
    sum(qty)                                 AS units,
    round(sum(net_amount), 2)                AS net_revenue,
    round(sum(gross_margin), 2)              AS gross_margin,
    count(DISTINCT order_id)                 AS orders,
    round(avg(discount_pct), 1)              AS avg_discount_pct
  FROM sahra_gold.sales_enriched
  WHERE is_revenue
  GROUP BY ALL
)
SELECT
  *,
  round(gross_margin / nullif(net_revenue, 0), 4)                        AS margin_pct,
  rank() OVER (PARTITION BY category ORDER BY net_revenue DESC)          AS rank_in_category
FROM p;

-- ---------- Customer segments (RFM)
CREATE OR REFRESH MATERIALIZED VIEW sahra_gold.customer_rfm
COMMENT 'One row per known customer: recency, frequency, monetary scores (1-5) and a marketing segment.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH base AS (
  SELECT
    customer_id,
    max(order_date)               AS last_order_date,
    count(DISTINCT order_id)      AS frequency,
    round(sum(net_amount), 2)     AS monetary
  FROM sahra_gold.sales_enriched
  WHERE is_revenue
    AND loyalty_tier_at_order NOT IN ('Guest', 'Unknown')
  GROUP BY customer_id
),
ref AS (SELECT max(order_date) AS as_of FROM sahra_gold.sales_enriched),
scored AS (
  SELECT
    b.*,
    datediff(r.as_of, b.last_order_date)                                  AS recency_days,
    6 - ntile(5) OVER (ORDER BY datediff(r.as_of, b.last_order_date))     AS r_score,  -- recent = 5
    ntile(5) OVER (ORDER BY b.frequency)                                  AS f_score,
    ntile(5) OVER (ORDER BY b.monetary)                                   AS m_score
  FROM base b CROSS JOIN ref r
)
SELECT
  s.customer_id,
  c.first_name,
  c.last_name,
  c.loyalty_tier                AS current_tier,
  c.emirate,
  c.signup_date,
  s.last_order_date,
  s.recency_days,
  s.frequency,
  s.monetary,
  s.r_score,
  s.f_score,
  s.m_score,
  CASE
    WHEN s.r_score >= 4 AND s.f_score >= 4 THEN 'Champions'
    WHEN s.f_score >= 4                    THEN 'Loyal'
    WHEN s.r_score >= 4 AND s.f_score <= 2 THEN 'New & Promising'
    WHEN s.r_score <= 2 AND s.f_score >= 3 THEN 'At Risk'
    WHEN s.r_score <= 2                    THEN 'Hibernating'
    ELSE 'Needs Attention'
  END                           AS segment
FROM scored s
LEFT JOIN sahra_silver.customers_current c
  ON s.customer_id = c.customer_id;
