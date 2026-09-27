-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Explore and verify each layer
-- MAGIC
-- MAGIC *Sahra Retail Lakehouse · by Imran Sheikh · [LinkedIn](https://www.linkedin.com/in/imranazhar/)*
-- MAGIC Run this after the first pipeline update. Each cell checks one idea. Compare your numbers
-- MAGIC with the "Expected" comments: they come from running the same SQL on the same seeded data.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

-- MAGIC %md ## 1 · Row counts through the medallion

-- COMMAND ----------

SELECT 'bronze.orders_raw'      AS table_name, count(*) AS row_count FROM sahra_bronze.orders_raw      -- Expected 77,902
UNION ALL SELECT 'silver.orders',              count(*) FROM sahra_silver.orders                        -- Expected 77,303 (599 duplicates removed)
UNION ALL SELECT 'silver.order_items',         count(*) FROM sahra_silver.order_items                   -- Expected 198,384
UNION ALL SELECT 'silver.order_items_quarantine', count(*) FROM sahra_silver.order_items_quarantine     -- Expected 408
UNION ALL SELECT 'silver.customers_history',   count(*) FROM sahra_silver.customers_history             -- Expected 3,395
UNION ALL SELECT 'silver.customers_current',   count(*) FROM sahra_silver.customers_current             -- Expected 3,100
UNION ALL SELECT 'gold.sales_enriched',        count(*) FROM sahra_gold.sales_enriched                  -- Expected 198,384
UNION ALL SELECT 'gold.daily_store_sales',     count(*) FROM sahra_gold.daily_store_sales               -- Expected 4,379
UNION ALL SELECT 'gold.customer_rfm',          count(*) FROM sahra_gold.customer_rfm;                   -- Expected 3,025

-- COMMAND ----------

-- MAGIC %md ## 2 · Bronze keeps the mess (on purpose)

-- COMMAND ----------

-- Messy values that Silver must fix
SELECT store_id, count(*) AS n FROM sahra_bronze.orders_raw WHERE store_id NOT RLIKE '^ST[0-9]{2}$' GROUP BY 1;

-- COMMAND ----------

SELECT status, count(*) AS n FROM sahra_bronze.orders_raw GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

-- Orders the POS sent twice
SELECT order_id, count(*) AS copies FROM sahra_bronze.orders_raw GROUP BY 1 HAVING count(*) > 1 ORDER BY 1 LIMIT 10;

-- COMMAND ----------

-- Lineage: every row knows which file it came from
SELECT source_file, count(*) AS orders, min(ingested_at) AS first_loaded
FROM sahra_bronze.orders_raw GROUP BY 1 ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md ## 3 · Silver: quality rules and quarantine

-- COMMAND ----------

SELECT reject_reason, count(*) AS lines FROM sahra_silver.order_items_quarantine GROUP BY 1;
-- Expected: non_positive_qty 284, missing_price 124

-- COMMAND ----------

-- The second timestamp format was parsed, not dropped
SELECT r.order_ts AS raw_value, o.order_ts AS parsed
FROM sahra_bronze.orders_raw r JOIN sahra_silver.orders o USING (order_id)
WHERE r.order_ts LIKE '%/%' LIMIT 5;

-- COMMAND ----------

-- MAGIC %md ## 4 · SCD Type 2: customer history

-- COMMAND ----------

-- Customers with more than one version
SELECT customer_id, loyalty_tier, emirate, __START_AT, __END_AT
FROM sahra_silver.customers_history
WHERE customer_id IN (
  SELECT customer_id FROM sahra_silver.customers_history GROUP BY 1 HAVING count(*) > 1 LIMIT 3)
ORDER BY customer_id, __START_AT;

-- COMMAND ----------

-- Point-in-time join in action: same customer, different tier on different orders
SELECT customer_id, order_date, loyalty_tier_at_order, count(DISTINCT order_id) AS orders
FROM sahra_gold.sales_enriched
WHERE customer_id = (
  SELECT customer_id FROM sahra_gold.sales_enriched
  WHERE loyalty_tier_at_order NOT IN ('Guest','Unknown')
  GROUP BY 1 HAVING count(DISTINCT loyalty_tier_at_order) > 1 LIMIT 1)
GROUP BY ALL ORDER BY order_date;

-- COMMAND ----------

-- MAGIC %md ## 5 · Gold: the numbers the dashboard will show

-- COMMAND ----------

SELECT
  round(sum(net_amount))                          AS net_revenue_aed,   -- Expected ~35.9M
  round(sum(gross_margin))                        AS gross_margin_aed,  -- Expected ~9.9M
  count(DISTINCT order_id)                        AS orders,            -- Expected 70,901
  round(sum(net_amount) / count(DISTINCT order_id), 2) AS avg_basket_aed -- Expected ~506
FROM sahra_gold.sales_enriched WHERE is_revenue;

-- COMMAND ----------

SELECT d.retail_event,
       round(sum(s.net_revenue) / count(DISTINCT s.order_date)) AS revenue_per_day
FROM sahra_gold.daily_store_sales s JOIN sahra_gold.dim_date d ON s.order_date = d.date
GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · Delta Lake lab: history and time travel
-- MAGIC Pipeline tables are managed by the pipeline, so we practise time travel on a normal Delta table.

-- COMMAND ----------

CREATE OR REPLACE TABLE sahra_gold.store_targets AS
SELECT store_id, store_name, round(sum(net_revenue) * 1.10, -3) AS target_2026_aed
FROM sahra_gold.daily_store_sales GROUP BY ALL;

-- COMMAND ----------

UPDATE sahra_gold.store_targets SET target_2026_aed = target_2026_aed * 1.5 WHERE store_id = 'ST12';  -- push online harder

-- COMMAND ----------

DESCRIBE HISTORY sahra_gold.store_targets;

-- COMMAND ----------

SELECT v0.store_id, v0.target_2026_aed AS original, now.target_2026_aed AS current
FROM sahra_gold.store_targets VERSION AS OF 0 v0
JOIN sahra_gold.store_targets now USING (store_id)
WHERE v0.target_2026_aed <> now.target_2026_aed;

-- COMMAND ----------

-- Undo the change
RESTORE TABLE sahra_gold.store_targets TO VERSION AS OF 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7 · Governance: documentation, tags, access
-- MAGIC Tables created by the pipeline are owned by the pipeline, so their comments live in the pipeline SQL
-- MAGIC (the `COMMENT '...'` lines). Here we practise on `store_targets`, a normal table you own.

-- COMMAND ----------

COMMENT ON TABLE sahra_gold.store_targets IS '2026 revenue targets per store (AED), set by Finance.';
ALTER TABLE sahra_gold.store_targets ALTER COLUMN target_2026_aed COMMENT 'Revenue target for 2026 in AED, excluding VAT';

-- COMMAND ----------

-- Tags make data easy to find and classify (look for them in Catalog Explorer)
ALTER TABLE sahra_gold.store_targets SET TAGS ('domain' = 'finance', 'owner_team' = 'fp&a');

-- COMMAND ----------

DESCRIBE TABLE EXTENDED sahra_gold.store_targets;

-- COMMAND ----------

-- Who can read the Gold layer today?
SHOW GRANTS ON SCHEMA workspace.sahra_gold;

-- COMMAND ----------

-- Access control examples. In a team workspace you would grant to groups:
-- GRANT USE SCHEMA, SELECT ON SCHEMA workspace.sahra_gold TO `analysts`;
-- GRANT SELECT ON TABLE workspace.sahra_silver.customers_current TO `marketing`;
SHOW TABLES IN sahra_gold;
