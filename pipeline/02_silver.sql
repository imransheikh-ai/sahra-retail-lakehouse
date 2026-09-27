-- =====================================================================================
-- SAHRA RETAIL LAKEHOUSE  |  Lakeflow Declarative Pipeline  |  02 SILVER
-- Author: Imran Sheikh | https://www.linkedin.com/in/imranazhar/
-- -------------------------------------------------------------------------------------
-- Silver = clean, typed, de-duplicated, trustworthy records.
--   * Expectations (CONSTRAINT ... EXPECT) are data-quality rules. Results show up in the
--     pipeline UI and event log:
--       - no action            -> keep the row, count the failure (warn)
--       - ON VIOLATION DROP ROW -> remove the row, count the failure
--       - ON VIOLATION FAIL UPDATE -> stop the pipeline (for rules that must never break)
--   * Rejected order lines go to a quarantine table instead of disappearing silently.
--   * Customers keep full history with AUTO CDC ... STORED AS SCD TYPE 2.
-- =====================================================================================

-- ---------- Stores: typed store dimension
CREATE OR REFRESH MATERIALIZED VIEW sahra_silver.stores (
  CONSTRAINT store_id_format EXPECT (store_id RLIKE '^ST[0-9]{2}$') ON VIOLATION FAIL UPDATE
)
COMMENT 'Store dimension: one row per store, typed columns.'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT * EXCEPT (rn) FROM (
SELECT
  upper(trim(store_id))            AS store_id,
  store_name,
  city,
  emirate,
  store_format,
  CAST(latitude  AS DOUBLE)        AS latitude,
  CAST(longitude AS DOUBLE)        AS longitude,
  CAST(opened_date AS DATE)        AS opened_date,
  CAST(size_sqm AS INT)            AS size_sqm,
  row_number() OVER (PARTITION BY upper(trim(store_id)) ORDER BY ingested_at DESC) AS rn  -- latest copy wins
FROM sahra_bronze.stores_raw
) WHERE rn = 1;

-- ---------- Products: typed product dimension with margin
CREATE OR REFRESH MATERIALIZED VIEW sahra_silver.products (
  CONSTRAINT price_positive EXPECT (unit_price_aed > 0) ON VIOLATION DROP ROW,
  CONSTRAINT cost_below_price EXPECT (unit_cost_aed <= unit_price_aed)
)
COMMENT 'Product dimension: one row per product with list price, cost and margin (AED).'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT * EXCEPT (rn) FROM (
SELECT
  product_id,
  product_name,
  category,
  subcategory,
  brand,
  CAST(unit_price_aed AS DOUBLE)                                   AS unit_price_aed,
  CAST(unit_cost_aed  AS DOUBLE)                                   AS unit_cost_aed,
  round(1 - CAST(unit_cost_aed AS DOUBLE) / CAST(unit_price_aed AS DOUBLE), 4) AS list_margin_pct,
  CAST(is_private_label AS BOOLEAN)                                AS is_private_label,
  CAST(launched_on AS DATE)                                        AS launched_on,
  row_number() OVER (PARTITION BY product_id ORDER BY ingested_at DESC) AS rn
FROM sahra_bronze.products_raw
) WHERE rn = 1;

-- ---------- Orders (header): parse timestamps, fix keys, normalise status, remove duplicates
CREATE OR REFRESH MATERIALIZED VIEW sahra_silver.orders (
  CONSTRAINT order_id_present EXPECT (order_id IS NOT NULL)                              ON VIOLATION DROP ROW,
  CONSTRAINT order_ts_parsed  EXPECT (order_ts IS NOT NULL)                              ON VIOLATION DROP ROW,
  CONSTRAINT known_status     EXPECT (status IN ('COMPLETED', 'RETURNED', 'CANCELLED')),
  CONSTRAINT valid_store_id   EXPECT (store_id RLIKE '^ST[0-9]{2}$')
)
COMMENT 'Clean order headers: one row per order_id, timestamps parsed, keys and status normalised.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH cleaned AS (
  SELECT
    order_id,
    -- the POS sends two timestamp formats; try both
    coalesce(try_to_timestamp(order_ts, "yyyy-MM-dd'T'HH:mm:ss"),
             try_to_timestamp(order_ts, 'dd/MM/yyyy HH:mm'))  AS order_ts,
    upper(trim(store_id))                                    AS store_id,   -- ' st03 ' -> 'ST03'
    coalesce(customer_id, 'GUEST')                           AS customer_id,
    channel,
    payment_method,
    upper(trim(status))                                      AS status,     -- 'Completed ' -> 'COMPLETED'
    currency,
    items,
    source_file,
    ingested_at
  FROM sahra_bronze.orders_raw
)
SELECT * EXCEPT (rn) FROM (
  SELECT
    *,
    to_date(order_ts) AS order_date,
    size(items)       AS line_count,
    -- the POS sometimes re-sends an order: keep the first copy we received
    row_number() OVER (PARTITION BY order_id ORDER BY ingested_at, source_file) AS rn
  FROM cleaned
) WHERE rn = 1;

-- ---------- Order lines: explode the nested items array, compute amounts
CREATE OR REFRESH MATERIALIZED VIEW sahra_silver.order_items (
  CONSTRAINT positive_qty  EXPECT (qty > 0)                ON VIOLATION DROP ROW,
  CONSTRAINT price_present EXPECT (unit_price IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'One row per order line with gross, discount and net amounts in AED.'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  o.order_id,
  o.order_ts,
  o.order_date,
  o.store_id,
  o.customer_id,
  o.channel,
  o.payment_method,
  o.status,
  i.line_no,
  i.product_id,
  i.qty,
  i.unit_price,
  i.discount_pct,
  round(i.qty * i.unit_price, 2)                               AS gross_amount,
  round(i.qty * i.unit_price * i.discount_pct / 100.0, 2)      AS discount_amount,
  round(i.qty * i.unit_price * (1 - i.discount_pct / 100.0), 2) AS net_amount
FROM sahra_silver.orders o
LATERAL VIEW explode(o.items) t AS i;

-- ---------- Quarantine: the lines the rules above rejected, with the reason
CREATE OR REFRESH MATERIALIZED VIEW sahra_silver.order_items_quarantine
COMMENT 'Order lines rejected by data-quality rules. Review, fix upstream, re-send.'
TBLPROPERTIES ('quality' = 'quarantine')
AS SELECT
  o.order_id,
  o.order_date,
  o.store_id,
  o.source_file,
  i.line_no,
  i.product_id,
  i.qty,
  i.unit_price,
  CASE
    WHEN i.qty IS NULL OR i.qty <= 0 THEN 'non_positive_qty'
    WHEN i.unit_price IS NULL        THEN 'missing_price'
  END AS reject_reason
FROM sahra_silver.orders o
LATERAL VIEW explode(o.items) t AS i
WHERE i.qty IS NULL OR i.qty <= 0 OR i.unit_price IS NULL;

-- ---------- Customers: full change history (SCD Type 2)
-- Each change to loyalty tier or emirate closes the old row (__END_AT) and opens a new one
-- (__START_AT). Sales can then be joined to the tier the customer had ON THE DAY of the order.
CREATE OR REFRESH STREAMING TABLE sahra_silver.customers_history
COMMENT 'Customer dimension with full history (SCD Type 2). Current rows have __END_AT = NULL.'
TBLPROPERTIES ('quality' = 'silver');

CREATE FLOW customers_scd2
AS AUTO CDC INTO sahra_silver.customers_history
FROM STREAM(sahra_bronze.customers_raw)
KEYS (customer_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (_rescued_data, source_file, ingested_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON loyalty_tier, emirate, city;
-- Older workspaces: replace the flow with
--   APPLY CHANGES INTO sahra_silver.customers_history FROM STREAM(sahra_bronze.customers_raw)
--   KEYS (customer_id) SEQUENCE BY updated_at COLUMNS * EXCEPT (_rescued_data, source_file, ingested_at)
--   STORED AS SCD TYPE 2;

-- ---------- Customers: current view (one row per customer)
CREATE OR REFRESH MATERIALIZED VIEW sahra_silver.customers_current
COMMENT 'Current state of every customer (latest SCD2 row).'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  emirate,
  loyalty_tier,
  CAST(marketing_opt_in AS BOOLEAN) AS marketing_opt_in,
  signup_date,
  __START_AT                        AS current_since
FROM sahra_silver.customers_history
WHERE __END_AT IS NULL;
