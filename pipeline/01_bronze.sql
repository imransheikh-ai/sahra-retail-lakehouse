-- =====================================================================================
-- SAHRA RETAIL LAKEHOUSE  |  Lakeflow Declarative Pipeline  |  01 BRONZE
-- Author: Imran Sheikh | https://www.linkedin.com/in/imranazhar/
-- -------------------------------------------------------------------------------------
-- Bronze = the data exactly as it arrived, plus lineage columns.
--   * STREAMING TABLE + read_files()  ->  Auto Loader: each file is read ONCE.
--     Drop a new file into the landing folder, refresh the pipeline, and only that
--     file is processed.
--   * No cleaning here. If something is wrong upstream we can always replay from Bronze.
--
-- Pipeline settings:  default catalog = workspace,  default schema = sahra_bronze
-- Landing folder   :  /Volumes/workspace/sahra_bronze/landing/<entity>/
-- =====================================================================================

-- ---------- Orders: JSON Lines from POS + e-commerce, one row per order, items nested
CREATE OR REFRESH STREAMING TABLE sahra_bronze.orders_raw
COMMENT 'Raw POS and online orders as received (JSON Lines). Items are still a nested array.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path              AS source_file,      -- which file the row came from
  _metadata.file_modification_time AS file_modified_at,
  current_timestamp()              AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/sahra_bronze/landing/orders/',
  format             => 'json',
  rescuedDataColumn  => '_rescued_data',                -- anything that does not fit the schema lands here
  schemaHints        => 'order_id STRING, order_ts STRING, store_id STRING, customer_id STRING, status STRING,
                         items ARRAY<STRUCT<line_no INT, product_id STRING, qty INT, unit_price DOUBLE, discount_pct INT>>'
);

-- ---------- Customers: CRM exports (full snapshot in Jan, then changes + new sign-ups)
CREATE OR REFRESH STREAMING TABLE sahra_bronze.customers_raw
COMMENT 'Raw CRM customer exports (CSV). The same customer can appear in several exports.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/sahra_bronze/landing/customers/',
  format            => 'csv',
  header            => true,
  rescuedDataColumn => '_rescued_data',
  schemaHints       => 'customer_id STRING, phone STRING, signup_date DATE, updated_at TIMESTAMP'
);

-- ---------- Products: ERP product master (CSV)
CREATE OR REFRESH STREAMING TABLE sahra_bronze.products_raw
COMMENT 'Raw product master from the ERP (CSV).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/sahra_bronze/landing/products/',
  format            => 'csv',
  header            => true,
  rescuedDataColumn => '_rescued_data'
);

-- ---------- Stores: store master (CSV)
CREATE OR REFRESH STREAMING TABLE sahra_bronze.stores_raw
COMMENT 'Raw store master (CSV).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/sahra_bronze/landing/stores/',
  format            => 'csv',
  header            => true,
  rescuedDataColumn => '_rescued_data'
);
