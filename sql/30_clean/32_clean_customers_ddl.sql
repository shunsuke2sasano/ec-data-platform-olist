-- sql/30_clean/32_clean_customers_ddl.sql
-- Purpose:
--   clean_olist.customers テーブルを作成する。
-- Notes:
--   customer_id 単位の最新レコードを保持する clean layer。
--   customers は日付軸の明細ではないため partition は行わず、検索しやすさのため cluster を付与する。

CREATE TABLE IF NOT EXISTS `ec-data-platform.clean_olist.customers` (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING,

  is_customer_id_present BOOL,
  dq_error_reason STRING,

  raw_ingest_date DATE,
  raw_loaded_at TIMESTAMP,
  source_file STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
CLUSTER BY customer_state, customer_city;