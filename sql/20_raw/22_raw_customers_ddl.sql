-- sql/20_raw/22_raw_customers_ddl.sql
-- Purpose:
--   raw_olist.customers テーブルを作成する。
-- Notes:
--   GCS由来の customers データを保持する raw layer。
--   ingest_date 単位で再実行可能にするため PARTITION BY ingest_date を採用。
--   source_file には GCS 上の元ファイル名を保持する。

CREATE TABLE IF NOT EXISTS `ec-data-platform.raw_olist.customers` (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING,

  ingest_date DATE,
  loaded_at TIMESTAMP,
  source_file STRING
)
PARTITION BY ingest_date;