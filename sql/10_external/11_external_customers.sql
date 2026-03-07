-- sql/10_external/11_external_customers.sql
-- Purpose:
--   GCS上の customers CSV を参照する external table を作成する。
-- Notes:
--   スキーマは明示指定。
--   dt=YYYY-MM-DD は GCS パス上にあり、テーブル列としては持たない。
--   ingest_date は raw insert 時に _FILE_NAME から抽出する。

CREATE OR REPLACE EXTERNAL TABLE `ec-data-platform.raw_olist.customers_external`
(
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://ec-data-platform-olist/raw/customers/dt=2026-03-05/*.csv'],
  skip_leading_rows = 1
);