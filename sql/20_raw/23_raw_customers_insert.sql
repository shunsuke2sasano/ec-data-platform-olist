-- sql/20_raw/23_raw_customers_insert.sql
-- Purpose:
--   customers_external から raw_olist.customers へロードする。
-- Notes:
--   ingest_date は _FILE_NAME の dt=YYYY-MM-DD から抽出する。
--   DELETE + INSERT により ingest_date 単位で冪等に再実行できる。

DECLARE run_date DATE DEFAULT '2026-03-05';

DELETE
FROM `ec-data-platform.raw_olist.customers`
WHERE ingest_date = run_date;

INSERT INTO `ec-data-platform.raw_olist.customers` (
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  ingest_date,
  loaded_at,
  source_file
)
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  SAFE_CAST(REGEXP_EXTRACT(_FILE_NAME, r'dt=(\d{4}-\d{2}-\d{2})') AS DATE) AS ingest_date,
  CURRENT_TIMESTAMP() AS loaded_at,
  _FILE_NAME AS source_file
FROM `ec-data-platform.raw_olist.customers_external`
WHERE SAFE_CAST(REGEXP_EXTRACT(_FILE_NAME, r'dt=(\d{4}-\d{2}-\d{2})') AS DATE) = run_date;