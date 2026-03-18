-- sql/20_raw/21_raw_orders_insert.sql
-- Purpose:
--   orders_external（GCS参照）から raw_olist.orders（BigQuery保存）へロードする。
-- Notes:
--   run_date はGCSパスの dt=YYYY-MM-DD と合わせる。
--   同じrun_dateを再実行する場合は、DELETE→INSERT か MERGE へ拡張する（MVPではINSERT）。
--   同日のデータを作りなおすことで冪等性を確保。

DECLARE run_date DATE DEFAULT DATE '2026-03-05';


DELETE FROM `ec-data-platform.raw_olist.orders`
WHERE ingest_date = run_date;

INSERT INTO `ec-data-platform.raw_olist.orders`
SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date,

  SAFE_CAST(REGEXP_EXTRACT(_FILE_NAME, r'dt=(\d{4}-\d{2}-\d{2})') AS DATE) AS ingest_date,
  CURRENT_TIMESTAMP() AS loaded_at,
  _FILE_NAME AS source_file
FROM `ec-data-platform.raw_olist.orders_external`;