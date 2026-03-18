-- sql/20_raw/20_raw_orders_ddl.sql
-- Purpose:
--   raw層のorders保存テーブルを作成する（partition: ingest_date）。
-- Notes:
--   raw層は「原本保存」が目的。元データ列は基本そのまま保持し、運用のためのメタ列を追加する。
--   - ingest_date: 取り込み日（GCSパスの dt=YYYY-MM-DD を想定）
--   - loaded_at  : ロード時刻
--   - source_file: 参照元ファイル（外部テーブルの _FILE_NAME ）

CREATE TABLE IF NOT EXISTS`ec-data-platform.raw_olist.orders`
(
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP,

  ingest_date DATE,
  loaded_at TIMESTAMP,
  source_file STRING
)
PARTITION BY ingest_date;