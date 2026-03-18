-- sql/10_external/10_external_orders.sql
-- Purpose:
--   GCS上のOlist orders CSVを BigQuery から参照するための external table を作成する。
-- Notes:
--   external table は BigQuery にデータを保存せず、クエリ実行時にGCSを読み込む。
--   skip_leading_rows=1 はCSVヘッダー行をデータとして読み込まないため。

CREATE EXTERNAL TABLE IF NOT EXISTS`ec-data-platform.raw_olist.orders_external`
(
 order_id STRING
 customer_id STRING
 order_status STRING
 order_purchase_timestamp TIMESTAMP
 order_approved_at TIMESTAMP
 order_delivered_carrier_date TIMESTAMP
 order_delivered_customer_date TIMESTAMP
 order_estimated_deliver_date TIMESTAMP
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://ec-data-platform-olist/raw/orders/dt=2026-03-05/*.csv'],
  skip_leading_rows = 1
);