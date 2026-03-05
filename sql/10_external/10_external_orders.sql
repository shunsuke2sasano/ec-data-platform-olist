-- sql/10_external/10_external_orders.sql
-- Purpose:
--   GCS上のOlist orders CSVを BigQuery から参照するための external table を作成する。
-- Notes:
--   external table は BigQuery にデータを保存せず、クエリ実行時にGCSを読み込む。
--   skip_leading_rows=1 はCSVヘッダー行をデータとして読み込まないため。

CREATE OR REPLACE EXTERNAL TABLE `ec-data-platform.raw_olist.orders_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ec-data-platform-olist/raw/orders/dt=2026-03-05/*.csv'],
  skip_leading_rows = 1
);