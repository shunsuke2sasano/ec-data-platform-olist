-- sql/40_mart/42_mart_fact_orders_init.sql
-- Purpose:
--   注文イベントを分析用 fact テーブルとして格納する。
-- Grain:
--   1注文 = 1行
-- Notes:
--   mart層の中心テーブル。
--   顧客や日付のdimensionとJOINして利用する。

CREATE TABLE IF NOT EXISTS `ec-data-platform.mart_olist.fact_orders` (
  order_id STRING,
  customer_id STRING,
  purchase_date DATE,
  order_status STRING,
  is_delivered BOOL,
  is_canceled BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY purchase_date
CLUSTER BY customer_id, order_status;