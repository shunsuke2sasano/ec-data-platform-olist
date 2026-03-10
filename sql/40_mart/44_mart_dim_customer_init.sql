-- sql/40_mart/44_mart_dim_customer_init.sql
-- Purpose:
--   顧客属性を分析用 dimension テーブルとして格納する。
-- Grain:
--   1 customer_id = 1行
-- Notes:
--   地域別・顧客属性別分析の軸として利用する。

CREATE TABLE IF NOT EXISTS `ec-data-platform.mart_olist.dim_customer` (
  customer_id STRING,
  customer_unique_id STRING,
  customer_city STRING,
  customer_state STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
CLUSTER BY customer_state, customer_city;