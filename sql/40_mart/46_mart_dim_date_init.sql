-- sql/40_mart/46_mart_dim_date_init.sql
-- Purpose:
--   日付属性を分析用 dimension テーブルとして格納する。
-- Grain:
--   1日 = 1行
-- Notes:
--   年/月/曜日などの分析軸を提供する。

CREATE TABLE IF NOT EXISTS `ec-data-platform.mart_olist.dim_date` (
  purchase_date DATE,
  year_num INT64,
  month_num INT64,
  day_num INT64,
  year_month STRING,
  day_of_week_num INT64,
  day_of_week_name STRING,
  is_weekend BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY purchase_date;