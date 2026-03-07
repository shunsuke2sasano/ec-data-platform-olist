CREATE TABLE IF NOT EXISTS `ec-data-platform.mart_olist.daily_orders` (
  purchase_date DATE,
  order_count INT64,
  unique_customer_count INT64,
  delivered_count INT64,
  canceled_count INT64,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY purchase_date;