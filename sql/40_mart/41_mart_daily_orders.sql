-- mart/41_mart_daily_orders.sql
-- Purpose:
--   clean_olist.orders をもとに、日次KPIテーブル mart_olist.daily_orders を更新する。
-- Grain:
--   1 purchase_date = 1 row
-- Notes:
--   同日再実行に耐えられるよう MERGE を使用する。

MERGE `ec-data-platform.mart_olist.daily_orders` AS T
USING (
  SELECT
    purchase_date,
    COUNT(*) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customer_count,
    COUNTIF(order_status = 'delivered') AS delivered_count,
    COUNTIF(order_status = 'canceled') AS canceled_count,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
  FROM `ec-data-platform.clean_olist.orders`
  WHERE purchase_date IS NOT NULL
  GROUP BY purchase_date
) AS S
ON T.purchase_date = S.purchase_date

WHEN MATCHED THEN
  UPDATE SET
    T.order_count = S.order_count,
    T.unique_customer_count = S.unique_customer_count,
    T.delivered_count = S.delivered_count,
    T.canceled_count = S.canceled_count,
    T.updated_at = S.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    purchase_date,
    order_count,
    unique_customer_count,
    delivered_count,
    canceled_count,
    created_at,
    updated_at
  )
  VALUES (
    S.purchase_date,
    S.order_count,
    S.unique_customer_count,
    S.delivered_count,
    S.canceled_count,
    S.created_at,
    S.updated_at
  );