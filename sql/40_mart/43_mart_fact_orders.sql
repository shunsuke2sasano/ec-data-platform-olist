-- sql/40_mart/43_mart_fact_orders.sql
-- Purpose:
--   clean_olist.orders から fact_orders を作成・更新する。
-- Grain:
--   1注文 = 1行

MERGE `ec-data-platform.mart_olist.fact_orders` AS T
USING (
  SELECT
    order_id,
    customer_id,
    purchase_date,
    order_status,
    order_status = 'delivered' AS is_delivered,
    order_status = 'canceled' AS is_canceled,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
  FROM `ec-data-platform.clean_olist.orders`
  WHERE order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND purchase_date IS NOT NULL
    AND purchase_date = run_date
) AS S
ON T.order_id = S.order_id

WHEN MATCHED THEN
  UPDATE SET
    T.customer_id = S.customer_id,
    T.purchase_date = S.purchase_date,
    T.order_status = S.order_status,
    T.is_delivered = S.is_delivered,
    T.is_canceled = S.is_canceled,
    T.updated_at = S.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    order_id,
    customer_id,
    purchase_date,
    order_status,
    is_delivered,
    is_canceled,
    created_at,
    updated_at
  )
  VALUES (
    S.order_id,
    S.customer_id,
    S.purchase_date,
    S.order_status,
    S.is_delivered,
    S.is_canceled,
    S.created_at,
    S.updated_at
  );