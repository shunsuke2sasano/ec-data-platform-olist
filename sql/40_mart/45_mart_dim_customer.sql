-- sql/40_mart/45_mart_dim_customer.sql
-- Purpose:
--   clean_olist.customers から dim_customer を作成・更新する。
-- Grain:
--   1 customer_id = 1行

MERGE `ec-data-platform.mart_olist.dim_customer` AS T
USING (
  SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
  FROM `ec-data-platform.clean_olist.customers`
  WHERE customer_id IS NOT NULL
) AS S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN
  UPDATE SET
    T.customer_unique_id = S.customer_unique_id,
    T.customer_city = S.customer_city,
    T.customer_state = S.customer_state,
    T.updated_at = S.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    created_at,
    updated_at
  )
  VALUES (
    S.customer_id,
    S.customer_unique_id,
    S.customer_city,
    S.customer_state,
    S.created_at,
    S.updated_at
  );