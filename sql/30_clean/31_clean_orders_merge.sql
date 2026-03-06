-- sql/30_clean/31_clean_orders_merge.sql
-- ============================================
-- Purpose
--   raw_olist.orders から clean_olist.orders を生成
--
-- Logic
--   1. 文字列正規化
--   2. 派生列生成
--   3. データ品質フラグ
--   4. order_id単位重複排除
--   5. MERGE UPSERT
--
-- Partition
--   purchase_date
--
-- Cluster
--   customer_id, order_status
-- ============================================

DECLARE run_date DATE DEFAULT DATE '2026-03-05';

CREATE TABLE IF NOT EXISTS `ec-data-platform.clean_olist.orders` (
  order_id STRING,
  customer_id STRING,
  order_status STRING,

  purchase_at TIMESTAMP,
  approved_at TIMESTAMP,
  delivered_carrier_at TIMESTAMP,
  delivered_customer_at TIMESTAMP,
  estimated_delivery_date DATE,

  purchase_date DATE,

  ingest_date DATE,
  raw_loaded_at TIMESTAMP,
  source_file STRING,

  clean_loaded_at TIMESTAMP,
  is_purchase_at_parsed BOOL,
  dq_error_reason STRING
)
PARTITION BY purchase_date
CLUSTER BY customer_id, order_status;

MERGE `ec-data-platform.clean_olist.orders` T
USING (

WITH src AS (
  SELECT *
  FROM `ec-data-platform.raw_olist.orders`
  WHERE ingest_date = run_date
),

typed AS (
  SELECT
    NULLIF(TRIM(order_id), "") AS order_id,
    NULLIF(TRIM(customer_id), "") AS customer_id,
    NULLIF(LOWER(TRIM(order_status)), "") AS order_status,

    order_purchase_timestamp AS purchase_at,
    order_approved_at AS approved_at,
    order_delivered_carrier_date AS delivered_carrier_at,
    order_delivered_customer_date AS delivered_customer_at,

    DATE(order_estimated_delivery_date) AS estimated_delivery_date,

    ingest_date,
    loaded_at AS raw_loaded_at,
    source_file
  FROM src
),

enriched AS (
  SELECT
    *,
    DATE(purchase_at) AS purchase_date,
    purchase_at IS NOT NULL AS is_purchase_at_parsed,
    CASE
      WHEN order_id IS NULL THEN 'MISSING_ORDER_ID'
      WHEN purchase_at IS NULL THEN 'INVALID_PURCHASE_TS'
      ELSE NULL
    END AS dq_error_reason
  FROM typed
),

dedup AS (
  SELECT
    order_id,
    customer_id,
    order_status,
    purchase_at,
    approved_at,
    delivered_carrier_at,
    delivered_customer_at,
    estimated_delivery_date,
    purchase_date,
    ingest_date,
    raw_loaded_at,
    source_file,
    CURRENT_TIMESTAMP() AS clean_loaded_at,
    is_purchase_at_parsed,
    dq_error_reason
  FROM enriched
  WHERE order_id IS NOT NULL
    AND purchase_date IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY raw_loaded_at DESC, ingest_date DESC, source_file DESC
  ) = 1
)

SELECT * FROM dedup

) S
ON T.order_id = S.order_id

WHEN MATCHED AND S.raw_loaded_at >= T.raw_loaded_at THEN
UPDATE SET
  customer_id = S.customer_id,
  order_status = S.order_status,
  purchase_at = S.purchase_at,
  approved_at = S.approved_at,
  delivered_carrier_at = S.delivered_carrier_at,
  delivered_customer_at = S.delivered_customer_at,
  estimated_delivery_date = S.estimated_delivery_date,
  purchase_date = S.purchase_date,
  ingest_date = S.ingest_date,
  raw_loaded_at = S.raw_loaded_at,
  source_file = S.source_file,
  clean_loaded_at = S.clean_loaded_at,
  is_purchase_at_parsed = S.is_purchase_at_parsed,
  dq_error_reason = S.dq_error_reason

WHEN NOT MATCHED THEN
INSERT (
  order_id,
  customer_id,
  order_status,
  purchase_at,
  approved_at,
  delivered_carrier_at,
  delivered_customer_at,
  estimated_delivery_date,
  purchase_date,
  ingest_date,
  raw_loaded_at,
  source_file,
  clean_loaded_at,
  is_purchase_at_parsed,
  dq_error_reason
)
VALUES (
  S.order_id,
  S.customer_id,
  S.order_status,
  S.purchase_at,
  S.approved_at,
  S.delivered_carrier_at,
  S.delivered_customer_at,
  S.estimated_delivery_date,
  S.purchase_date,
  S.ingest_date,
  S.raw_loaded_at,
  S.source_file,
  S.clean_loaded_at,
  S.is_purchase_at_parsed,
  S.dq_error_reason
);