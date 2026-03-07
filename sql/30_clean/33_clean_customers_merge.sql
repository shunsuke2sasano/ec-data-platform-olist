-- sql/30_clean/33_clean_customers_merge.sql
-- Purpose:
--   raw_olist.customers を整形し、clean_olist.customers に UPSERT する。
-- Grain:
--   1 customer_id = 1 row
-- Notes:
--   - 空文字の正規化（TRIM / NULLIF）
--   - 文字列整形（LOWER）
--   - customer_id 単位で重複排除
--   - raw_loaded_at 後勝ち
--   - customer_id が NULL の行は clean テーブルには採用しない

MERGE `ec-data-platform.clean_olist.customers` AS T
USING (
  WITH base AS (
    SELECT
      NULLIF(TRIM(customer_id), '') AS customer_id,
      NULLIF(TRIM(customer_unique_id), '') AS customer_unique_id,
      NULLIF(TRIM(customer_zip_code_prefix), '') AS customer_zip_code_prefix,
      LOWER(NULLIF(TRIM(customer_city), '')) AS customer_city,
      LOWER(NULLIF(TRIM(customer_state), '')) AS customer_state,

      ingest_date AS raw_ingest_date,
      loaded_at AS raw_loaded_at,
      source_file,

      NULLIF(TRIM(customer_id), '') IS NOT NULL AS is_customer_id_present,
      CASE
        WHEN NULLIF(TRIM(customer_id), '') IS NULL THEN 'customer_id_missing'
        ELSE NULL
      END AS dq_error_reason
    FROM `ec-data-platform.raw_olist.customers`
  ),

  deduped AS (
    SELECT *
    FROM base
    WHERE customer_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY raw_loaded_at DESC, raw_ingest_date DESC, source_file DESC
    ) = 1
  )

  SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    is_customer_id_present,
    dq_error_reason,
    raw_ingest_date,
    raw_loaded_at,
    source_file,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
  FROM deduped
) AS S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN
  UPDATE SET
    T.customer_unique_id = S.customer_unique_id,
    T.customer_zip_code_prefix = S.customer_zip_code_prefix,
    T.customer_city = S.customer_city,
    T.customer_state = S.customer_state,
    T.is_customer_id_present = S.is_customer_id_present,
    T.dq_error_reason = S.dq_error_reason,
    T.raw_ingest_date = S.raw_ingest_date,
    T.raw_loaded_at = S.raw_loaded_at,
    T.source_file = S.source_file,
    T.updated_at = S.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    is_customer_id_present,
    dq_error_reason,
    raw_ingest_date,
    raw_loaded_at,
    source_file,
    created_at,
    updated_at
  )
  VALUES (
    S.customer_id,
    S.customer_unique_id,
    S.customer_zip_code_prefix,
    S.customer_city,
    S.customer_state,
    S.is_customer_id_present,
    S.dq_error_reason,
    S.raw_ingest_date,
    S.raw_loaded_at,
    S.source_file,
    S.created_at,
    S.updated_at
  );