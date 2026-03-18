-- sql/40_mart/47_mart_dim_date.sql
-- Purpose:
--   GENERATE_DATE_ARRAY で固定範囲の全日付を生成 dim_date を作成・更新する。
-- Grain:
--   1日 = 1行

MERGE `ec-data-platform.mart_olist.dim_date` AS T
USING (
  SELECT DISTINCT
    d AS purchase_date,
    EXTRACT(YEAR FROM purchase_date) AS year_num,
    EXTRACT(MONTH FROM purchase_date) AS month_num,
    EXTRACT(DAY FROM purchase_date) AS day_num,
    FORMAT_DATE('%Y-%m', purchase_date) AS year_month,
    EXTRACT(DAYOFWEEK FROM purchase_date) AS day_of_week_num,
    CASE EXTRACT(DAYOFWEEK FROM purchase_date)
      WHEN 1 THEN 'Sunday'
      WHEN 2 THEN 'Monday'
      WHEN 3 THEN 'Tuesday'
      WHEN 4 THEN 'Wednesday'
      WHEN 5 THEN 'Thursday'
      WHEN 6 THEN 'Friday'
      WHEN 7 THEN 'Saturday'
    END AS day_of_week_name,
    EXTRACT(DAYOFWEEK FROM purchase_date) IN (1, 7) AS is_weekend,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
  FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE '2016-01-01', DATE '2030-12-31', INTERVAL 1 DAY)
  ) AS d
) AS S
ON T.purchase_date = S.purchase_date

WHEN MATCHED THEN
  UPDATE SET
    T.year_num = S.year_num,
    T.month_num = S.month_num,
    T.day_num = S.day_num,
    T.year_month = S.year_month,
    T.day_of_week_num = S.day_of_week_num,
    T.day_of_week_name = S.day_of_week_name,
    T.is_weekend = S.is_weekend,
    T.updated_at = S.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    purchase_date,
    year_num,
    month_num,
    day_num,
    year_month,
    day_of_week_num,
    day_of_week_name,
    is_weekend,
    created_at,
    updated_at
  )
  VALUES (
    S.purchase_date,
    S.year_num,
    S.month_num,
    S.day_num,
    S.year_month,
    S.day_of_week_num,
    S.day_of_week_name,
    S.is_weekend,
    S.created_at,
    S.updated_at
  );