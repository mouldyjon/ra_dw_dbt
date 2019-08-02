{{
    config(
        materialized='table'
    )
}}
SELECT
  PARSE_TIMESTAMP('%m/%d/%Y',period) AS period_ts,
  account_code,
  account,
  account_type,
  description,
  actual_or_budget,
  CASE
    WHEN TRIM(account_type) IN ('OVERHEADS', 'EXPENSE', 'DIRECTCOSTS') THEN CAST(REPLACE(REPLACE(amount,'£ ',''),',','') AS float64)*-1
  ELSE
  CAST(REPLACE(REPLACE(amount,'£ ',''),',','') AS float64)
END
  AS amount
FROM
  {{ ref('profit_and_loss_export')}}
