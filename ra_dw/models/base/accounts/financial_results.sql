{{
    config(
        materialized='table'
    )
}}
SELECT
  period,
  ROUND(ifnull(gross_sales,
      0),2) AS gross_sales,
  ROUND((ifnull(gross_sales,
        0)+ifnull(cogs,
        0))/gross_sales,2) AS gross_margin_pct,
  ROUND(ifnull(operating_expenses,
      0),2) AS operating_expenses,
  ROUND(ifnull(cogs,
      0),2) AS cogs,
  ROUND((ifnull(gross_sales,
        0) + ifnull(cogs,
        0)),2) AS net_sales,
  ROUND((ifnull(gross_sales,
        0) + ifnull(operating_expenses,
        0) + ifnull(cogs,
        0)),2) AS net_profit,
  ROUND((gross_sales + ifnull(operating_expenses,
        0) + ifnull(cogs,
        0)) / gross_sales,2) AS net_margin_pct
FROM (
  SELECT
    period,
    SUM(gross_sales) AS gross_sales,
    SUM(operating_expenses) AS operating_expenses,
    SUM(cogs) AS cogs
  FROM (
    SELECT
      period,
      CASE
        WHEN account_type = 'Sales' THEN total_amount
    END
      AS gross_sales,
      CASE
        WHEN account_type = 'Operating Expenses' THEN total_amount
    END
      AS operating_expenses,
      CASE
        WHEN account_type = 'Cost of Goods Sold' THEN total_amount
    END
      AS cogs
    FROM (
      SELECT
        CASE
          WHEN xero_profit_and_loss.account_type IN ('REVENUE', 'OTHERINCOME') THEN 'Sales'
          WHEN xero_profit_and_loss.account_type IN ('OVERHEADS',
          'EXPENSE') THEN 'Operating Expenses'
          WHEN xero_profit_and_loss.account_type = 'DIRECTCOSTS' THEN 'Cost of Goods Sold'
      END
        AS account_type,
        xero_profit_and_loss.period_ts AS period,
        COALESCE(SUM(xero_profit_and_loss.amount ),
          0) AS total_amount
      FROM
        {{ ref('xero_profit_and_loss') }} AS xero_profit_and_loss
      GROUP BY
        1,
        2))
  GROUP BY
    1)
WHERE
  period IS NOT NULL
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
ORDER BY
  1
