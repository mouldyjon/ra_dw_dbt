{{
    config(
        materialized='table'
    )
}}
SELECT
    'YTD Average' AS period,
    AVG(financial_results.net_margin_pct ) AS financial_results_avg_net_margin_pct
FROM `ra-development.analytics.financial_results` AS financial_results
WHERE (TIMESTAMP_TRUNC(CAST(financial_results.period  AS TIMESTAMP), MONTH)  >= CAST(CONCAT(CAST(CAST(2019 AS INT64) AS STRING), CAST('-' AS STRING), CAST(LPAD(CAST(CAST(11 AS INT64) AS STRING), 2, '0') AS STRING), CAST('-' AS STRING), CAST(LPAD(CAST(CAST(01 AS INT64) AS STRING), 2, '0') AS STRING), CAST(' 00:00:00' AS STRING)) AS TIMESTAMP)) AND (TIMESTAMP_TRUNC(CAST(financial_results.period  AS TIMESTAMP), MONTH)  < TIMESTAMP_TRUNC(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), MONTH))
GROUP BY
    1
ORDER BY
    2 DESC
