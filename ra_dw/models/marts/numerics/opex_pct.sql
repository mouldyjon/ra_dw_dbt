{{
    config(
        materialized='table'
    )
}}
SELECT
    (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )) AS financial_results_period_month,

    round(100*(COALESCE(SUM(financial_results.operating_expenses*-1 ), 0)  /
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(financial_results.gross_sales  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6))) AS opex_pct
FROM {{ ref('financial_results') }} AS financial_results
WHERE ((financial_results.period  ) >= ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))) AND (financial_results.period  ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS DATE), INTERVAL 6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS TIMESTAMP)) AS STRING))))))
GROUP BY
    1
ORDER BY
    1
