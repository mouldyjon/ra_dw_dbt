SELECT
    (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )) AS financial_results_period_month,
    COALESCE(SUM(financial_results.net_profit ), 0) AS financial_results_total_net_profit,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(financial_results.gross_sales  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(cast( ( (FORMAT_TIMESTAMP('%Y-%m', financial_results.period )))  as string)   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS financial_results_total_gross_sales
FROM `ra-development.ra_data_warehouse_dbt_prod.financial_results` AS financial_results
WHERE (TIMESTAMP_TRUNC(CAST(financial_results.period  AS TIMESTAMP), MONTH)  >= CAST(CONCAT(CAST(CAST(2019 AS INT64) AS STRING), CAST('-' AS STRING), CAST(LPAD(CAST(CAST(11 AS INT64) AS STRING), 2, '0') AS STRING), CAST('-' AS STRING), CAST(LPAD(CAST(CAST(01 AS INT64) AS STRING), 2, '0') AS STRING), CAST(' 00:00:00' AS STRING)) AS TIMESTAMP)) AND (TIMESTAMP_TRUNC(CAST(financial_results.period  AS TIMESTAMP), MONTH)  < TIMESTAMP_TRUNC(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), MONTH))
GROUP BY
    1
ORDER BY
