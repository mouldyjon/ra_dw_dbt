{{
    config(
        materialized='table'
    )
}}
with net_margin_l3m as (SELECT
    AVG(financial_results.net_margin_pct ) AS net_margin_l3m
FROM ra_data_warehouse_dbt_prod.financial_results AS financial_results
WHERE ((financial_results.period  ) >= ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -3 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))) AND (financial_results.period  ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -3 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS DATE), INTERVAL 3 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -3 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS TIMESTAMP)) AS STRING)))))))
, net_margin_lm as (SELECT
    AVG(financial_results.net_margin_pct ) AS net_margin_lm
FROM ra_data_warehouse_dbt_prod.financial_results AS financial_results
WHERE ((financial_results.period  ) >= ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))) AND (financial_results.period  ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS DATE), INTERVAL 1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS TIMESTAMP)) AS STRING))))))
)
select round(100*net_margin_lm.net_margin_lm) as net_margin_lm, round(100*net_margin_l3m.net_margin_l3m) as net_margin_l3m
from net_margin_lm join net_margin_l3m
on 1=1
