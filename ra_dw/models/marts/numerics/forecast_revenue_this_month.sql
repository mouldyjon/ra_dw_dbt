{{
    config(
        materialized='table'
    )
}}
SELECT
  ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE
                      WHEN ( CASE
                        WHEN deals.current_stage_label IN ('Closed Won and Scheduled',
                        'Verbally Won and Working at Risk') THEN 'Active Projects'
                        WHEN deals.current_stage_label IN ('Closed Lost') THEN 'Lost'
                        WHEN deals.current_stage_label IN ('Closed Won and Delivered') THEN 'Delivered'
                        WHEN deals.current_stage_label IN ('Deal Agreed and Awaiting Sign-off', 'Proposal Sent') THEN 'About to Close'
                      ELSE
                      'Pipeline'
                    END
                      ) IN ('Active Projects',
                      'About to Close') THEN ( deal_revenue_forecast.weighted_amount_monthly_forecast )
                    ELSE
                    0
                  END
                    ,
                    0)*(1/1000*1.0), 9) AS NUMERIC) + (CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 1, 15)) AS int64) AS numeric) * 4294967296 + CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 16, 8)) AS int64) AS numeric)) * 0.000000001 )) - SUM(DISTINCT (CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 1, 15)) AS int64) AS numeric) * 4294967296 + CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 16, 8)) AS int64) AS numeric)) * 0.000000001) ) / (1/1000*1.0) AS FLOAT64),
      0), 6) AS monthly_forecast
FROM
  `ra-development.analytics.customer_master` AS customer_master
LEFT JOIN
  `ra-development.analytics.bridge_associatedcompanyids` AS bridge
ON
  customer_master.hubspot_company_id = bridge.associatedcompanyids
INNER JOIN
  `ra-development.analytics.deals` AS deals
ON
  deals.deal_id = bridge.deal_id
LEFT JOIN
  `ra-development.analytics.deal_revenue_forecast` deal_revenue_forecast
ON
  deals.deal_id = deal_revenue_forecast.deal_id
  AND (FORMAT_TIMESTAMP('%F %T', deals.dbt_valid_to )) IS NULL
WHERE
  ((TIMESTAMP(deal_revenue_forecast.month_ts) ) >= ((TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH)))
    AND (TIMESTAMP(deal_revenue_forecast.month_ts) ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL 1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))))
HAVING
  (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE
                        WHEN ( CASE
                          WHEN deals.current_stage_label IN ('Closed Won and Scheduled',
                          'Verbally Won and Working at Risk') THEN 'Active Projects'
                          WHEN deals.current_stage_label IN ('Closed Lost') THEN 'Lost'
                          WHEN deals.current_stage_label IN ('Closed Won and Delivered') THEN 'Delivered'
                          WHEN deals.current_stage_label IN ('Deal Agreed and Awaiting Sign-off', 'Proposal Sent') THEN 'About to Close'
                        ELSE
                        'Pipeline'
                      END
                        ) IN ('Active Projects',
                        'About to Close') THEN ( deal_revenue_forecast.weighted_amount_monthly_forecast )
                      ELSE
                      0
                    END
                      ,
                      0)*(1/1000*1.0), 9) AS NUMERIC) + (CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 1, 15)) AS int64) AS numeric) * 4294967296 + CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 16, 8)) AS int64) AS numeric)) * 0.000000001 )) - SUM(DISTINCT (CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 1, 15)) AS int64) AS numeric) * 4294967296 + CAST(CAST(CONCAT('0x', SUBSTR(to_hex(md5(CAST(( deal_revenue_forecast.deal_id ) AS STRING))), 16, 8)) AS int64) AS numeric)) * 0.000000001) ) / (1/1000*1.0) AS FLOAT64),
        0), 6) > 0)
