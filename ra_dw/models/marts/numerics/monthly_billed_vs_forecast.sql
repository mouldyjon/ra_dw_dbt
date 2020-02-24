{{
    config(
        materialized='table'
    )
}}
with forecast_revenue as (SELECT
    (FORMAT_TIMESTAMP('%Y-%m', timestamp(deal_revenue_forecast.month_ts) )) AS deal_revenue_forecast_month_ts_month,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(case when  ( CASE
      WHEN deals.current_stage_label in ('Closed Won and Scheduled','Verbally Won and Working at Risk') then 'Active Projects'
      WHEN deals.current_stage_label in ('Closed Lost') then 'Lost'
      when deals.current_stage_label in ('Closed Won and Delivered') then 'Delivered'
      when deals.current_stage_label in ('Deal Agreed and Awaiting Sign-off','Proposal Sent') then 'About to Close'
      else 'Pipeline' end )  in ('Active Projects','About to Close') then  ( deal_revenue_forecast.weighted_amount_monthly_forecast )  else 0 end  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(( deal_revenue_forecast.deal_id )    AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(( deal_revenue_forecast.deal_id )    AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(( deal_revenue_forecast.deal_id )    AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(( deal_revenue_forecast.deal_id )    AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS deal_revenue_forecast_total_weighted_amount_monthly_forecast
FROM `ra-development.analytics.customer_master`  AS customer_master
LEFT JOIN `ra-development.analytics.bridge_associatedcompanyids`  AS bridge ON customer_master.hubspot_company_id = bridge.associatedcompanyids
INNER JOIN `ra-development.analytics.deals`  AS deals ON deals.deal_id = bridge.deal_id
LEFT JOIN `ra-development.analytics.deal_revenue_forecast` deal_revenue_forecast ON deals.deal_id = deal_revenue_forecast.deal_id
      and  (FORMAT_TIMESTAMP('%F %T', deals.dbt_valid_to )) is null
WHERE ((timestamp(deal_revenue_forecast.month_ts)  ) >= ((TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH))) AND (timestamp(deal_revenue_forecast.month_ts)  ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL 1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))))
GROUP BY
    1),
 actual_billed as (
 SELECT
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(ifnull(harvest_invoices.support_amount_billed,0) + ifnull(license_referral_fee_amount_billed,0) + ifnull(services_amount_billed,0) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(concat(cast(harvest_invoices.id as string),cast(harvest_invoices.invoice_line_item_id as string))   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(concat(cast(harvest_invoices.id as string),cast(harvest_invoices.invoice_line_item_id as string))   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(concat(cast(harvest_invoices.id as string),cast(harvest_invoices.invoice_line_item_id as string))   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(concat(cast(harvest_invoices.id as string),cast(harvest_invoices.invoice_line_item_id as string))   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS harvest_invoices_revenue_amount_billed
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN `ra-development.analytics.timesheets` AS timesheets ON customer_master.harvest_customer_id = timesheets.client_id
    INNER JOIN `ra-development.analytics.projects` AS harvest_projects ON timesheets.project_id = harvest_projects.id
    LEFT JOIN `ra-development.analytics.client_invoices` AS harvest_invoices ON customer_master.harvest_customer_id = harvest_invoices.client_id AND harvest_projects.id = harvest_invoices.project_id
WHERE ((harvest_invoices.created_at  ) >= ((TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH))) AND (harvest_invoices.created_at  ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL 1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))))
 )
select deal_revenue_forecast_total_weighted_amount_monthly_forecast as forecast, 1 as filler, harvest_invoices_revenue_amount_billed as billed
from forecast_revenue join actual_billed on 1=1
