{{
    config(
        materialized='table'
    )
}}

SELECT
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(( deals.current_amount )   ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS deals_total_amount
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN `ra-development.analytics.bridge_associatedcompanyids` AS bridge ON customer_master.hubspot_company_id = bridge.associatedcompanyids
    INNER JOIN `ra-development.analytics.deals` AS deals ON bridge.deal_id = deals.deal_id
WHERE (deals.current_stage_label ) IN ('Proposal Sent', 'Deal Agreed & Awaiting Sign-off')
LIMIT 500
