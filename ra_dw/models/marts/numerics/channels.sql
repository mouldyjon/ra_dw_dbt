{{
    config(
        materialized='table'
    )
}}

SELECT
    coalesce(deals.current_partner_referral_type,'Direct')  AS channel,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(( cast(deals.current_amount * (deals.current_probability/1) as int64))   ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(( deals.deal_id )    AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS weighted_amount
FROM {{ ref('customer_master') }} AS customer_master
    LEFT JOIN {{ ref('bridge_associatedcompanyids') }} AS bridge ON customer_master.hubspot_company_id = bridge.associatedcompanyids
    INNER JOIN {{ ref('deals') }} AS deals ON bridge.deal_id = deals.deal_id
WHERE (deals.current_stage_label ) IN ('Closed Won and Scheduled', 'Verbally Won and Working at Risk', 'Deal Agreed & Awaiting Sign-off', 'Proposal Sent', 'Presentation Given & Sprints Scoped', 'Initial Enquiry', 'Meeting and Sales Qualified')
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 500
