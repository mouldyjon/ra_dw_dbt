{{
    config(
        materialized='table'
    )
}}
SELECT
  c.customer_id,
  e.event_ts,
  e.event_type AS opportunity_stage,
  event_value AS opportunity_value,
  event_target AS notes
FROM
  {{ ref('all_events') }} e
JOIN
  {{ ref('customer_master') }} c
ON
  CAST(e.event_source AS int64) = c.hubspot_company_id
WHERE
  event_type LIKE '%opportunity%'
  OR event_type = 'proposal sent'
GROUP BY
  1,
  2,
  3,
  4,
  5
