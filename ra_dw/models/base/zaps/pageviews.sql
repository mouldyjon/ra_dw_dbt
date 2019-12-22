{{
    config(
        materialized='table'
    )
}}
SELECT
  ifnull(m.customer_id,-999) as customer_id,
  event_ts,
  replace(event_type,'_pageview','') as site,
  event_source as visitor_city,
  event_value as visitor_domain,
  event_target as page_title,
  event_details as page_url
FROM
  {{ ref('all_events') }} e
LEFT OUTER JOIN
  {{ ref('customer_master') }} m
ON
  e.event_value = m.web_domain
WHERE
  event_type IN ('company_website_pageview',
    'drilltodetail_website_pageview')
