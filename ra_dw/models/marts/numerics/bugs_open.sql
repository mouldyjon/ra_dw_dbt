{{
    config(
        materialized='table',
        schema='numerics'
    )
}}
WITH tickets AS (SELECT * from {{ ref('tickets') }}
      )
SELECT
    COUNT(tickets.id ) AS tickets_total
FROM {{ ref('customer_master') }} AS customer_master
    LEFT JOIN tickets AS tickets ON customer_master.customer_id = tickets.customer_id
WHERE (tickets.issuetype_name ) = 'Bug'AND (NOT COALESCE(tickets.issue_completed  , FALSE))
