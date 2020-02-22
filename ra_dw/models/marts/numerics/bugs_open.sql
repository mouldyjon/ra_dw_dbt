{{
    config(
        materialized='table'
    )
}}
WITH tickets AS (SELECT * from `ra-development.ra_data_warehouse_dbt_prod.tickets`
      )
SELECT
    COUNT(tickets.id ) AS tickets_total
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN `ra-development.analytics.tickets` AS tickets ON customer_master.customer_id = tickets.customer_id
WHERE (tickets.issuetype_name ) = 'Bug'AND (NOT COALESCE(tickets.issue_completed  , FALSE))
