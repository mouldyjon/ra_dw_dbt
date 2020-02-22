{{
    config(
        materialized='table'
    )
}}
WITH tickets AS (SELECT * from `ra-development.ra_data_warehouse_dbt_prod.tickets`
      )
SELECT
    tickets.displayname  AS tickets_displayname,
    COUNT(tickets.id ) AS tickets_total
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN `ra-development.analytics.tickets` AS tickets ON customer_master.customer_id = tickets.customer_id
WHERE (tickets.issuetype_name ) = 'Support'AND (NOT COALESCE(tickets.issue_completed  , FALSE))
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 500
