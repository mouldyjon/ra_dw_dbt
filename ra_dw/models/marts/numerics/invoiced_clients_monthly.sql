{{
    config(
        materialized='table'
    )
}}
SELECT
    (FORMAT_TIMESTAMP('%Y-%m', harvest_invoices.created_at )) AS harvest_invoices_project_invoice_created_month,
    COUNT(DISTINCT harvest_invoices.client_id ) AS harvest_invoices_total_clients
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN `ra-development.analytics.timesheets` AS timesheets ON customer_master.harvest_customer_id = timesheets.client_id
    INNER JOIN `ra-development.analytics.projects` AS harvest_projects ON timesheets.project_id = harvest_projects.id
    LEFT JOIN `ra-development.analytics.client_invoices` AS harvest_invoices ON customer_master.harvest_customer_id = harvest_invoices.client_id AND harvest_projects.id = harvest_invoices.project_id
WHERE ((harvest_invoices.created_at  ) >= ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))) AND (harvest_invoices.created_at  ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS DATE), INTERVAL 6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL -6 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))) AS TIMESTAMP)) AS STRING))))))
GROUP BY
    1
ORDER BY
    1
