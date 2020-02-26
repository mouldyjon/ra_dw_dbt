{{
    config(
        materialized='table'
    )
}}
SELECT
    (CAST(timesheets.spent_date  AS DATE)) AS timesheets_spent_date_date,
    COALESCE(SUM(timesheets.hours ), 0) AS timesheets_total_timesheet_hours
FROM {{ ref('customer_master') }} AS customer_master
    LEFT JOIN {{ ref('timesheets') }} AS timesheets ON customer_master.harvest_customer_id = timesheets.client_id
WHERE  ((timesheets.spent_date  ) >= ((TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH))) AND (timesheets.spent_date  ) < ((TIMESTAMP(CONCAT(CAST(DATE_ADD(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS DATE), INTERVAL 1 MONTH) AS STRING), ' ', CAST(TIME(CAST(TIMESTAMP_TRUNC(CAST(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) AS TIMESTAMP), MONTH) AS TIMESTAMP)) AS STRING))))))
GROUP BY
    1
ORDER BY
    1 DESC
