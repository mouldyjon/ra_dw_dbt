{{
    config(
        materialized='table'
    )
}}
SELECT
    (FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(timesheets.spent_date , WEEK(MONDAY)))) AS timesheets_spent_date_week,
    COUNT(DISTINCT timesheets.project_id )  / COUNT(DISTINCT timesheets.user_id ) AS avg_projects_per_consultant
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN `ra-development.analytics.timesheets` AS timesheets ON customer_master.harvest_customer_id = timesheets.client_id
    INNER JOIN `ra-development.analytics.projects` AS harvest_projects ON timesheets.project_id = harvest_projects.id
WHERE (((( ( timesheets.spent_date ) ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)), INTERVAL (-6 * 7) DAY))) AND ( ( timesheets.spent_date ) ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)), INTERVAL (-6 * 7) DAY), INTERVAL (6 * 7) DAY)))))) AND ((timesheets.billable ) AND (harvest_projects.is_active ))
GROUP BY
    1
ORDER BY
    1
LIMIT 500
