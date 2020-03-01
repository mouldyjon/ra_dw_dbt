{{
    config(
        materialized='table'
    )
}}
SELECT
    FORMAT_TIMESTAMP('%Y/%W',TIMESTAMP_TRUNC(timesheets.spent_date , WEEK(MONDAY))) AS week,
    trunc(100*(SUM(timesheets.hours) / COUNT(DISTINCT timesheets.user_id )) /40) AS avg_utilisation
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN `ra-development.analytics.timesheets` AS timesheets ON customer_master.harvest_customer_id = timesheets.client_id
    INNER JOIN `ra-development.analytics.harvest_users` AS harvest_users ON timesheets.user_id = harvest_users.id
WHERE (((( ( timesheets.spent_date ) ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)), INTERVAL (-8 * 7) DAY))) AND ( ( timesheets.spent_date ) ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)), INTERVAL (-8 * 7) DAY), INTERVAL (8 * 7) DAY)))))) AND ((timesheets.billable )  OR (concat(concat(harvest_users.first_name,' '),harvest_users.last_name) ) IS NULL))
GROUP BY
    1
ORDER BY
    1
