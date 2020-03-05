{{
    config(
        materialized='table',
        schema='numerics'
    )
}}
with projects as (SELECT
    customer_master.customer_name,
    harvest_projects.name  AS harvest_projects_name,
    harvest_projects.id  AS harvest_projects_id,
    harvest_projects.project_total_timesheet_hours AS harvest_projects_project_total_timesheet_hours,
        (CASE WHEN harvest_projects.is_fixed_fee  THEN 'Yes' ELSE 'No' END) AS harvest_projects_project_fixed_fee,
    case when harvest_projects.budget_by = 'project' then 'Total Project Hours' else harvest_projects.budget_by end AS harvest_projects_budget_by,
    harvest_projects.budget  AS harvest_projects_budget
FROM {{ ref('customer_master') }} AS customer_master
    LEFT JOIN {{ ref('timesheets') }} AS timesheets ON customer_master.harvest_customer_id = timesheets.client_id
    INNER JOIN {{ ref('projects') }} AS harvest_projects ON timesheets.project_id = harvest_projects.id
WHERE (customer_master.customer_name NOT LIKE '%Rittman Analytics%' AND customer_master.customer_name NOT LIKE '%Rittman Analytics Internal%' AND (customer_master.customer_name NOT LIKE '%Brighton & Hove City Council%' AND customer_master.customer_name NOT LIKE '%East Sussex County Council%') OR customer_master.customer_name IS NULL) AND (harvest_projects.is_active ) AND ((harvest_projects.is_billable ) AND (harvest_projects.budget ) IS NOT NULL) and harvest_projects.name not in ('Sprint 1 (Hubspot)','Monthly Support Call-Off Hours','Sprint #2 : Hubspot Module Customization')
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7)
select concat(concat(customer_name,': '),harvest_projects_name) as project,
ROUND(100*(harvest_projects_project_total_timesheet_hours / harvest_projects_budget))	as budget_used
from projects
order by 2 desc
LIMIT 10
