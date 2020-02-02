{{
    config(
        materialized='table'
    )
}}
select p.starts_on,
       p.is_active,
       p.id,
       p.cost_budget,
       p.name,
       p.is_fixed_fee,
       p.cost_budget_include_expenses,
       p.fee,
       p.budget,
       p.over_budget_notification_percentage,
       p.code,
       p.ends_on,
       p.budget_by,
       p.client_id,
       p.is_billable,
       p.hourly_rate,
COALESCE(SUM(t.hours ), 0) AS project_total_timesheet_hours
from {{ ref('harvest_projects')}} p
left outer join {{ ref('timesheets')}} t
ON p.id = t.project_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
