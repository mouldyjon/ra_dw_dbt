{{
    config(
        materialized='table'
    )
}}
with assignments as (select * from (
SELECT harvest_user_id, a.id, allocation, project_id, start_date, end_date, a._sdc_sequence , max(a._sdc_sequence) over (partition by a.id order by a._sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND unbounded following) as latest_sdc_sequence
FROM {{ ref('harvest_forecast_assignments') }} a
join {{ ref('harvest_forecast_people') }} p
on person_id = p.id)
where _sdc_sequence = latest_sdc_sequence),
projects as (select * from (
SELECT harvest_id, id, client_id, _sdc_sequence , max(_sdc_sequence) over (partition by id order by _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND unbounded following) as latest_sdc_sequence
FROM {{ ref('harvest_forecast_projects') }})
where _sdc_sequence = latest_sdc_sequence),
clients as (select * from (
SELECT harvest_id, id, _sdc_sequence , max(_sdc_sequence) over (partition by id order by _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND unbounded following) as latest_sdc_sequence
FROM {{ ref('harvest_forecast_clients') }})
where _sdc_sequence = latest_sdc_sequence), 
billable_rate as (SELECT user_id, project_id , billable_rate FROM {{ ref('harvest_time_entries') }} 
where billable is true
group by 1,2,3)
select assignments.*, 
timestamp_diff(assignments.end_date, assignments.start_date, DAY)+1 as forecast_days, billable_rate.billable_rate, (billable_rate.billable_rate*8)*(timestamp_diff(assignments.end_date, assignments.start_date, DAY)+1) as forecast_revenue, projects.harvest_id as project_harvest_id, clients.harvest_id as client_harvest_id
from assignments 
join projects on assignments.project_id = projects.id
join clients on projects.client_id = clients.id
join billable_rate on billable_rate.project_id = projects.harvest_id
and  billable_rate.user_id = assignments.harvest_user_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
