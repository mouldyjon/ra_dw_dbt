{{
    config(
        materialized='table'
    )
}}
SELECT
  t.id,
  t.spent_date,
  t.user_id,
  t.project_id,
  t.client_id,
  t.invoice_id,
  t.billable,
  t.is_billed,
  t.is_locked,
  t._sdc_batched_at,
  t._sdc_sequence,
  t.billable_rate,
  t.cost_rate,
  t.notes,
  t.hours,
  t.billable_rate * t.hours as billable_revenue,
  case when t.is_billed then t.billable_rate * t.hours end as billed_revenue,
  t.task_assignment_id,
  ht.id as task_id
FROM
  {{ ref('harvest_time_entries') }} t
  join {{ ref('harvest_projects') }} p on t.project_id = p.id
  join {{ ref('customer_master') }} c on p.client_id = c.harvest_customer_id
  left outer join {{ ref('harvest_user_project_tasks') }} upt on t.task_assignment_id = upt.project_task_id and upt.user_id = t.user_id
  left outer join {{ ref('harvest_project_tasks') }} pt on upt.project_task_id = pt.id
  left outer join {{ ref('harvest_tasks') }} ht on pt.task_id = ht.id
  join {{ ref('harvest_users') }} u on t.user_id = u.id
