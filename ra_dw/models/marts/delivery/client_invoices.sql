{{
    config(
        materialized='table'
    )
}}
select i.*, e.total_rechargeable_expenses
from {{ ref ('harvest_invoices') }} i
left outer join (select invoice_id, sum(total_cost) as total_rechargeable_expenses FROM {{ ref ('harvest_expenses')}}  where billable group by 1 ) e
on i.id = e.invoice_id
