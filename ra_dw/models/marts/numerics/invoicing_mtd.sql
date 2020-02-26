{{
    config(
        materialized='table'
    )
}}
SELECT c.customer_name, sum(case when i.currency = 'USD' then i.revenue_amount_billed * .79 else i.revenue_amount_billed end) as billing 
from {{ ref('customer_master') }} c
left outer join {{ ref('client_invoices') }} i
on c.harvest_customer_id = i.client_id
 where
 state in ('open','draft')
 and date_trunc(date(created_at),MONTH) = date_trunc(date(current_timestamp),MONTH)
 or date_trunc(date(sent_at),MONTH) = date_trunc(date(current_timestamp),MONTH)
 group by 1
 order by 2 desc
