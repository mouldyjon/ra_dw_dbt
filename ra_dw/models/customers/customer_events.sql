{{
    config(
        materialized='table', partition_by='DATE(event_ts)'
    )
}}

select *, 
row_number() over (partition by customer_id order by event_ts) as event_seq,
min(case when event_type = 'Billable Day' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) first_billable_day_ts,
max(case when event_type = 'Billable Day' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) last_billable_day_ts,
timestamp_diff(current_timestamp(),max(case when event_type = 'Billable Day' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING), DAY) days_since_last_billable_day, 
timestamp_diff(current_timestamp(),max(case when event_type = 'Incoming Email' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING), DAY) days_since_last_incoming_email, 
timestamp_diff(current_timestamp(),max(case when event_type = 'Outgoing Email' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING), DAY) days_since_last_outgoing_email,
date_diff(date(event_ts),min(case when event_type = 'Billable Day' then date(event_ts) end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING), MONTH) months_since_first_billable_day,
date_diff(date(event_ts),min(case when event_type = 'Billable Day' then date(event_ts) end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING), WEEK) weeks_since_first_billable_day,
date_diff(date(event_ts),min(date(event_ts)) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING), MONTH) months_since_first_contact_day,
date_diff(date(event_ts),min(date(event_ts)) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING), WEEK) weeks_since_first_contact_day,
min(case when event_type = 'Client Invoiced' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) first_invoice_day_ts,
max(case when event_type = 'Client Invoiced' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) last_invoice_day_ts,
max(case when event_type = 'Site Visited' then event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) last_site_visit_day_ts,
max(case when event_type = 'Billable Day' then true else false end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as billable_client,
max(case when event_type like '%Sales%' then true else false end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as sales_prospect,
max(case when event_type like '%Sales%' then true else false end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as site_visitor,
max(case when event_details like '%Blog%' then true else false end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as blog_reader,
max(case when event_details like '%Podcast%' then true else false end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as podcast_reader
from (
-- sales opportunity stages
SELECT 
		deals.lastmodifieddate AS event_ts,
   customer_master.customer_id as customer_id,
       customer_master.customer_name as customer_name,
	deals.dealname  AS event_details,
	deals.dealstage  AS event_type,
	AVG(deals.amount ) AS event_value
FROM {{ ref('customer_master') }}  AS customer_master
LEFT JOIN {{ ref('deals') }}  AS deals ON customer_master.hubspot_company_id = deals.associatedcompanyids
LEFT JOIN {{ ref('hubspot_owners') }}  AS owners ON deals.hubspot_owner_id = cast(owners.ownerid as string)
where deals.lastmodifieddate is not null

GROUP BY 1,2,3,4,5
union all
-- consulting days
SELECT 
	time_entries.spent_date AS event_ts,
	customer_master.customer_id  AS customer_id,
         customer_master.customer_name as customer_name,

	projects.name  AS event_details,
	CASE WHEN time_entries.billable  THEN 'Billable Day' ELSE 'Non-Billable Day' END AS event_type,
	time_entries.hours * time_entries.billable_rate  AS event_value
FROM `ra-development.ra_data_warehouse.customer_master`  AS customer_master
LEFT JOIN {{ ref('harvest_projects') }}  AS projects ON customer_master.harvest_customer_id = projects.client_id
LEFT JOIN {{ ref('harvest_time_entries') }}  AS time_entries ON time_entries.project_id = projects.id
WHERE 
	time_entries.spent_date is not null
GROUP BY 1,2,3,4,5,6
union all
-- incoming and outgoing emails
SELECT 
	communications.communication_timestamp  AS event_ts,
	customer_master.customer_id  AS customer_id,
         customer_master.customer_name as customer_name,

	
  	communications.communications_subject  AS event_details,
    case when communications.communication_type = 'INCOMING_EMAIL' then 'Incoming Email'
  when communications.communication_type = 'EMAIL' then 'Outgoing Email'
       else communications.communications_subject end AS event_type,
    1 as event_value
FROM `ra-development.ra_data_warehouse.customer_master`  AS customer_master
LEFT JOIN `ra-development.ra_data_warehouse.communications`  AS communications ON customer_master.hubspot_company_id = communications.hubspot_company_id
where communications.communication_timestamp is not null
GROUP BY 1,2,3,4,5
union all
-- sales opportunity stages
SELECT 
	invoices.issue_date AS event_ts,
	customer_master.customer_id  AS customer_id,
         customer_master.customer_name as customer_name,

	invoices.subject  AS event_details,
  'Client Invoiced' as event_type,
	sum(invoices.amount)  AS event_value
FROM {{ ref('customer_master') }}  AS customer_master
LEFT JOIN {{ ref('harvest_invoices') }}  AS invoices ON customer_master.harvest_customer_id = invoices.client_id
where invoices.issue_date is not null
GROUP BY 1,2,3,4,5
union all
SELECT 
	invoices.issue_date AS event_ts,
	customer_master.customer_id  AS customer_id,
           customer_master.customer_name as customer_name,

	invoice_line_items.description  AS event_details,
  'Client Credited' as event_type,
	COALESCE(SUM(invoice_line_items.amount ), 0) AS event_value
FROM {{ ref('customer_master') }} AS customer_master
LEFT JOIN {{ ref('harvest_invoices') }}   AS invoices ON customer_master.harvest_customer_id = invoices.client_id
LEFT JOIN {{ ref('harvest_invoice_line_items') }}   AS invoice_line_items ON invoices.id = invoice_line_items.invoice_id 
GROUP BY 1,2,3,4,5
HAVING 
	(COALESCE(SUM(invoice_line_items.amount ), 0) < 0)
union all
select * from (
select invoices.paid_at AS event_ts,
	customer_master.customer_id  AS customer_id,
           customer_master.customer_name as customer_name,

  invoices.subject  AS event_details,  
  case when invoices.paid_at <= invoices.due_date then 'Client Paid' else 'Client Paid Late' end as event_type,
	sum(invoices.amount)  AS event_value
  FROM {{ ref('customer_master') }}  AS customer_master
  LEFT JOIN {{ ref('harvest_invoices') }}  AS invoices ON customer_master.harvest_customer_id = invoices.client_id
  where invoices.paid_at is not null
GROUP BY 1,2,3,4,5)
union all
select pageviews.timestamp AS event_ts,
        customer_master.customer_id  AS customer_id,
           customer_master.customer_name as customer_name,

  pageviews.page_subcategory  AS event_details,
  'Site Visited' as event_type,
  sum(1) as event_value
  FROM {{ ref('customer_master') }}  AS customer_master
  LEFT JOIN {{ ref('pageviews') }} AS pageviews ON customer_master.customer_name = pageviews.network
GROUP BY 1,2,3,4,5


)
where customer_name not in ('Rittman Analytics','MJR Analytics')











