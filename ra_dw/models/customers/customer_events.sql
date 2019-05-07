{{
    config(
        materialized='table',
        partition_by='DATE(event_ts)'
    )
}}
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY  customer_id ORDER BY event_ts) AS event_seq,
    MIN(CASE WHEN event_type = 'Billable Day' THEN event_ts END) OVER (PARTITION BY customer_id ORDER BY event_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_billable_day_ts,
    MAX(CASE WHEN event_type = 'Billable Day' THEN event_ts END) OVER (PARTITION BY customer_id ORDER BY event_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_billable_day_ts,
    timestamp_diff(current_timestamp(),MAX(CASE WHEN event_type = 'Billable Day' THEN event_ts END) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), DAY) days_since_last_billable_day,
    timestamp_diff(current_timestamp(),MAX(CASE WHEN event_type = 'Incoming Email' THEN event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), DAY) days_since_last_incoming_email,
    timestamp_diff(current_timestamp(),MAX(CASE WHEN event_type = 'Outgoing Email' THEN event_ts end) over (partition by customer_id order by event_ts rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), DAY) days_since_last_outgoing_email,
    date_diff(date(event_ts),MIN( CASE WHEN event_type = 'Billable Day' THEN date(event_ts) END) OVER (PARTITION BY customer_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), MONTH) months_since_first_billable_day,
    date_diff(date(event_ts),MIN( CASE WHEN event_type = 'Billable Day' THEN date(event_ts) END) OVER (PARTITION BY customer_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), WEEK) weeks_since_first_billable_day,
    date_diff(date(event_ts),MIN( date(event_ts)) OVER (PARTITION BY customer_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), MONTH) months_since_first_contact_day,
    date_diff(date(event_ts),MIN( date(event_ts)) OVER (PARTITION BY customer_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), WEEK) weeks_since_first_contact_day,
    MIN(CASE WHEN event_type = 'Client Invoiced' THEN event_ts END) OVER (PARTITION BY customer_id ORDER BY event_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_invoice_day_ts,
    MAX(CASE WHEN event_type = 'Client Invoiced' THEN event_ts END) OVER (PARTITION BY customer_id ORDER BY event_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_invoice_day_ts,
    MAX(CASE WHEN event_type = 'Billable Day' THEN true ELSE false END) OVER (PARTITION BY customer_id ORDER BY event_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS billable_client,
    MAX(CASE WHEN event_type LIKE '%Sales%' THEN true ELSE false END) OVER (PARTITION BY customer_id ORDER BY event_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sales_prospect
FROM
-- sales opportunity stages
    (SELECT
		    deals.lastmodifieddate AS event_ts,
        customer_master.customer_id AS customer_id,
        customer_master.customer_name AS customer_name,
	      deals.dealname AS event_details,
	      deals.dealstage AS event_type,
	      AVG(deals.amount) AS event_value
    FROM
        {{ ref('customer_master') }} AS customer_master
    LEFT JOIN
        {{ ref('deals') }} AS deals
        ON customer_master.hubspot_company_id = deals.associatedcompanyids
    LEFT JOIN
        {{ ref('hubspot_owners') }} AS owners
        ON deals.hubspot_owner_id = CAST(owners.ownerid AS STRING)
    WHERE
        deals.lastmodifieddate IS NOT null
    {{ dbt_utils.group_by(n=5) }}
    UNION ALL
-- consulting days
    SELECT
      	time_entries.spent_date AS event_ts,
      	customer_master.customer_id AS customer_id,
        customer_master.customer_name AS customer_name,
	      projects.name AS event_details,
	      CASE WHEN time_entries.billable THEN 'Billable Day' ELSE 'Non-Billable Day' END AS event_type,
	      time_entries.hours * time_entries.billable_rate AS event_value
    FROM
        {{ ref('customer_master') }} AS customer_master
    LEFT JOIN
        {{ ref('harvest_projects') }} AS projects
        ON customer_master.harvest_customer_id = projects.client_id
    LEFT JOIN
        {{ ref('harvest_time_entries') }} AS time_entries
        ON time_entries.project_id = projects.id
    WHERE
        time_entries.spent_date IS NOT null
    {{ dbt_utils.group_by(n=6) }}
UNION ALL
-- incoming and outgoing emails
    SELECT
      	communications.communication_timestamp AS event_ts,
      	customer_master.customer_id AS customer_id,
        customer_master.customer_name AS customer_name,
        communications.communications_subject AS event_details,
        CASE WHEN communications.communication_type = 'INCOMING_EMAIL' THEN 'Incoming Email'
             WHEN communications.communication_type = 'EMAIL' THEN 'Outgoing Email'
             ELSE communications.communications_subject
        END AS event_type,
        1 AS event_value
    FROM
        {{ ref('customer_master') }} AS customer_master
    LEFT JOIN
        {{ ref('communications') }} AS communications
        ON customer_master.hubspot_company_id = communications.hubspot_company_id
    WHERE
        communications.communication_timestamp IS NOT null
    {{ dbt_utils.group_by(n=5) }}
UNION ALL
-- sales opportunity stages
    SELECT
      	invoices.issue_date AS event_ts,
      	customer_master.customer_id AS customer_id,
        customer_master.customer_name AS customer_name,
	      invoices.subject AS event_details,
        'Client Invoiced' AS event_type,
	      SUM(invoices.amount) AS event_value
    FROM
        {{ ref('customer_master') }} AS customer_master
    LEFT JOIN
        {{ ref('harvest_invoices') }} AS invoices
        ON customer_master.harvest_customer_id = invoices.client_id
    WHERE
        invoices.issue_date IS NOT null
    {{ dbt_utils.group_by(n=5) }}
UNION ALL
    SELECT
	     invoices.issue_date AS event_ts,
	     customer_master.customer_id AS customer_id,
       customer_master.customer_name AS customer_name,
       invoice_line_items.description AS event_details,
       'Client Credited' AS event_type,
	      COALESCE(SUM(invoice_line_items.amount ), 0) AS event_value
    FROM
        {{ ref('customer_master') }} AS customer_master
    LEFT JOIN
        {{ ref('harvest_invoices') }} AS invoices
        ON customer_master.harvest_customer_id = invoices.client_id
    LEFT JOIN
        {{ ref('harvest_invoice_line_items') }} AS invoice_line_items
        ON invoices.id = invoice_line_items.invoice_id
    {{ dbt_utils.group_by(n=5) }}
    HAVING
	     (COALESCE(SUM(invoice_line_items.amount ), 0) < 0)
UNION ALL
    SELECT
        *
    FROM
        (SELECT
            invoices.paid_at AS event_ts,
	          customer_master.customer_id AS customer_id,
            customer_master.customer_name AS customer_name,
            invoices.subject AS event_details,
            CASE WHEN invoices.paid_at <= invoices.due_date THEN 'Client Paid' ELSE 'Client Paid Late' END AS event_type,
	          SUM(invoices.amount) AS event_value
        FROM
            {{ ref('customer_master') }} AS customer_master
        LEFT JOIN {{ ref('harvest_invoices') }}  AS invoices
            ON customer_master.harvest_customer_id = invoices.client_id
        WHERE
          invoices.paid_at IS NOT null
        {{ dbt_utils.group_by(n=5) }}
        )
    )
WHERE
    customer_name NOT IN ('Rittman Analytics', 'MJR Analytics')
