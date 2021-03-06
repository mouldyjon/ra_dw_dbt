{{
    config(
        materialized='table',
        partition_by='DATE(event_ts)'
    )
}}
SELECT
    *,
    {{ dbt_utils.datediff('last_billable_day_ts', current_timestamp(), 'day')}} AS days_since_last_billable_day,
    {{ dbt_utils.datediff('last_incoming_email_ts', current_timestamp(), 'day')}} AS days_since_last_incoming_email,
    {{ dbt_utils.datediff('last_outgoing_email_ts', current_timestamp(), 'day')}} AS days_since_last_outgoing_email
FROM
  (SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts) AS event_seq,
      MIN(CASE WHEN event_type = 'Billable Day' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS first_billable_day_ts,
      MAX(CASE WHEN event_type = 'Billable Day' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_billable_day_ts,
      MIN(CASE WHEN event_type = 'Client Invoiced' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS first_invoice_day_ts,
      MAX(CASE WHEN event_type = 'Client Invoiced' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_invoice_day_ts,
      MAX(CASE WHEN event_type = 'Incoming Email' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_incoming_email_ts,
      MAX(CASE WHEN event_type = 'Outgoing Email' THEN event_ts END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS last_outgoing_email_ts,
      MIN(event_ts)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS first_contact_ts,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type = 'Billable Day' THEN date(event_ts) END)          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},MONTH) AS months_since_first_billable_day,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type = 'Billable Day' THEN date(event_ts) END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},WEEK) AS weeks_since_first_billable_day,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type like '%Email%' THEN date(event_ts) END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},MONTH) AS months_since_first_contact_day,
      DATE_DIFF(date(event_ts),MIN(CASE WHEN event_type like '%Email%' THEN date(event_ts) END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }},WEEK) AS weeks_since_first_contact_day,
      MAX(CASE WHEN event_type = 'Billable Day' THEN true ELSE false END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS billable_client,
      MAX(CASE WHEN event_type LIKE '%Sales%' THEN true ELSE false END)
          {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS sales_prospect
  FROM
  -- sales opportunity stages
      (SELECT
  		    deals.current_dealstage_ts AS event_ts,
          customer_master.customer_id AS customer_id,
          customer_master.customer_name AS customer_name,
          deals.partner_referral_type as event_source,
  	      deals.current_dealname AS event_details,
  	      deals.current_stage_label AS event_type,
  	      deals.current_amount * deals.current_probability AS event_value,
          1 as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('deals') }} AS deals
          ON customer_master.hubspot_company_id = deals.associatedcompanyids
      LEFT JOIN
          {{ ref('owners') }} AS owners
          ON deals.hubspot_owner_id = CAST(owners.ownerid AS STRING)
      UNION ALL

  -- incoming and outgoing emails
      SELECT
        	communications.communication_timestamp AS event_ts,
        	customer_master.customer_id AS customer_id,
          customer_master.customer_name AS customer_name,
          communications.communications_from_firstname_lastname as event_source,
          communications.communications_subject AS event_details,
          CASE WHEN communications.communication_type = 'INCOMING_EMAIL' THEN 'Incoming Email'
               WHEN communications.communication_type = 'EMAIL' THEN 'Outgoing Email'
               ELSE communications.communications_subject
          END AS event_type,
          1 AS event_value,
          1 as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('communications') }} AS communications
          ON customer_master.hubspot_company_id = communications.hubspot_company_id
      WHERE
          communications.communication_timestamp IS NOT null
      {{ dbt_utils.group_by(n=8) }}
  UNION ALL
      SELECT
        	invoices.issue_date AS event_ts,
        	customer_master.customer_id AS customer_id,
          customer_master.customer_name AS customer_name,
          cast(null as string) as event_source,
  	      invoices.subject AS event_details,
          'Client Invoiced' AS event_type,
  	      SUM(invoices.revenue_amount_billed) AS event_value,
          sum(1) as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('client_invoices') }} AS invoices
          ON customer_master.harvest_customer_id = invoices.client_id
      WHERE
          invoices.issue_date IS NOT null
      {{ dbt_utils.group_by(n=6) }}
  UNION ALL
      SELECT
  	     invoices.issue_date AS event_ts,
  	     customer_master.customer_id AS customer_id,
         customer_master.customer_name AS customer_name,
         cast(null as string) as event_source,
         invoice_line_items.description AS event_details,
         'Client Credited' AS event_type,
  	      COALESCE(SUM(invoice_line_items.amount ), 0) AS event_value,
          sum(1) as event_units
      FROM
          {{ ref('customer_master') }} AS customer_master
      LEFT JOIN
          {{ ref('harvest_invoices') }} AS invoices
          ON customer_master.harvest_customer_id = invoices.client_id
      LEFT JOIN
          {{ ref('harvest_invoice_line_items') }} AS invoice_line_items
          ON invoices.id = invoice_line_items.invoice_id
      {{ dbt_utils.group_by(n=6) }}
      HAVING
  	     (COALESCE(SUM(invoice_line_items.amount ), 0) < 0)
  UNION ALL
      SELECT
              pageviews.event_ts AS event_ts,
              customer_master.customer_id  AS customer_id,
              customer_master.customer_name AS customer_name,
              pageviews.visitor_city as event_source,
              pageviews.page_title AS event_details,
              concat(pageviews.site,' site visit') AS event_type,
              sum(1) as event_value,
              sum(1) as event_units
      FROM
          {{ ref('customer_master') }}  AS customer_master
     LEFT JOIN
          {{ ref('pageviews') }} AS pageviews
          ON customer_master.customer_id = pageviews.customer_id
      {{ dbt_utils.group_by(n=6) }}
  UNION ALL
      SELECT
      timestamp_trunc(History_Completed_Time,DAY) AS event_ts,
      c.customer_id AS customer_id,
      c.customer_name AS customer_name,
      all_history.User_Name as event_source,
      all_history.dashboard_title as event_details,
      'daily_looker_usage_mins' AS event_type,
      SUM(all_history.History_Approximate_Web_Usage_in_Minutes )/60 AS event_value,
      sum(1) as event_units
    FROM
      {{ ref('all_history') }} AS all_history
    JOIN
      {{ ref('customer_master') }} AS c
    ON
      all_history.site = c.customer_name
    {{ dbt_utils.group_by(n=6) }}

  UNION ALL
          SELECT
                  event_ts,
                  customer_master.customer_id  AS customer_id,
                  customer_master.customer_name AS customer_name,
                  client_slack_messages.communications_from_firstname_lastname as event_source,
                  client_slack_messages.communications_text AS event_details,
                  'client_slack_message' AS event_type,
                  sum(1) as event_value,
                  sum(1) as event_units
          FROM
              {{ ref('customer_master') }}  AS customer_master
         LEFT JOIN
              {{ ref('client_slack_messages') }} AS client_slack_messages
              ON customer_master.customer_id = client_slack_messages.customer_id
          {{ dbt_utils.group_by(n=6) }}
  UNION ALL


SELECT
    tickets.status_change_ts AS event_ts,
    customer_master.customer_id  AS customer_id,
    customer_master.customer_name AS customer_name,
    tickets.project_name  AS event_source,
    tickets.summary  AS event_details,
    'jira_ticket_closed' as event_type,
    COALESCE(SUM(tickets.issue_hours_to_complete ), 0) AS event_value,
    COUNT(tickets.id ) AS event_units
FROM `ra-development.analytics.customer_master` AS customer_master
    LEFT JOIN (SELECT * from `ra-development.ra_data_warehouse_dbt_prod.tickets`
        ) AS tickets ON customer_master.customer_id = tickets.customer_id
WHERE (tickets.statuscategory ) = 'Done'
GROUP BY
    1,
    2,
    3,
    4,
    5
  UNION ALL
  SELECT
    timesheets.spent_date  AS event_ts,
    customer_master.customer_id  AS customer_id,
    customer_master.customer_name AS customer_name,
    harvest_projects.code  AS event_source,
    concat(concat(harvest_users.first_name,' '),harvest_users.last_name)  AS event_details,
    'timesheet_hours_logged' as event_type,
    timesheets.billable_rate  AS event_value,
    timesheets.hours  AS event_units
   FROM {{ ref('customer_master') }} AS customer_master
   LEFT JOIN {{ ref('timesheets') }} AS timesheets ON customer_master.harvest_customer_id = timesheets.client_id
   INNER JOIN {{ ref('projects') }} AS harvest_projects ON timesheets.project_id = harvest_projects.id
   INNER JOIN {{ ref('harvest_users') }} AS harvest_users ON timesheets.user_id = harvest_users.id

))
