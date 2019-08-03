SELECT
  event_ts,
  c.customer_id,
  CASE
    WHEN event_source = 'clients-into' THEN 'INTO University Partnerships'
END
  AS customer_name,
  event_value AS communications_text,
  1 AS event_value,
  event_target AS communications_from_firstname_lastname
FROM
  {{ ref('all_events') }} e
JOIN
  {{ ref('customer_master') }} c
ON
  CASE
    WHEN event_source = 'clients-into' THEN 'INTO University Partnerships'
END
  = c.customer_name
WHERE
  event_type = 'client_slack_message'
