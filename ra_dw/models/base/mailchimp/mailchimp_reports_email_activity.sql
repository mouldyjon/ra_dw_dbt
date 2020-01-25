SELECT
  _sdc_received_at AS _sdc_received_at,
  _sdc_sequence AS _sdc_sequence,
  _sdc_table_version AS _sdc_table_version,
  action AS Event,
  campaign_id AS campaign_id,
  email_address AS email_address,
  email_id AS email_id,
  ip AS ip,
  list_id AS list_id,
  list_is_active AS list_is_active,
  TIMESTAMP AS event_at,
  TYPE AS bounce_type,
  url AS url,
FROM
  {{ source(
    'stitch_mailchimp',
    'reports_email_activity'
  ) }}
