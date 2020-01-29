WITH reports_email_activity AS (
  SELECT
    *,
    MAX(_sdc_batched_at) over (
      PARTITION BY event_id
      ORDER BY
        _sdc_batched_at RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS max_sdc_batched_at
  FROM
    {{ ref('mailchimp_reports_email_activity') }}
),
latest_version AS (
  SELECT
    *
  FROM
    reports_email_activity
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
links AS (
  SELECT
    url,
    campaign_id,
  FROM
    latest_version
    GROUP BY url, campaign_id
)
SELECT
  *
FROM
  links
