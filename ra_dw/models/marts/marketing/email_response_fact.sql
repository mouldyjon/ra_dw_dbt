WITH responses AS (
  SELECT
    *
  FROM
    {{ ref('mailchimp_reports_email_activity') }}
),
select_responses AS (
  SELECT
  *,
    MAX(_sdc_batched_at) over (
      PARTITION BY event_id
      ORDER BY _sdc_batched_at
      RANGE BETWEEN unbounded preceding AND unbounded following
    ) AS max_sdc_batched_at
  FROM
  responses
),
latest_version AS (
  SELECT
    *
  FROM
    select_responses
  WHERE
    _sdc_batched_at = max_sdc_batched_at
)
SELECT
  *
FROM
  latest_version
