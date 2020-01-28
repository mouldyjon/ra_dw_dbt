WITH segment_members AS (
  SELECT
    *
  FROM
    {{ ref('mailchimp_segment_members') }}
),
select_segment_members AS (
  SELECT
    _sdc_batched_at,
    email_address,
    list_member_id,
    last_changed_at,
    segment_id,
    opted_in_at,
    email_id,
    unsubscribe_reason,
    MAX(_sdc_batched_at) over (
      PARTITION BY list_member_id
      ORDER BY
        _sdc_batched_at RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS max_sdc_batched_at
  FROM
    segment_members
),
latest_version AS (
  SELECT
    *
  FROM
    select_segment_members
  WHERE
    _sdc_batched_at = max_sdc_batched_at
)
SELECT
  *
FROM
  latest_version