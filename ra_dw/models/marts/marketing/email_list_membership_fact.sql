WITH list_members AS (
  SELECT
    *
  FROM
    {{ ref('mailchimp_list_members') }}
),
select_list_members AS (
  SELECT
    _sdc_batched_at,
    list_id,
    email_address,
    list_member_id,
    last_changed_at AS valid_from,
    opted_in_at,
    email_id,
    MAX(_sdc_batched_at) over (
      PARTITION BY list_member_id, valid_from
      ORDER BY _sdc_batched_at
      RANGE BETWEEN unbounded preceding AND unbounded following
    ) AS valid_to
  FROM
    list_members
),
latest_version AS (
  SELECT
    *
  FROM
    select_list_members
  WHERE
    _sdc_batched_at = valid_to
)
SELECT
  *
FROM
  latest_version
