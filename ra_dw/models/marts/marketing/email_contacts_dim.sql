WITH contacts AS (
  SELECT
    _sdc_batched_at AS _sdc_batched_at,
    email_address AS email_address,
    ip_opted_in AS ip_opted_in,
    language AS language,
    last_changed_at AS last_changed_at,
    country_code AS country_code,
    latitude AS latitude,
    longitude AS longitude,
    timezone AS timezone,
    member_rating AS member_rating,
    address AS address,
    birthday AS birthday,
    forename AS forename,
    surname AS surname,
    phone_number AS phone_number,
    avg_click_rate AS avg_click_rate,
    avg_open_rate AS avg_open_rate,
    status AS status,
    opted_in_at AS opted_in_at,
    email_id AS email_id,
    _sdc_table_version,
    email_id
  FROM
  {{ ref('mailchimp_list_members') }} list,
  {{ ref('mailchimp_list_segment_members') }} segment
  WHERE
  list.email_id = segment.email_id
),
select_contacts AS (
  SELECT
    *,
    MAX(_sdc_batched_at) over (
      PARTITION BY (
        email_address,
        last_changed_at
      )
      ORDER BY
        _sdc_batched_at RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS max_sdc_batched_at
  FROM
    contacts
),
latest_version AS (
  SELECT
    *
  FROM
    select_contacts
  WHERE
    _sdc_batched_at = max_sdc_batched_at
)
SELECT
  *
FROM
  latest_version
