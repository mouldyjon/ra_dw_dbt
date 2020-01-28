WITH contacts AS (
  SELECT
    COALESCE(list._sdc_batched_at, segment._sdc_batched_at) AS _sdc_batched_at,
    COALESCE(list.email_address, segment.email_address) AS email_address,
    COALESCE(list.ip_opted_in, segment.ip_opted_in) AS ip_opted_in,
    COALESCE(list.language, segment.language) AS language,
    COALESCE(list.last_changed_at, segment.last_changed_at) AS last_changed_at,
    COALESCE(list.country_code, segment.country_code) AS country_code,
    COALESCE(list.latitude, segment.latitude) AS latitude,
    COALESCE(list.longitude, segment.longitude) AS longitude,
    COALESCE(list.timezone, segment.timezone) AS timezone,
    COALESCE(list.member_rating, segment.member_rating) AS member_rating,
    COALESCE(list.address, segment.address) AS address,
    COALESCE(list.birthday, segment.birthday) AS birthday,
    COALESCE(list.forename, segment.forename) AS forename,
    COALESCE(list.surname, segment.surname) AS surname,
    COALESCE(list.phone_number, segment.phone_number) AS phone_number,
    COALESCE(list.avg_click_rate, segment.avg_click_rate) AS avg_click_rate,
    COALESCE(list.avg_open_rate, segment.avg_open_rate) AS avg_open_rate,
    COALESCE(list.status, segment.status) AS status,
    COALESCE(list.opted_in_at, segment.opted_in_at) AS opted_in_at,
    COALESCE(list.email_id, segment.email_id) AS email_id,
    COALESCE(list.unsubscribe_reason, segment.unsubscribe_reason) AS unsubscribe_reason,
    list._sdc_table_version,
    list.email_id
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
        last_changed_at,
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
