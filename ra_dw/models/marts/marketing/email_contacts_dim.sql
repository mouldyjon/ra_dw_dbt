WITH contacts AS (
  SELECT
    _sdc_batched_at AS _sdc_batched_at,
    email_address AS email_address,
    ip_opted_in AS ip_opted_in,
    language AS language,
    last_changed_at AS valid_from,
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
    _sdc_table_version
  FROM
  {{ ref('mailchimp_list_members') }}
),

select_contacts AS (
  SELECT
    *,
    MAX(_sdc_batched_at) over (
      PARTITION BY
        email_id,
        valid_from
      ORDER BY
        _sdc_batched_at RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS valid_to
  FROM
    contacts
),

latest_version AS (
  SELECT
    *
  FROM
    select_contacts
  WHERE
    _sdc_batched_at = valid_to
)
SELECT
  *
FROM
  latest_version
