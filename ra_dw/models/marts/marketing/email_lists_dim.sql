WITH lists AS (
  SELECT
    *
  FROM
    {{ ref('mailchimp_lists') }}
),
select_lists AS (
  SELECT
    list_id,
    default_from_email,
    default_from_name,
    default_language,
    default_subject,
    default_from_address1,
    default_from_address2,
    default_from_city,
    default_from_company,
    default_from_country,
    default_from_phone,
    default_from_state,
    default_from_zip,
    NAME,
    last_sub_date,
    last_unsub_date,
    member_count,
    member_count_since_send,
    open_rate,
    target_sub_rate,
    unsubscribe_count,
    unsubscribe_count_since_send,
    subscribe_url_long,
    visibility,
    _sdc_batched_at,
    MAX(_sdc_batched_at) over (
      PARTITION BY list_id
      ORDER BY
        _sdc_batched_at RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS max_sdc_batched_at
    FROM lists
),
latest_version AS (
  SELECT
    *
  FROM
    select_lists
  WHERE
    _sdc_batched_at = max_sdc_batched_at
)

SELECT
  *
FROM
  latest_version
