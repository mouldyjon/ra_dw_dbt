WITH segments AS (
  SELECT
    *
  FROM
    {{ ref('mailchimp_list_segments') }}
),
select_segments AS (
  SELECT
  _sdc_batched_at,
  _sdc_received_at,
  _sdc_sequence,
  _sdc_table_version,
  created_at,
  segement_id,
  list_id,
  member_count,
  name,
  updated_at,
    MAX(_sdc_batched_at) over (
      PARTITION BY segment_id
      ORDER BY
        _sdc_batched_at RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS max_sdc_batched_at
  FROM
    segments
),
latest_version AS (
  SELECT
    *
  FROM
    select_segments
  WHERE
    _sdc_batched_at = max_sdc_batched_at
)
SELECT
  *
FROM
  latest_version
