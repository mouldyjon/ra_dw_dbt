{{
    config(
        materialized='table'
    )
}}
SELECT
  *
FROM (
  SELECT
    *,
    MAX(updated_at) OVER (PARTITION BY id ORDER BY updated_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_updated_at
  FROM
    {{ ref('harvest_base_projects') }}
  ORDER BY
    id,
    updated_at)
WHERE
  updated_at = latest_updated_at
