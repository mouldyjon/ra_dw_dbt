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
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
    FROM
        {{ source('harvest', 'time_entries') }}
    )
WHERE
    _sdc_batched_at = latest_sdc_batched_at
{{ dbt_utils.group_by(n=29) }}
