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
        {{ source('harvest', 'contacts') }}
    )
WHERE
    _sdc_batched_at = latest_sdc_batched_at
