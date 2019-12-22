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
         MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at,
    CASE WHEN DATE_DIFF(DATE(due_date),DATE(paid_at),DAY) <=0 THEN true ELSE false END AS was_paid_ontime

    FROM
        {{ source('harvest', 'invoices') }}
    )
WHERE
    _sdc_batched_at = latest_sdc_batched_at
