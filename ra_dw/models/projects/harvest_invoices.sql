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
         MAX(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_sequence,
    CASE WHEN DATE_DIFF(DATE(due_date),DATE(paid_at),DAY) <=0 THEN true ELSE false END AS was_paid_ontime

    FROM
        {{ ref('harvest_base_invoices') }}
    )
WHERE
    _sdc_sequence = latest_sdc_sequence
