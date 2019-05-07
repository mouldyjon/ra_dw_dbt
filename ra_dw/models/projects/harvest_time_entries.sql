{{
    config(
        materialized='incremental'
    )
}}

SELECT
    *
FROM (
    SELECT
        *,
        MAX(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_sequence
    FROM
        {{ ref('harvest_base_time_entries') }}
    )
WHERE
    _sdc_sequence = latest_sdc_sequence
{% if is_incremental() %}
    AND _sdc_sequence > (SELECT max(_sdc_sequence) FROM {{ this }})
{% endif %}
