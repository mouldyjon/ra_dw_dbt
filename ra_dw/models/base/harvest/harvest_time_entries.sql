{{
    config(
        materialized='table'
    )
}}
SELECT
    t.*,concat(concat(concat(u.first_name,' '),' '),u.last_name) as consultant_firstname_lastname
FROM (
    SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
    FROM
        {{ source('harvest', 'time_entries') }}
    ) t
JOIN {{ ref('harvest_users') }} u
    ON t.user_id = u.id
WHERE
    t._sdc_batched_at = t.latest_sdc_batched_at
{{ dbt_utils.group_by(n=30) }}
