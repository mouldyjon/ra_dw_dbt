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
        MAX(t._sdc_sequence) OVER (PARTITION BY id ORDER BY t._sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_sequence
    FROM
        {{ ref('harvest_base_time_entries') }}
    ) t
JOIN {{ ref('harvest_users') }} u
    ON t.user_id = u.id
WHERE
    t._sdc_sequence = t.latest_sdc_sequence
