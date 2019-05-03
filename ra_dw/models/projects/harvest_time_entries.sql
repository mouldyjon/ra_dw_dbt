{{
    config(
        materialized='incremental'
    )
}}

select * from (
SELECT *, max(_sdc_sequence) over (partition by id order by _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND unbounded following) as latest_sdc_sequence
FROM {{ ref('harvest_base_time_entries') }})
where _sdc_sequence = latest_sdc_sequence

{% if is_incremental() %}
and _sdc_sequence > (select max(_sdc_sequence) from {{ this }})
{% endif %}

