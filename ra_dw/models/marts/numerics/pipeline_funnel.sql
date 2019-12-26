{{
    config(
        materialized='table'
    )
}}
SELECT stage_label, stage_displayorder, sum(amount) as amount FROM {{ ref('deals_labelled') }}
where stage_label not like ('%Closed Won and Delivered%')
and stage_label not like ('%Closed Lost%')
and stage_label not like ('%Closed Won with Contracts Signed%')
group by 1,2
order by 2
