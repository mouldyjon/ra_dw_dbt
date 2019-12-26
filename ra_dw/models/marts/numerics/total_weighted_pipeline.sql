{{
    config(
        materialized='table'
    )
}}
SELECT sum(amount*probability) as weighted_pipeline 
FROM {{ ref('deals_labelled')}}
where stage_label not like ('%Closed Won and Delivered%')
and stage_label not like ('%Closed Lost%')
and stage_label not like ('%Closed Won with Contracts Signed%')
