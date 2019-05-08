{{
    config(
        materialized='table'
    )
}}
select * from {{ ref('segment_combined_pageviews')}}
