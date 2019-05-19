{{
    config(
        materialized='table'
    )
}}
select * from {{ ref('profit_and_loss') }}
