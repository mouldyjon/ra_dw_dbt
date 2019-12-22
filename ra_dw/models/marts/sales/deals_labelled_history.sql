{{
    config(
        materialized='table'
    )
}}

with deals as (

    select * from {{ ref('deals_snapshot') }}

),

stages as (

    select * from {{ ref('pipeline_stages') }}

),

pipelines as (

    select * from {{ ref('pipelines') }}

),

joining as (

    select * from deals

    left join stages on deals.dealstage = stages.stageid

    left join pipelines on stages.pipelineid = pipelines.pipelineid

    left outer join {{ ref('owners') }} u

    on safe_cast(deals.hubspot_owner_id as int64) = u.ownerid

),

velocity as (

    select

    *,
      lag(dealstage_ts) over (
    partition by deal_id
    order by stage_displayorder) previous_stage

    from joining

),

date_difference as (

    select *,
      timestamp_diff(dealstage_ts,previous_stage, day) as days_diff
    from velocity

)

select * except (pipelineid,_sdc_batched_at) from date_difference
