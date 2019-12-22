{{
    config(
        materialized='table'
    )
}}

with deals as (

    select * from {{ ref('deals') }}

),

stages as (

    select * from {{ ref('pipeline_stages') }}

),

pipelines as (

    select * from {{ ref('pipelines') }}

)

    select d.*,
    s.*,
    p.pipeline_label,
    p.pipeline_displayorder,
    p.pipeline_active,
    timestamp_diff(current_timestamp,start_date_ts,DAY) as days_until_end,
    timestamp(date_add(date(d.start_date_ts), interval safe_cast(d.duration_months as int64) month)) as end_date_ts,
    case when (s.stage_label in ('Approved to Start (At Risk)','Won (Contract Signed)')
    and timestamp_diff(current_timestamp,start_date_ts,DAY) < 365/2)
    or
      (s.stage_label in ('Approved to Start (At Risk)','Won (Contract Signed)')
       and
       d.start_date_ts < current_timestamp
       and date_add(date(d.start_date_ts), interval safe_cast(d.duration_months as int64) month) > current_date)
      then true else false end as is_active


    from deals d

    left join stages s on d.dealstage = s.stageid

    left join pipelines p on s.pipelineid = p.pipelineid

    left outer join {{ ref('owners') }} u

    on safe_cast(d.hubspot_owner_id as int64) = u.ownerid
