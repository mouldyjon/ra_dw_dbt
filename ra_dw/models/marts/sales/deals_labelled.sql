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
    p.pipeline_active


    from deals d

    left join stages s on d.dealstage = s.stageid

    left join pipelines p on s.pipelineid = p.pipelineid

    left outer join {{ ref('owners') }} u

    on safe_cast(d.hubspot_owner_id as int64) = u.ownerid
