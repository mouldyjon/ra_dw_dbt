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

    select *,
    timestamp_diff(current_timestamp,d.start_date_ts,DAY) as days_until_end,
    timestamp(date_add(date(d.start_date_ts), interval safe_cast(d.duration_days as int64) day)) as end_date_ts,
    case when (s.stage_label in ('Closed Won and Scheduled','Verbally Won and Working at Risk')
    and timestamp_diff(current_timestamp,start_date_ts,DAY) < 365/2)
    or
      (s.stage_label in ('Closed Won and Scheduled','Verbally Won and Working at Risk')
       and
       d.start_date_ts < current_timestamp
       and date_add(date(d.start_date_ts), interval safe_cast(d.duration_days as int64) day) > current_date)
      then true else false end as is_active

     from deals d

    left join stages s on d.dealstage = s.stageid

    left join pipelines p on s.pipelineid = p.pipelineid

    left outer join {{ ref('owners') }} u

    on safe_cast(d.hubspot_owner_id as int64) = u.ownerid

),

scd_deals as (

    select

-- SCD2 "historical values" attributes

    *,

-- SCD3 "previous value" attributes


      lag(dealstage_ts) over (partition by deal_id order by dbt_updated_at) previous_dealstage_ts,
      lag(stage_displayorder) over (partition by deal_id order by dbt_updated_at) previous_stage_displayorder,
      lag(stage_label) over (partition by deal_id order by dbt_updated_at) previous_stage_label,


-- SCD1 "current" value attributes

      last_value(dealstage_ts) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_dealstage_ts,
      last_value(dealname) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_dealname,
      last_value(createdate) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_createdate,
      last_value(hubspot_owner_id) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_hubspot_owner_id,
      last_value(hubspot_owner_email) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_hubspot_owner_email,
      last_value(dealstage_id) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_dealstage_id,
      last_value(amount) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_amount,
      last_value(probability) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_probability,
      last_value(closedate) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_closedate,
      last_value(stage_displayorder) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_stage_displayorder,
      last_value(salesperson_full_name) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_salesperson_full_name,
      last_value(salesperson_email) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_salesperson_email,
      last_value(stage_label) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_stage_label,
      last_value(is_active) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_is_active,
      last_value(start_date_ts) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_start_date_ts,
      last_value(duration_days) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_duration_days,
      last_value(end_date_ts) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) current_end_date_ts,






-- and "original" value attributes

      first_value(createdate) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) original_createddate,
      first_value(amount) over (partition by deal_id order by dbt_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) original_amount

    from joining

),

date_difference as (

    select *,
      timestamp_diff(dealstage_ts,previous_dealstage_ts, day) as days_between_stage,
      timestamp_diff(current_timestamp(),original_createddate,day) as days_since_deal_created,
      current_amount - original_amount as amount_diff_since_deal_created

    from scd_deals

)

select * except (pipelineid,_sdc_batched_at) from date_difference
