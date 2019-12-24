/*
  This snapshot table will live in:
    analytics.snapshots.orders_snapshot
*/

{% snapshot deals_snapshot %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='deal_id',
          updated_at='_sdc_batched_at',
        )
    }}

with deals as (

        select * from {{ source('hubspot', 'deals') }}

),

deal_stage_with_max as (

       select
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY dealid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_sdc_batched_at

       from deals

),

latest_version as (

        select * from deal_stage_with_max
        where _sdc_batched_at = latest_sdc_batched_at

),

new_deal as (

    select
      properties.hs_sales_email_last_replied.value AS sales_email_last_replied,
      properties.closed_lost_reason.value AS closed_lost_reason,
      properties.dealname.value AS dealname,
      properties.hubspot_owner_id.value AS hubspot_owner_id,
      properties.hubspot_owner_id.sourceid as hubspot_owner_email,
      properties.hs_lastmodifieddate.value AS lastmodifieddate,
      properties.notes_last_updated.value AS notes_last_updated,
      properties.dealstage.value AS dealstage,
      properties.dealstage.value as dealstage_id,
      properties.dealstage.timestamp as dealstage_ts,
      properties.pipeline.value AS pipeline,
      properties.closedate.value AS closedate,
      properties.createdate.value AS createdate,
      properties.amount_in_home_currency.value AS amount,
      properties.notes_last_contacted.value AS notes_last_contacted,
      properties.amount_in_home_currency.value AS amount_in_home_currency,
      properties.hubspot_owner_assigneddate.value AS hubspot_owner_assigneddate,
      properties.num_notes.value AS num_notes,
      properties.description.value AS description,
      properties.dealstage.source as source, -- added 06/11/1
      properties.dealstage.sourceid as salesperson_email,
      associations.associatedcompanyids as associatedcompanyids, -- added 06/11/2019
      dealid AS deal_id,
      _sdc_batched_at

    from latest_version

)

select * from new_deal

{% endsnapshot %}
