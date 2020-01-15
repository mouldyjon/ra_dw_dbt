/*
  This snapshot table will live in:
    analytics.snapshots.deals_snapshot
*/

{% snapshot deals_snapshot %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='deal_id',
          check_cols='all'
        )
    }}

    with deals as (

            select *  from (
              select *,
              MAX(_sdc_batched_at) OVER (PARTITION BY dealid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) latest_sdc_batched_at
               from {{ source('hubspot', 'deals') }})
                where latest_sdc_batched_at = _sdc_batched_at


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
          properties.dealstage.sourceid as salesperson_email, -- added 06/11/2019
          case when properties.closedate.value is not null then true else false end AS is_closed, -- added 18/12/2019
          properties.pricing_model.value AS pricing_model, -- added 18/12/2019
          properties.source.value as deal_source,
          properties.products_in_solution.value as products_in_solution,
          properties.sprint_type.value as sprint_type,
          properties.days_to_close.value as days_to_close,
          properties.partner_referral.value as partner_referral_type,
          properties.deal_components.value as deal_components,
          properties.dealtype.value as deal_type,
          properties.assigned_consultant.value as assigned_consultant,
          case when timestamp_millis(safe_cast(properties.delivery_schedule_date.value as int64)) > timestamp_millis(safe_cast(properties.delivery_start_date.value as int64))  then timestamp_millis(safe_cast(properties.delivery_schedule_date.value as int64))
            when timestamp_millis(safe_cast(properties.delivery_start_date.value as int64)) >= timestamp_millis(safe_cast(properties.delivery_schedule_date.value as int64)) then timestamp_millis(safe_cast(properties.delivery_start_date.value as int64))
            when timestamp_millis(safe_cast(properties.delivery_start_date.value as int64)) is null then timestamp_millis(safe_cast(properties.delivery_schedule_date.value as int64))
            when timestamp_millis(safe_cast(properties.delivery_schedule_date.value as int64)) is null then timestamp_millis(safe_cast(properties.delivery_start_date.value as int64))
            end as start_date_ts,
          case when properties.number_of_sprints.value is not null then properties.number_of_sprints.value * 14
              when properties.number_of_sprints.value is null and properties.sprint_type.value like '%Data Analytics%' then (properties.amount_in_home_currency.value / 4000) * 14
              when properties.number_of_sprints.value is null and properties.sprint_type.value like '%Data Engineering%' then (properties.amount_in_home_currency.value / 6000) * 14 end as duration_days,
          case when properties.deal_components.value like '%Services%' then 1 else 0 end as count_services_deal_component,
          case when properties.deal_components.value like '%Training%' then 1 else 0 end as count_support_deal_component,
          case when properties.deal_components.value like '%License Referral%' then 1 else 0 end as count_license_referral_deal_component,
          case when properties.deal_components.value like '%Managed Services%' then 1 else 0 end as count_managed_services_deal_component,
          case when properties.sprint_type.value like '%Data Analytics%' then 1 else 0 end as count_data_analytics_sprint_type,
          case when properties.sprint_type.value like '%Data Engineering%' then 1 else 0 end as count_data_engineering_sprint_type,
          case when properties.sprint_type.value like '%Data Science%' then 1 else 0 end as count_data_science_sprint_type,
          case when properties.sprint_type.value like '%Data Strategy%' then 1 else 0 end as count_data_strategy_sprint_type,
          case when properties.sprint_type.value like '%Data Integration%' then 1 else 0 end as count_data_integration_sprint_type,
          case when properties.sprint_type.value like '%Data Collection%' then 1 else 0 end as count_data_collection_sprint_type,
          case when properties.products_in_solution.value like '%Looker%' then 1 else 0 end as count_looker_product_in_solution,
          case when properties.products_in_solution.value like '%dbt%' then 1 else 0 end as count_dbt_product_in_solution,
          case when properties.products_in_solution.value like '%Stitch%' then 1 else 0 end as count_stitch_product_in_solution,
          case when properties.products_in_solution.value like '%Segment%' then 1 else 0 end as count_segment_product_in_solution,
          case when properties.products_in_solution.value like '%GCP%' then 1 else 0 end as count_gcp_product_in_solution,
          case when properties.products_in_solution.value like '%Snowflake%' then 1 else 0 end as count_snowflake_product_in_solution,
          case when properties.products_in_solution.value like '%Fivetran%' then 1 else 0 end as count_fivetran_product_in_solution,
          case when properties.products_in_solution.value like '%Qubit%' then 1 else 0 end as count_qubit_product_in_solution,
          case when properties.source.value like '%Partner Referral%' then 1 else 0 end as count_partner_referral_source,
          case when properties.source.value like '%CEO Network%' then 1 else 0 end as count_ceo_network_source,
          case when properties.source.value like '%Staff Referral%' then 1 else 0 end as count_staff_referral_referral_source,
          case when properties.source.value like '%Organic Search%' then 1 else 0 end as count_organic_search_source,
          case when properties.source.value like '%Repeat Customer%' then 1 else 0 end as count_repeat_customer_source,
          case when properties.source.value like '%Paid Search/Campaign%' then 1 else 0 end as count_paid_search_source,
          associations.associatedcompanyids[offset(off)] as associatedcompanyids, -- added 18/12/2019
          dealid AS deal_id,
          _sdc_batched_at

        from deals,
                  unnest(associations.associatedcompanyids) with offset off

    )

    select * from new_deal


{% endsnapshot %}
