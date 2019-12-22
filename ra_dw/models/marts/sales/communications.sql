{{
    config(
        materialized='table'
    )
}}


with engagements as (

    select * from {{ ref('hubspot_engagements') }}

),


owners as (

    select * from {{ ref('hubspot_owners') }}

),

communications_id_with_max as (

    select
      *,
      max(_sdc_batched_at) over (partition by engagement.id order by _sdc_batched_at range between unbounded preceding and unbounded following) as latest_sdc_batched_at

    from engagements

),

latest_version as (

    select * from communications_id_with_max
    where _sdc_batched_at = latest_sdc_batched_at

),

communications as (

    select

      engagement.id as engagement_id,
      engagement_id as communications_id,
      associations.companyids[offset(off)] as hubspot_company_id,
      associations.dealids as sales_opportunity_id,
      engagement.timestamp as communication_timestamp,
      engagement.ownerid as ownerid,
      engagement.type as communication_type,
      metadatato.email as communications_to_email,
      metadata.text as communications_text,
      metadata.subject as communications_subject,
      concat(concat(metadata.from.firstname,' '), metadata.from.lastname) as communications_from_firstname_lastname,
      metadata.status as communications_status,
      metadatacc.email as communications_cc_email

    from

      latest_version,
      unnest(associations.companyids) with offset off,
      unnest(associations.dealids),
      unnest(metadata.cc) metadatacc,
      unnest(metadata.to) metadatato

),


final as (

    select * from communications

    left outer join owners using (ownerid)

)

select * from final
