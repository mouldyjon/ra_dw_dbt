{{
    config(
        materialized='table'
    )
}}


with companies as (

    select * from {{ ref('hubspot_base_companies') }}

),

companyid_with_max as (

    select
    
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY companyid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_sdc_batched_at

    from companies

),

latest_version as (

     select * from companyid_with_max
     where _sdc_batched_at = latest_sdc_batched_at

),

companies_tables as (

    select
      properties.founded_year.value AS hubspot_founded_year,
      properties.first_deal_created_date.value AS hubspot_first_deal_created_date,
      properties.hs_sales_email_last_replied.value AS hubspot_sales_email_last_replied,
      properties.linkedin_company_page.value AS hubspot_linkedin_company_page,
      properties.twitterhandle.value AS hubspot_twitterhandle,
      properties.numberofemployees.value AS hubspot_numberofemployees,
      properties.country.value AS hubspot_country,
      properties.total_money_raised.value AS hubspot_total_money_raised,
      properties.city.value AS hubspot_city,
      properties.hs_analytics_num_page_views.value AS hubspot_num_page_views,
      properties.annualrevenue.value AS hubspot_annual_revenue,
      properties.num_contacted_notes.value AS hubspot_num_contacted_notes,
      properties.web_technologies.value AS hubspot_web_technologies,
      properties.website.value AS hubspot_website,
      properties.hubspot_owner_id.value AS hubspot_owner_id,
      properties.facebook_company_page.value AS hubspot_facebook_company_page,
      properties.industry.value AS hubspot_industry,
      properties.address2.value AS hubspot_address2,
      properties.linkedinbio.value AS hubspot_linkedin_bio,
      properties.name.value AS hubspot_company_name,
      properties.is_public.value AS hubspot_is_public,
      properties.notes_last_updated.value AS hubspot_notes_last_updated,
      properties.num_associated_deals.value AS hubspot_num_associated_deals,
      properties.address.value AS hubspot_address,
      properties.domain.value AS hubspot_domain,
      properties.closedate.value AS hubspot_closedate,
      properties.first_contact_createdate.value AS hubspot_first_contact_createdate,
      properties.createdate.value AS hubspot_created_date,
      properties.phone.value AS hubspot_phone,
      properties.state.value AS hubspot_state,
      properties.lifecyclestage.value AS hubspot_lifecycle_stage,
      properties.notes_last_contacted.value AS hubspot_notes_last_contacted,
      properties.hubspot_owner_assigneddate.value AS hubspot_owner_assigned_date,
      properties.num_notes.value AS hubspot_num_notes,
      properties.recent_deal_close_date.value AS hubspot_recent_deal_close_date,
      properties.zip.value AS hubspot_zip,
      properties.description.value AS hubspot_description,
      properties.num_associated_contacts.value AS hubspot_num_associated_contacts,
      companyid AS hubspot_company_id

    from latest_version

)

select * from companies_tables
