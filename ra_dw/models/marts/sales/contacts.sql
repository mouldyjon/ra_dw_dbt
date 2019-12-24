with contacts as (

  select * from {{ ref('hubspot_base_contacts') }}

),

select_contacts as (

    select

      vid as contactid,
      properties.lastname.value lastname,
      properties.city.value city,
      safe_cast(properties.associatedcompanyid.value as int64) associatedcompanyid,
      properties.email.value email,
      properties.lastmodifieddate.value lastmodifieddate,
      properties.createdate.value createdate,
      properties.phone.value phone,
      properties.lifecyclestage.value lifecyclestage,
      properties.jobtitle.value jobtitle,
      properties.firstname.value firstname,
      properties.hubspot_team_id.value hubspot_team_id,
      properties.company.value company,
      properties.hubspot_owner_id.value hubspot_owner_id,
      properties.recent_deal_amount.value recent_deal_amount,
      properties.num_associated_deals.value num_associated_deals,
      properties.website.value website,
      properties.address.value address,
      properties.state.value state,
      properties.zip.value zip,
      properties.mobilephone.value mobilephone,
      canonical_vid,
      profile_url,
      is_contact,
      _sdc_batched_at,
      max(_sdc_batched_at) over (partition by vid order by _sdc_batched_at range between unbounded preceding and unbounded following) as max_sdc_batched_at

    from contacts

),

latest_version  as (

    select * from select_contacts

    where _sdc_batched_at = max_sdc_batched_at

)

select * from latest_version
