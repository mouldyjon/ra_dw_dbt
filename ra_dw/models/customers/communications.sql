{{
    config(
        materialized='table'
    )
}}
SELECT
  engagement_id communications_id,
  associations.companyids[OFFSET(off)] hubspot_company_id,
  associations.dealids sales_opportunity_id,
  engagement.timestamp communication_timestamp,
  engagement.ownerid communication_ownerid,
  concat(concat(o.firstname,' '),o.lastname) as owner_full_name,
  engagement.type communication_type,
  metadatato.email communications_to_email,
  metadata.text communications_text, 
  metadata.subject communications_subject, 
  concat(concat(metadata.from.firstname,' '),metadata.from.lastname) communications_from_firstname_lastname,
  metadata.status communications_status,
  metadatacc.email communications_cc_email
from `ra-development.stitch_hubspot.engagements`,
unnest(associations.companyids) WITH OFFSET off,
unnest(associations.dealids),
unnest(metadata.cc) metadatacc,
unnest(metadata.to) metadatato
join {{ ref('hubspot_owners') }} o
on engagement.ownerid = o.ownerid
