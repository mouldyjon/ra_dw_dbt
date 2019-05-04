{{ config(materialized='table') }}
select * from (
SELECT
  associations.associatedcompanyids[OFFSET(off)]  associatedcompanyids,
  properties.hs_sales_email_last_replied.value sales_email_last_replied,
  properties.closed_lost_reason.value closed_lost_reason,
  properties.dealname.value dealname,
  properties.hubspot_owner_id.value hubspot_owner_id,
  properties.hs_lastmodifieddate.value lastmodifieddate,
  properties.notes_last_updated.value notes_last_updated,
  case when properties.dealstage.value = '7a1f6388-75b5-479c-99cb-da3479c12629' then 'Sales Opportunity Identified'
       when properties.dealstage.value = '553a886b-24bc-4ec4-bca3-b1b7fcd9e1c7' then 'Sales Closed Subject to Contract'
       when properties.dealstage.value = '7c41062e-06c6-4a4a-87eb-de503061b23c' then 'Sales Closed Won and Delivered'
       when properties.dealstage.value = 'presentationscheduled' then 'Sales Presentation Scheduled'
       when properties.dealstage.value = 'appointmentscheduled' then 'Sales Appointment Scheduled'
       when properties.dealstage.value = 'qualifiedtobuy' then 'Sales Qualified to Buy'
       when properties.dealstage.value = 'contractsent' then 'Sales Contract Sent'
       when properties.dealstage.value = 'closedwon' then 'Sales Closed Won'
       when properties.dealstage.value = 'closedlost' then 'Sales Closed Lost'
       else properties.dealstage.value end as dealstage,
 case when properties.dealstage.value = '7a1f6388-75b5-479c-99cb-da3479c12629' then 1
       when properties.dealstage.value = '553a886b-24bc-4ec4-bca3-b1b7fcd9e1c7' then 6
       when properties.dealstage.value = '7c41062e-06c6-4a4a-87eb-de503061b23c' then 9
       when properties.dealstage.value = 'presentationscheduled' then 4
       when properties.dealstage.value = 'appointmentscheduled' then 3
       when properties.dealstage.value = 'qualifiedtobuy' then 2
       when properties.dealstage.value = 'contractsent' then 5
       when properties.dealstage.value = 'closedwon' then 8
       when properties.dealstage.value = 'closedlost' then 7 end as dealstage_sortindex,
case when properties.dealstage.value = '7a1f6388-75b5-479c-99cb-da3479c12629' then 10
       when properties.dealstage.value = '553a886b-24bc-4ec4-bca3-b1b7fcd9e1c7' then 60
       when properties.dealstage.value = '7c41062e-06c6-4a4a-87eb-de503061b23c' then 100
       when properties.dealstage.value = 'presentationscheduled' then 40
       when properties.dealstage.value = 'appointmentscheduled' then 30
       when properties.dealstage.value = 'qualifiedtobuy' then 20
       when properties.dealstage.value = 'contractsent' then 50
       when properties.dealstage.value = 'closedwon' then 80
       when properties.dealstage.value = 'closedlost' then 0 end as dealstage_pipeline_modifier,
  properties.pipeline.value pipeline,
  properties.dealtype.value dealtype,
  properties.closedate.value closedate,
  properties.createdate.value createdate,
  properties.amount.value amount,
  properties.notes_last_contacted.value notes_last_contacted,
  properties.amount_in_home_currency.value amount_in_home_currency,
  properties.hubspot_owner_assigneddate.value hubspot_owner_assigneddate,
  properties.num_notes.value num_notes,
  properties.description.value description,
  dealid deal_id,
  _sdc_sequence,
  max(_sdc_sequence) over (partition by dealid order by _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND unbounded following) as latest_sdc_sequence
FROM
  {{ ref('hubspot_deals') }},
  unnest(associations.associatedcompanyids) WITH OFFSET off)
where _sdc_sequence = latest_sdc_sequence 
