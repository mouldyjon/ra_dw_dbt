{{ config(materialized='table') }}
select row_number() over () as customer_id, 
concat('Customer ',cast(row_number() over () as string)) as demo_company_name,
case when harvest_customer_id is not null then true else false end as is_services_client, 
                                            case when hubspot_company_id is not null then true else false end as is_crm_tracked_client,
                                            case when xero_is_supplier is true then true else false end as is_supplier_company,
                                            case when site_visitor_category is not null then true else false end as is_site_visitor,
* from (select * from (
SELECT customer_name, row_number() over (partition by lower(customer_name)) as c_r,
hubspot_company_id, xero_contact_id, harvest_customer_id, site_visitor_customer_id,harvest_address, xero_is_customer, xero_is_supplier, xero_customer_status, harvest_customer_created_at, harvest_customer_currency, harvest_customer_is_active, hubspot_first_deal_created_date, hubspot_twitterhandle, hubspot_country, hubspot_total_money_raised, hubspot_city, hubspot_total_revenue, hubspot_annual_revenue, hubspot_website, hubspot_owner_id, hubspot_industry, hubspot_linkedin_bio, hubspot_is_public, coalesce(hubspot_domain, site_visitor_domain) as web_domain, hubspot_created_date, hubspot_type, hubspot_state, hubspot_lifecycle_stage, hubspot_description,site_visitor_subcategory, site_visitor_category FROM {{ ref('combined_raw_companies') }} 
group by 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
order by 1)
where c_r = 1)
