{{
    config(
        materialized='table'
    )
}}
-- currently only BigQuery-compatible, because of CONCAT
-- Making it cross-database requires building custom adapter
-- (https://docs.getdbt.com/docs/building-a-new-adapter)
SELECT
    ROW_NUMBER() OVER() AS customer_id,
    CONCAT('Customer ', CAST(ROW_NUMBER() OVER() AS string)) AS demo_company_name,
    CASE WHEN harvest_customer_id IS NOT null THEN true ELSE false END AS is_services_client,
    CASE WHEN hubspot_company_id IS NOT null THEN true ELSE false END AS is_crm_tracked_client,
    CASE WHEN xero_is_supplier IS true THEN true ELSE false END AS is_supplier_company,
    CASE WHEN site_visitor_category IS NOT null THEN true ELSE false END AS is_site_visitor,    
*
FROM
    (SELECT
        *
    FROM
        (SELECT
            customer_name,
            hubspot_company_id,
            xero_contact_id,
            harvest_customer_id,
            site_visitor_customer_id,
            harvest_address,
            xero_is_customer,
            xero_is_supplier,
            xero_customer_status,
            harvest_customer_created_at,
            harvest_customer_currency,
            harvest_customer_is_active,
            hubspot_first_deal_created_date,
            hubspot_twitterhandle,
            hubspot_country,
            hubspot_total_money_raised,
            hubspot_city,
            hubspot_total_revenue,
            hubspot_annual_revenue,
            hubspot_website,
            hubspot_owner_id,
            hubspot_industry,
            hubspot_linkedin_bio,
            hubspot_is_public,
            coalesce(hubspot_domain, site_visitor_domain) as web_domain,
            hubspot_created_date,
            hubspot_type,
            hubspot_state,
            hubspot_lifecycle_stage,
            hubspot_description,
            site_visitor_subcategory, site_visitor_category,
            ROW_NUMBER() OVER (PARTITION BY LOWER(customer_name)) AS c_r
        FROM
            {{ ref('combined_raw_companies') }}
        {{ dbt_utils.group_by(n=32) }}
        ORDER BY
            1)
    WHERE
        c_r = 1)
