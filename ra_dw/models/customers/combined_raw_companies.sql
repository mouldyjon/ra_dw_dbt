
SELECT
    COALESCE(
      COALESCE(
        COALESCE(hub.hubspot_company_name,
              hrv.harvest_customer_name),
        xer.xero_customer_name),
      vis.site_visitor_customer_name) AS customer_name,
    hub.*,
    hrv.*,
    xer.*,
    vis.*
FROM
    {{ ref('hubspot_companies') }} hub
FULL OUTER JOIN
    {{ ref('harvest_customers') }} hrv
    ON LOWER(hub.hubspot_company_name) = LOWER(hrv.harvest_customer_name)
FULL OUTER JOIN
    {{ ref('xero_companies') }} xer
    ON LOWER(hub.hubspot_company_name) = LOWER(xer.xero_customer_name)
FULL OUTER JOIN 
    {{ ref('site_visitor_companies') }} vis
    ON LOWER(hub.hubspot_domain) = LOWER(vis.site_visitor_domain)

