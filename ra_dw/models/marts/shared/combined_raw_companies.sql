
SELECT
      COALESCE(
        COALESCE(hub.hubspot_company_name,
              hrv.harvest_customer_name),
        xer.contact_name) AS customer_name,
    hub.*,
    hrv.*,
    xer.*
FROM
    {{ ref('companies') }} hub
FULL OUTER JOIN
    {{ ref('harvest_customers') }} hrv
    ON LOWER(hub.hubspot_company_name) = LOWER(hrv.harvest_customer_name)
FULL OUTER JOIN
    {{ ref('xero_contacts') }} xer
    ON LOWER(hub.hubspot_company_name) = LOWER(xer.contact_name)
