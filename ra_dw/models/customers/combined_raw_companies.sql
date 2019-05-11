select coalesce(coalesce(hub.hubspot_company_name,hrv.harvest_customer_name),xer.xero_customer_name, vis.site_visitor_customer_name) as customer_name,  hub.*, hrv.*, xer.*, vis.*
from {{ ref('hubspot_companies') }} hub
full outer join {{ ref('harvest_customers') }} hrv
on lower(hub.hubspot_company_name) = lower(hrv.harvest_customer_name)
full outer join {{ ref('xero_companies') }} xer
on lower(hub.hubspot_company_name) = lower(xer.xero_customer_name)
full outer join {{ ref('site_visitor_companies') }} vis
on hub.hubspot_domain = vis.site_visitor_domain
