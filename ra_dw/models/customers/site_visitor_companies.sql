SELECT
  network site_visitor_customer_name,
  network site_visitor_customer_id,
  visitor_subcategory site_visitor_subcategory,
  visitor_category site_visitor_category,
  domain site_visitor_domain
from {{ ref('pageviews') }}
group by 1,2,3,4,5
