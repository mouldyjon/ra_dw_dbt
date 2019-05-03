SELECT
  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(name,', Ltd.',''),', Inc','')
  ,' LLC',''),' Cosmetics',''),' Ltd.',''),' Inc',''),' Digital Limited',''),' Group',''),' Consulting',''),'Resolving UK Limited','Resolving Group Ltd') harvest_customer_name,
  id harvest_customer_id,
  address harvest_address,
  updated_at harvest_customer_updated_at,
  created_at harvest_customer_created_at,
  currency harvest_customer_currency,
  is_active harvest_customer_is_active
from {{ ref('harvest_base_clients') }}
