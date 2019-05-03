select contactid as xero_contact_id, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(name,', Ltd.',''),', Inc','')
  ,' LLC',''),' Cosmetics',''),' Ltd.',''),' Inc',''),' Digital Limited',''),' Group',''),' Consulting',''),'BHCC','Brighton & Hove City Council') xero_customer_name, iscustomer xero_is_customer, issupplier xero_is_supplier, defaultcurrency xero_customer_default_currency, contactstatus xero_customer_status, contactnumber xero_customer_contact_number 
from {{ ref('xero_base_contacts') }}
where firstname is null
group by 1,2,3,4,5,6,7
