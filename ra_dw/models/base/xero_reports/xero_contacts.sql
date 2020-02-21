{{
    config(
        materialized='table'
    )
}}
WITH
  contacts AS (
  SELECT
    contactid as contact_id,
    name as contact_name,
    emailaddress as contact_email,
    issupplier as contact_is_supplier,
    accountspayabletaxtype as contact_accounts_payable_tax_type,
    accountsreceivabletaxtype as contact_account_receiveable_tax_type,
    taxnumber as contact_tax_number,
    iscustomer as contact_is_customer,
    defaultcurrency as contact_default_currency,
    contactstatus as contact_status,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY contactid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at,
  FROM
    {{ source('xero', 'contacts') }} )
SELECT
  * EXCEPT (_sdc_batched_at,
    max_sdc_batched_at)
FROM
  contacts
WHERE
  _sdc_batched_at = max_sdc_batched_at
