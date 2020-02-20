{{
    config(
        materialized='table'
    )
}}
with accounts as
(
SELECT
  accountid as account_id,
  name as account_name,
  code as account_code,
  type as account_type,
  class as account_class,
  status as account_status,
  description as account_description,
  reportingcode as account_reporting_code,
  reportingcodename as account_reporting_code_name,
  currencycode as account_currency_code,
  bankaccounttype as account_bank_account_type,
  bankaccountnumber as account_bank_account_number,
  systemaccount as account_is_system_account,
  taxtype as account_tax_type,
  showinexpenseclaims as account_show_in_expense_claims,
  enablepaymentstoaccount as account_enable_payments_to_account,
  _sdc_batched_at,
  max(_sdc_batched_at) over (partition by accountid order by _sdc_batched_at range between unbounded preceding and unbounded following) as max_sdc_batched_at
from {{ source('xero', 'accounts') }}
)
select * except (_sdc_batched_at, max_sdc_batched_at)
from accounts
where _sdc_batched_at = max_sdc_batched_at
