{{
    config(
        materialized='table'
    )
}}
WITH
  payments AS (
  SELECT
    paymentid as payment_id,
    account.accountid as payment_account_id,
    account.code as payment_code,
    invoice.contact.contactid as payment_contact_id,
    invoice.isdiscounted as payment_is_discounted,
    invoice.currencycode as payment_currency_code,
    invoice.invoicenumber as payment_invoice_number,
    invoice.invoiceid as payment_invoice_id,
    invoice.type as payment_invoice_type,
    status as payment_status,
    paymenttype as payment_type,
    reference as payment_reference,
    amount as payment_amount,
    date as payment_date,
    isreconciled as payment_is_reconciled,
    bankamount as payment_bank_amount,
    currencyrate as payment_currency_rate,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY paymentid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at,
  FROM
    {{ source('xero', 'payments') }})
SELECT
  * EXCEPT (_sdc_batched_at,
    max_sdc_batched_at)
FROM
  payments
WHERE
  _sdc_batched_at = max_sdc_batched_at
