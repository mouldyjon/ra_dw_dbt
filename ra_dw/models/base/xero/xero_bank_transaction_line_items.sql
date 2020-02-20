{{
    config(
        materialized='table'
    )
}}
WITH
  bank_transactions AS (
  SELECT
    banktransactionid AS bank_transaction_id,
    contact.contactid as bank_transaction_contact_id,
    bankaccount.name as bank_account_name,
    bankaccount.accountid as bank_account_id,
    currencycode as bank_transaction_currency_code,
    status as bank_transaction_status,
    reference as bank_transaction_reference,
    type as bank_transaction_type,
    date as bank_transaction_date,
    isreconciled as bank_transaction_is_reconciled,
    lineitems.lineitemid as bank_transaction_item_id,
    lineitems.accountcode as bank_transaction_line_item_account_code,
    tracking.option as bank_transactions_line_item_tracking_option,
    tracking.name as bank_transactions_line_item_tracking_name,
    lineitems.quantity as bank_transaction_line_item_quantity,
    lineitems.unitamount as bank_transaction_line_item_unit_amount,
    lineitems.taxtype as bank_transaction_line_item_tax_type,
    lineitems.description as bank_transaction_line_item_description,
    lineitems.lineamount as bank_transaction_line_item_line_amount,
    lineitems.taxamount as bank_transaction_line_item_tax_amount,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY lineitems.lineitemid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at,

  FROM
    {{ source('xero', 'bank_transactions') }} b,
     UNNEST(b.lineitems) AS lineitems,
     UNNEST(lineitems.tracking) as tracking )
SELECT
  * EXCEPT (_sdc_batched_at,
    max_sdc_batched_at)
FROM
  bank_transactions
WHERE
  _sdc_batched_at = max_sdc_batched_at
