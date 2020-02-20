{{
    config(
        materialized='table'
    )
}}
WITH
  invoices AS (
  SELECT
    invoiceid AS invoice_id,
    lineitems.lineitemid as invoice_line_item_id,
    lineitems.itemcode as invoice_line_item_code,
    lineitems.accountcode as invoice_line_item_account_code,
    tracking.option as invoice_line_item_tracking_option,
    tracking.name as invoice_line_item_tracking_name,
    lineitems.quantity as invoice_line_item_quantity,
    lineitems.unitamount as invoice_line_item_unit_amount,
    lineitems.taxtype as invoice_line_item_tax_type,
    lineitems.description as invoice_line_item_description,
    lineitems.lineamount as invoice_line_item_line_amount,
    lineitems.taxamount as invoice_line_item_tax_amount,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY lineitems.lineitemid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at,

  FROM
    {{ source('xero', 'invoices') }} i,
     UNNEST(i.lineitems) AS lineitems,
     UNNEST(lineitems.tracking) as tracking)
SELECT
  * EXCEPT (_sdc_batched_at,
    max_sdc_batched_at)
FROM
  invoices
WHERE
  _sdc_batched_at = max_sdc_batched_at
