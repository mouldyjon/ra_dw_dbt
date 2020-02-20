{{
    config(
        materialized='table'
    )
}}
WITH
  invoices AS (
  SELECT
    invoiceid as invoice_id,
    invoicenumber as invoice_number,
    reference as invoice_reference,
    plannedpaymentdate as invoice_planned_payment_date,
    status as invoice_status,
    type as invoice_type,
    haserrors as invoice_has_errors,
    duedatestring as invoice_due_date,
    url as invoice_url,
    senttocontact as invoice_sent_to_contact,
    isdiscounted as invoice_is_discounted,
    date as invoice_date,
    fullypaidondate as invoice_fully_paid_on_date,
    lineamounttypes as invoice_line_amount_types,
    total as invoice_total,
    totaltax as invoice_total_tax,
    amountpaid as invoice_amount_paid,
    amountdue as invoice_amount_due,
    currencycode as invoice_currency_code,
    subtotal as invoice_sub_total,
    amountcredited as invoice_amount_credited,
    currencyrate as invoice_currency_rate,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY invoiceid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    {{ source('xero', 'invoices') }})
SELECT
  * EXCEPT (_sdc_batched_at,
    max_sdc_batched_at)
FROM
  invoices
WHERE
  _sdc_batched_at = max_sdc_batched_at
