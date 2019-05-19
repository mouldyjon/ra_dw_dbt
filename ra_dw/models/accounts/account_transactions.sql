{{
    config(
        materialized='table'
    )
}}
SELECT
    bankaccount.name AS bank_account_name,
    bankaccount.accountid AS bank_account_accountid,
    lineitem.itemcode,
    lineitem.quantity,
    lineitem.unitamount,
    lineitem.taxamount,
    lineitem.lineamount,
    lineitem.accountcode,
    a.reportingcode AS account_reportingcode,
    a.bankaccounttype AS account_bankaccounttype,
    a.class AS account_class,
    a.reportingcodename AS account_reportingcodename,
    a.name AS account_name,
    a.status AS account_status,
    a.type AS account_type,
    a.systemaccount AS account_systemaccount,
    a.taxtype AS account_taxtype,
    a.description AS account_description,
    lineitem.description,
    lineitem.taxtype,
    total,
    t.currencycode,
    totaltax,
    banktransactionid,
    t.status AS transaction_status,
    reference,
    t.type AS transaction_type,
    subtotal,
    date AS transaction_date,
    isreconciled,
    datestring,
    lineamounttypes,
    contact.name AS contact_name,
    contact.contactid AS contactid,
    contact.contactnumber AS contactnumber,
    contact.contactid AS xero_company_id
FROM
    {{ ref('xero_bank_transactions') }} t
LEFT JOIN
    UNNEST(lineitems) AS lineitem
LEFT OUTER JOIN
    {{ ref('xero_accounts') }} a
    ON lineitem.accountcode = a.code
{{ dbt_utils.group_by(n=36) }}
