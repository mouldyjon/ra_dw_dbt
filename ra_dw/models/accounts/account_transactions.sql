{{
    config(
        materialized='table'
    )
}}
select bankaccount.name bank_account_name, 
       bankaccount.accountid bank_account_accountid,        
       lineitem.itemcode,
       lineitem.quantity, 
       lineitem.unitamount, 
       lineitem.taxamount, 
       lineitem.lineamount,
       lineitem.accountcode,
       a.reportingcode as account_reportingcode, a.bankaccounttype as account_bankaccounttype, a.class as account_class, a.reportingcodename as account_reportingcodename, a.name as account_name, a.status as account_status, a.type as account_type, a.systemaccount as account_systemaccount, a.taxtype as account_taxtype, a.description as account_description,
       lineitem.description,
       lineitem.taxtype,
       total, 
       t.currencycode,
       totaltax, 
       banktransactionid, 
       t.status as transaction_status,
       reference, 
       t.type as transaction_type, 
       subtotal, 
       date as transaction_date, 
       isreconciled, 
       datestring, 
       lineamounttypes, 
       contact.name contact_name, 
       contact.contactid contactid,
       contact.contactnumber contactnumber,
       contact.contactid xero_company_id
from {{ ref('xero_bank_transactions') }} t
left join unnest (lineitems) as lineitem
left outer join {{ ref('xero_accounts') }} a
on lineitem.accountcode = a.code
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
