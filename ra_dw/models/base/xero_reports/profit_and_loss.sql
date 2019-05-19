select * from {{ source('xero_reports', 'profit_and_loss') }}

