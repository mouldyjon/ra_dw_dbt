select * from {{ source('harvest', 'invoice_item_categories') }}