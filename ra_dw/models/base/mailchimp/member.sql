select * from {{ source('fivetran_mailchimp', 'member') }}
