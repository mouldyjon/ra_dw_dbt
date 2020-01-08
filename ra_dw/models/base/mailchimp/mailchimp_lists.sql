select * from {{ source('stitch_mailchimp', 'lists') }}
