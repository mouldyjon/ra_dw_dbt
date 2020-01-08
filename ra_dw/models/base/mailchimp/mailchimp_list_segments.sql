select * from {{ source('stitch_mailchimp', 'list_segments') }}
