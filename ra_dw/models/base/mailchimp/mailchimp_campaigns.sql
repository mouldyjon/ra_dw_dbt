select * from {{ source('stitch_mailchimp', 'campaigns') }}
