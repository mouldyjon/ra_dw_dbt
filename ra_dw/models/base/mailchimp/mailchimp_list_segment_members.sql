select * from {{ source('stitch_mailchimp', 'list_segment_members') }}
