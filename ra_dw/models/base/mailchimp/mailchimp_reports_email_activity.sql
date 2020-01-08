select * from {{ source('stitch_mailchimp', 'reports_email_activity') }}
