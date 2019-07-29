select * from {{ source('stitch_jira', 'issues') }}
