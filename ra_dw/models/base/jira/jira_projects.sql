select * from {{ source('stitch_jira', 'projects') }}
