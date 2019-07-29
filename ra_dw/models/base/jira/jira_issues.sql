select * from {{ source('jira', 'issues') }}
