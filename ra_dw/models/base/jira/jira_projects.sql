select * from {{ source('jira', 'projects') }}
