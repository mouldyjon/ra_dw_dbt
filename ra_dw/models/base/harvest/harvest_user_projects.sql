select * from {{ source('harvest', 'user_projects') }}