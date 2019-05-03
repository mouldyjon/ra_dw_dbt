select * from {{ source('harvest', 'time_entries') }}
