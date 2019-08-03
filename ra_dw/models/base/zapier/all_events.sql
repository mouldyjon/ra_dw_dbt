select * from {{ source('zapier_events_staging', 'events') }}
