{{ config( materialized='table') }}
select t.*,
s.session_id
from `ra-development.company_website.tracks_view` t
join {{ ref('segment_web_sessions__stitched') }} s
on t.user_id = s.blended_user_id
and t.received_at between s.session_start_tstamp	and s.session_end_tstamp
