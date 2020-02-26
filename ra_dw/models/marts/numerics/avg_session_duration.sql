{{
    config(
        materialized='table'
    )
}}
with this_week as (SELECT (sum(duration_in_s))/count(distinct blended_user_id) as lw_avg_session_duration_s FROM {{ ref('segment_web_sessions__stitched') }}
where date(timestamp_trunc(session_start_tstamp,WEEK)) >= date_sub(current_date,interval 1 week)
and first_page_url_host = 'rittmananalytics.com'),
     prev_week as (SELECT (sum(duration_in_s))/count(distinct blended_user_id) as pw_avg_session_duration_s FROM {{ ref('segment_web_sessions__stitched') }}
where date(timestamp_trunc(session_start_tstamp,WEEK)) >= date_sub(current_date,interval 2 week) and date(timestamp_trunc(session_start_tstamp,WEEK)) < date_sub(current_date,interval 1 week)
and first_page_url_host = 'rittmananalytics.com')
select  round(lw_avg_session_duration_s) as tw, round(pw_avg_session_duration_s) as lw
from this_week, prev_week
