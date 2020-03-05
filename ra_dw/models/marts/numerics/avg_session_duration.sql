{{
    config(
        materialized='table',
        schema='numerics'
    )
}}
WITH this_week AS
      (
       SELECT (sum(duration_in_s))/count(distinct blended_user_id) as lw_avg_session_duration_s
       FROM {{ ref('segment_web_sessions__stitched') }}
       WHERE DATE(timestamp_trunc(session_start_tstamp,WEEK)) >= date_sub(current_date,interval 1 week)
       AND first_page_url_host = 'rittmananalytics.com'),
     prev_week as
      (
       SELECT (sum(duration_in_s))/count(distinct blended_user_id) as pw_avg_session_duration_s
       FROM {{ ref('segment_web_sessions__stitched') }}
       WHERE DATE(timestamp_trunc(session_start_tstamp,WEEK)) >= DATE_SUB(current_date,interval 2 week)
       AND DATE(timestamp_trunc(session_start_tstamp,WEEK)) < DATE_SUB(current_date,interval 1 week)
       AND first_page_url_host = 'rittmananalytics.com'
      )
SELECT ROUND(lw_avg_session_duration_s) as tw,
       ROUND(pw_avg_session_duration_s) as lw
FROM this_week, prev_week
