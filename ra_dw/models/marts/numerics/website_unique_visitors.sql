{{
    config(
        materialized='table',
        schema='numerics'
    )
}}
SELECT FORMAT_TIMESTAMP('%Y/%W',timestamp_trunc(session_start_tstamp,WEEK)) as week, count(distinct blended_user_id) as weekly_unique_visitors
FROM {{ ref('segment_web_sessions__stitched') }}
where date(timestamp_trunc(session_start_tstamp,WEEK)) >= date_sub(current_date,interval 6 week)
and first_page_url_host = 'rittmananalytics.com'
group by 1
order by 1
