{{
    config(
        materialized='table',
        schema='numerics'
    )
}}
WITH pageviews_sessionized AS (SELECT * FROM {{ ref('pages_sessionized') }}
      )
SELECT
    pageviews_sessionized.session_visitor_type AS pageviews_sessionized_session_visitor_type,
    COUNT(DISTINCT ( pageviews_sessionized.anonymous_id)) AS pageviews_sessionized_count_visitors
FROM pageviews_sessionized
WHERE ((( ( pageviews_sessionized.session_start_ts) ) >= ((TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)))) AND ( ( pageviews_sessionized.session_start_ts) ) < ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)), INTERVAL (1 * 7) DAY)))))
GROUP BY
    1
