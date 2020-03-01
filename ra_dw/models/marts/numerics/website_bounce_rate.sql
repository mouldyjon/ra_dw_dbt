{{
    config(
        materialized='table'
    )
}}
WITH pageviews_sessionized AS (SELECT * FROM {{ ref('pages_sessionized') }}
      )
select week,
       round((bounces/sessions)*100) as bounce_rate
from(
SELECT
     FORMAT_TIMESTAMP('%Y/%W',timestamp_trunc(pageviews_sessionized.session_start_ts,WEEK)) as week,
     COUNT(*) AS sessions,
     COALESCE(SUM(pageviews_sessionized.bounced_sessions), 0) AS bounces
FROM pageviews_sessionized
WHERE ((( ( pageviews_sessionized.session_start_ts) ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)), INTERVAL (-5 * 7) DAY))) AND ( ( pageviews_sessionized.session_start_ts) ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(MONDAY)), INTERVAL (-5 * 7) DAY), INTERVAL (6 * 7) DAY)))))
GROUP BY
    1)
