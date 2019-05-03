{{
    config(
        materialized='table'
    )
}}
with event_type_seq_final as (select customer_id, user_session_id as event_type_seq, event_type,  event_ts, is_new_session
   from (
   SELECT customer_id,
                     event_type,
                     last_event,
       event_ts,
       is_new_session,
       SUM(is_new_session) OVER (ORDER BY customer_id, event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND current row ) AS global_session_id,
       SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND current row) AS user_session_id
      FROM (
        SELECT *,
              CASE WHEN event_type != lag(event_type,1) OVER (PARTITION BY customer_id ORDER BY event_ts) 
                     OR last_event IS NULL 
                   THEN 1 ELSE 0 END AS is_new_session
         FROM (
              SELECT customer_id,
                     customer_name,
                     case when event_type like '%Email' then 'Presales' else event_type end as event_type,
                     event_ts,
                     LAG(case when event_type like '%Email' then 'Presales' else event_type end,1) OVER (PARTITION BY customer_id ORDER BY event_ts) AS last_event
                FROM {{ ref('customer_events') }}
                order by customer_id, event_ts, event_type
              ) last
              order by customer_id, event_ts, event_type
       ) final
       order by customer_id, is_new_session desc, event_ts )
              where is_new_session = 1
       group by 1,2,3,4,5
       order by customer_id, user_session_id, event_ts)
 select customer_id, max(event_type_1) event_type_1, max(event_type_2) event_type_2, max(event_type_3) event_type_3, max(event_type_4) event_type_4, max(event_type_5) event_type_5, max(event_type_6) event_type_6, max(event_type_7 ) event_type_7, max(event_type_8) event_type_8, max(event_type_9) event_type_9, max(event_type_10) event_type_10
 from (
 select customer_id, 
 case when event_type_seq = 1 then event_type end as event_type_1, 
 case when event_type_seq = 2 then event_type end as event_type_2, 
 case when event_type_seq = 3 then event_type end as event_type_3,
  case when event_type_seq = 4 then event_type end as event_type_4,
 case when event_type_seq = 5 then event_type end as event_type_5,
 case when event_type_seq = 6 then event_type end as event_type_6,
 case when event_type_seq = 7 then event_type end as event_type_7,
 case when event_type_seq = 8 then event_type end as event_type_8,
 case when event_type_seq = 9 then event_type end as event_type_9,
 case when event_type_seq = 10 then event_type end as event_type_10
 from event_type_seq_final)
  group by 1
