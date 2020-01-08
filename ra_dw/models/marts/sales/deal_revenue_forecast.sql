{{
    config(
        materialized='table'
    )
}}

WITH daily_weighted_revenue as (
  SELECT
    *,
    (current_amount * current_probability) / contract_days AS contract_daily_weighted_revenue,
    current_amount / contract_days AS contract_daily_full_revenue,
    (current_amount / contract_days) - ((current_amount * current_probability) / contract_days) AS contract_diff_daily_revenue
  FROM (
    SELECT
      *,
      TIMESTAMP_DIFF(current_end_date_ts,current_start_date_ts,DAY) AS contract_days
    FROM (
      SELECT
        current_dealname,
        deal_id,
        current_amount,
        current_probability,
        CASE WHEN (current_start_date_ts IS NULL OR current_start_date_ts < current_timestamp) and current_stage_label in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 29 DAY))
             WHEN (current_start_date_ts IS NULL OR current_start_date_ts < current_timestamp) and current_stage_label not in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 15 DAY))
          ELSE current_start_date_ts END AS current_start_date_ts,
        CASE WHEN current_end_date_ts IS NULL and current_stage_label in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 29 DAY)), INTERVAL (CAST(ROUND(current_amount/4000,0) AS int64)*14) DAY)
             WHEN current_end_date_ts IS NULL and current_stage_label not in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 29 DAY)), INTERVAL (CAST(ROUND(current_amount/4000,0) AS int64)*14) DAY)
          ELSE current_end_date_ts END AS current_end_date_ts,
        CASE WHEN (current_start_date_ts IS NULL OR current_end_date_ts IS NULL or current_start_date_ts < current_timestamp) THEN TRUE
          ELSE FALSE END AS estimated_start_end_dates,
      FROM
        {{ ref('deals_labelled_history') }}
      WHERE
        current_stage_label not in ('Closed Lost','Closed Won and Delivered')
      GROUP BY
        1,2,3,4,5,6,7))
),
months as (
  SELECT *
  FROM UNNEST(GENERATE_DATE_ARRAY('2019-01-10', '2024-01-01', INTERVAL 1 DAY)) day_ts
)
SELECT deal_id,
       date_trunc(day_ts,MONTH) as month_ts,
       sum(revenue_days) as revenue_days,
       sum(daily_weighted_revenue) as weighted_amount_monthly_forecast,
       sum(daily_full_revenue) as full_amount_monthly_forecast,
       sum(daily_diff_revenue) as diff_amount_monthly_forecast
from (
  SELECT deal_id,
       day_ts,count(*) as revenue_days,
       sum(contract_daily_weighted_revenue) daily_weighted_revenue,
       sum(contract_daily_full_revenue) daily_full_revenue,
       sum(contract_diff_daily_revenue) daily_diff_revenue
  FROM months m
  JOIN daily_weighted_revenue d
  ON TIMESTAMP(m.day_ts) between d.current_start_date_ts and timestamp_sub(d.current_end_date_ts, interval 1 day)
  GROUP BY 1,2)
GROUP BY  1,2