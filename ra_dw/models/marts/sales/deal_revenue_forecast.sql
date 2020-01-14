{{
    config(
        materialized='table'
    )
}}
-- calculate the daily weighted and unweighted revenue for each deal
-- requires:
-- current_dealname,
-- deal_id,
-- current_amount,
-- current_probability,
-- current_stage_label
-- current_start_date_ts
-- current_end_date_ts

WITH daily_weighted_revenue as (
  SELECT
    *,
    (current_amount * current_probability) / nullif(contract_days,0) AS contract_daily_weighted_revenue,
    current_amount / nullif(contract_days,0) AS contract_daily_full_revenue,
    (current_amount / nullif(contract_days,0)) - ((current_amount * current_probability) / nullif(contract_days,0)) AS contract_diff_daily_revenue
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
        -- if there's no start date for deal in Hubspot, and it's in an early deal stage, assume a start date of 4 weeks from now
        CASE WHEN (current_start_date_ts IS NULL OR current_start_date_ts < current_timestamp) and current_stage_label in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 29 DAY))
        -- if there's no start date for deal in Hubspot and it's a late deal stage, assume a start date of 2 weeks from now
             WHEN (current_start_date_ts IS NULL OR current_start_date_ts < current_timestamp) and current_stage_label not in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 15 DAY))
          ELSE current_start_date_ts END AS current_start_date_ts,
          -- if there's no end date for deal in Hubspot, and it's in an early deal stage, assume an end date that's either start date as above + duration or estimate duration based on price and £4k data analytics sprint type
        CASE WHEN current_end_date_ts IS NULL and current_stage_label in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 29 DAY)), INTERVAL (CAST(ROUND(current_amount/4000,0) AS int64)*14) DAY)
             WHEN current_end_date_ts IS NULL and current_stage_label not in ('Initial Enquiry','Meeting and Sales Qualified','Presentation Given & Sprints Scoped','Awaiting Proposal') THEN TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(DATE_TRUNC(current_date,WEEK), INTERVAL 29 DAY)), INTERVAL (CAST(ROUND(current_amount/4000,0) AS int64)*14) DAY)
          ELSE current_end_date_ts END AS current_end_date_ts,
          -- and if we've estimated the start and end dates, set the estimated_start_end_dates boolean to true
        CASE WHEN (current_start_date_ts IS NULL OR current_end_date_ts IS NULL or current_start_date_ts < current_timestamp) THEN TRUE
          ELSE FALSE END AS estimated_start_end_dates,
      FROM
        {{ ref('deals') }}
      WHERE
      -- we only want to forecast open deals, so exclude lost and already delivered deals
        current_stage_label not in ('Closed Lost','Closed Won and Delivered')
      GROUP BY
        1,2,3,4,5,6,7))
),
-- generate one row per date, for joining to the deals using a BETWEEN comparison
months as (
  SELECT *
  FROM UNNEST(GENERATE_DATE_ARRAY('2019-01-10', '2024-01-01', INTERVAL 1 DAY)) day_ts
)
-- calculate monthly revenue for all deals in-scope by aggregating daily contract revenue for those deals
SELECT deal_id,
       date_trunc(day_ts,MONTH) as month_ts,
       sum(revenue_days) as revenue_days,
       sum(daily_weighted_revenue) as weighted_amount_monthly_forecast,
       sum(daily_full_revenue) as full_amount_monthly_forecast,
       sum(daily_diff_revenue) as diff_amount_monthly_forecast
from (
  -- calculate daily revenue for all deals in-scope across all contract days
  SELECT deal_id,
       day_ts,count(*) as revenue_days,
       sum(contract_daily_weighted_revenue) daily_weighted_revenue,
       sum(contract_daily_full_revenue) daily_full_revenue,
       sum(contract_diff_daily_revenue) daily_diff_revenue
  FROM months m
  JOIN daily_weighted_revenue d
  -- creates one row per contract deal per deal
  ON TIMESTAMP(m.day_ts) between d.current_start_date_ts and timestamp_sub(d.current_end_date_ts, interval 1 day)
  GROUP BY 1,2)
GROUP BY  1,2
