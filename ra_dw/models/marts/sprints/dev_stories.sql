{{
    config(
        materialized='table'
    )
}}
SELECT
  *
FROM
    {{ ref('dev_issues') }}
WHERE
  issuetype_name = 'Story'
