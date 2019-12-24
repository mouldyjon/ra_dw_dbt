{{
    config(
        materialized='table'
    )
}}
SELECT
  *
FROM
    {{ ref('jira_issues') }}
WHERE
  issuetype_name = 'Epic'
