{{
    config(
        materialized='view'
    )
}}
SELECT
  *
FROM (
  SELECT
    key,
    id,
    fields.created,
    fields.reporter.displayname,
    fields.reporter.active,
    fields.progress.progress as progress,
    fields.progress.total as total_progress,
    fields.aggregateprogress.progress as aggregate_progress,
    fields.timeestimate/3600 as estimated_hours,
    fields.timetracking.remainingestimateseconds/3600 as remaining_hours_to_complete,
    fields.aggregateprogress.total,
    fields.priority.name as priority_name,
    fields.issuetype.name issuetype_name,
    fields.status.description,
    fields.status.statuscategory.colorname as colourname,
    fields.status.statuscategory.name as statuscategory,
    fields.project.projecttypekey,
    fields.project.id as project_id,
    fields.project.name,
    fields.project.key as project_key,
    fields.parent.key as parent_key,
    fields.summary,
    fields.lastviewed,
    fields.updated,
    timestamp(substr(fields.statuscategorychangedate,1,23)) status_change_ts,
    _sdc_sequence,
    MAX(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_sequence
  FROM
    {{ ref('jira_issues') }})
WHERE
  _sdc_sequence = max_sdc_sequence
