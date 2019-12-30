{{
    config(
        materialized='table'
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
    case when fields.status.statuscategory.name = 'Done' then 1 else 0 end as count_completed,
    case when fields.status.statuscategory.name = 'In Progress' then 1 else 0 end as count_in_progress,
    case when fields.status.statuscategory.name = 'To Do' then 1 else 0 end as count_unassigned,
    case when fields.issuetype.name = 'Task' then 1 else 0 end as count_tasks,
    case when fields.issuetype.name = 'Sub-task' then 1 else 0 end as count_subtasks,
    case when fields.issuetype.name = 'Bug' then 1 else 0 end as count_bugs,
    case when fields.issuetype.name = 'Support' then 1 else 0 end as count_support_tickets,
    case when fields.project.projecttypekey = 'software' then 'Jira' else 'Jira Service Desk' end as service_category,
    case when fields.project.name like '%Florence%' then 'Florence'
         when fields.project.name like '%Colourpop%' then 'Colourpop'
         when upper(fields.project.name) like '%INTO%' then 'INTO University Partnerships'
         when fields.project.name like '%Funda%' then 'funda'
         when fields.project.name like '%Resolver%' then 'Resolving Group Ltd'
         when fields.project.name like 'LiveBetterWith' then 'Live Better With Ltd'
         else fields.project.name end as project_grouping,
    case when fields.status.statuscategory.name = 'Done' then true else false end as issue_completed,
    case when fields.status.statuscategory.name = 'Done' then timestamp_diff(timestamp(substr(fields.statuscategorychangedate,1,23)),fields.created,HOUR) end as issue_hours_to_complete,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    {{ source('jira', 'issues') }}
  )
WHERE
  _sdc_batched_at = max_sdc_batched_at
