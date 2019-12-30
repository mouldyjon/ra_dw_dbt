{{
    config(
        materialized='table'
    )
}}
SELECT i.*,
       c.customer_id,
       p.name as project_name,
       p.projecttypekey as project_type,
       p.description as project_description
       FROM {{ ref('jira_issues') }} i
join {{ ref('jira_projects') }} p
on i.project_id = p.id
left outer join {{ ref('customer_master') }} c
on i.project_grouping = c.customer_name
