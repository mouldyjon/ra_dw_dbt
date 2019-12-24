{{
    config(
        materialized='table'
    )
}}
SELECT
  *
FROM (
  SELECT
    id,
    lead.displayname,
    lead.active,
    name,
    projectkeys,
    projecttypekey,
    _sdc_sequence,
    MAX(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_sequence,
    description
  FROM
    {{ source('jira', 'projects') }})
WHERE
  _sdc_sequence = max_sdc_sequence
