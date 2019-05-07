{{
    config(
        materialized='table'
    )
}}
WITH assignments AS (
    SELECT
        *
    FROM (
        SELECT
            harvest_user_id,
            a.id,
            allocation,
            project_id,
            start_date,
            end_date,
            {{ dbt_utils.datediff(start_date, end_date, 'day')}} +1 AS forecast_days,
            a._sdc_sequence ,
            MAX(a._sdc_sequence) OVER (PARTITION BY a.id ORDER BY a._sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_sequence
        FROM
            {{ ref('harvest_forecast_assignments') }} a
        INNER JOIN
            {{ ref('harvest_forecast_people') }} p
            ON person_id = p.id
        )
    WHERE
        _sdc_sequence = latest_sdc_sequence
      ),
projects AS (
    SELECT
        *
    FROM (
        SELECT
            harvest_id,
            id,
            client_id,
            _sdc_sequence,
            MAX(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_sequence
        FROM
            {{ ref('harvest_forecast_projects') }}
          )
    WHERE
        _sdc_sequence = latest_sdc_sequence
      ),
clients AS (
    SELECT
        *
    FROM (
        SELECT
            harvest_id,
            id,
            _sdc_sequence,
            MAX(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_sequence
        FROM
            {{ ref('harvest_forecast_clients') }}
          )
    WHERE
        _sdc_sequence = latest_sdc_sequence
        ),
billable_rate AS (
    SELECT
        user_id,
        project_id,
        billable_rate
    FROM
        {{ ref('harvest_time_entries') }}
    WHERE
        billable IS TRUE
    {{ dbt_utils.group_by(n=3) }}
    )
SELECT
    assignments.*,
    billable_rate.billable_rate,
    (billable_rate.billable_rate*8)*forecast_days AS forecast_revenue,
    projects.harvest_id AS project_harvest_id,
    clients.harvest_id AS client_harvest_id
FROM
    assignments
INNER JOIN
    projects
    ON assignments.project_id = projects.id
INNER JOIN
    clients
    ON projects.client_id = clients.id
INNER JOIN
    billable_rate
    ON billable_rate.project_id = projects.harvest_id
    AND billable_rate.user_id = assignments.harvest_user_id
{{ dbt_utils.group_by(n=13) }}
