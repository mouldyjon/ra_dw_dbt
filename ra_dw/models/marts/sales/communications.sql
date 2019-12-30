{{
    config(
        materialized='table'
    )
}}


WITH
  deduped_communications AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY engagement_id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
    FROM
      {{ ref('hubspot_engagements') }} )
  WHERE
    _sdc_batched_at = latest_sdc_batched_at),
  owners AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY ownerid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
    FROM
      {{ ref('hubspot_owners') }} )
  WHERE
    _sdc_batched_at = latest_sdc_batched_at)
SELECT
  engagement.id AS engagement_id,
  engagement_id AS communications_id,
  associations.companyids[OFFSET(off)] AS hubspot_company_id,
  associations.dealids AS sales_opportunity_id,
  engagement.timestamp AS communication_timestamp,
  engagement.ownerid AS ownerid,
  engagement.type AS communication_type,
  metadata.text AS communications_text,
  metadata.subject AS communications_subject,
  CONCAT(CONCAT(metadata.FROM.firstname,' '), metadata.FROM.lastname) AS communications_from_firstname_lastname,
  metadata.status AS communications_status,
  metadatato.email AS communications_to_email,
  concat(cast(engagement_id as string),coalesce(cast(associations.companyids[OFFSET(off)] as string),'')) as pk
FROM (
  SELECT
    deduped_communications.*
  FROM
    deduped_communications
  LEFT OUTER JOIN
    owners
  ON
    deduped_communications.engagement.ownerid = owners.ownerid) communications,
  UNNEST(communications.associations.companyids) WITH OFFSET off
LEFT JOIN
  communications.associations.dealids
LEFT JOIN
  communications.metadata.TO metadatato
