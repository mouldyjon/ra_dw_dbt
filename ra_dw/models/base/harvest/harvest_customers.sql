
{{
    config(
        materialized='table'
    )
}}
  SELECT
      *
  FROM (
      SELECT
      REPLACE(
        {{ remove_substrings('name', [', Ltd.', ', Inc', ' LLC', ' Cosmetics', ' Ltd.', ' Inc', ' Digital Limited', ' Group', ' Consulting']) }},
        'Resolving UK Limited',
        'Resolving Group Ltd'
      ) AS harvest_customer_name,
      id AS harvest_customer_id,
      address AS harvest_address,
      updated_at AS harvest_customer_updated_at,
      created_at AS harvest_customer_created_at,
      currency AS harvest_customer_currency,
      is_active AS harvest_customer_is_active,
      _sdc_batched_at,
           MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at

      FROM
          {{ source('harvest', 'clients') }}
      )
  WHERE
      _sdc_batched_at = latest_sdc_batched_at
