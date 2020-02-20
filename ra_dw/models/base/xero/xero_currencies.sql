{{
    config(
        materialized='table'
    )
}}
with currencies as
(
SELECT
  code currency_code,
  description as currency_name,
  _sdc_batched_at,
  max(_sdc_batched_at) over (partition by code order by _sdc_batched_at range between unbounded preceding and unbounded following) as max_sdc_batched_at
from {{ source('xero', 'currencies') }}
)
select * except (_sdc_batched_at, max_sdc_batched_at)
from currencies
where _sdc_batched_at = max_sdc_batched_at
