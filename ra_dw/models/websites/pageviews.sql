{{
    config(
        materialized='table'
    )
}}
SELECT
  *,
  IFNULL(city, 'Other') AS city,
  IFNULL(countryLabel, 'Other') AS countryLabel,
  latitude,
  longitude
FROM (
  SELECT
    a.*,
    NET.IPV4_TO_INT64(NET.IP_FROM_STRING(ip)) AS clientIpNum,
    TRUNC(NET.IPV4_TO_INT64(NET.IP_FROM_STRING(ip))/(256*256)) AS classB
  FROM
    {{ ref('segment_combined_pageviews')}} ) AS a
LEFT OUTER JOIN
  {{ ref('geo_lookup')}} AS b
ON
  a.classB = b.classB
  AND a.clientIpNum BETWEEN b.startIpNum AND b.endIpNum
ORDER BY
  id ASC
