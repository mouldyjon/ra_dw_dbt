{{
    config(
        materialized='table'
    )
}}
SELECT
  site, anonymous_id, ip, context_library_name, context_page_path, context_page_search, context_page_title, context_page_url, context_user_agent, id, _sdc_received_at, path, received_at, search, title, url, timestamp, referrer, country, region, postalCode, latitude, longitude, metroCode, areaCode,
  IFNULL(city, 'Other') AS city,
  IFNULL(countryLabel, 'Other') AS countryLabel
FROM (
  SELECT
    site, anonymous_id, ip, context_library_name, context_page_path, context_page_search, context_page_title, context_page_url, context_user_agent, id, _sdc_received_at, path, received_at, search, title, url, timestamp, referrer,
    NET.IPV4_TO_INT64(NET.IP_FROM_STRING(ip)) AS clientIpNum,
    TRUNC(NET.IPV4_TO_INT64(NET.IP_FROM_STRING(ip))/(256*256)) AS classB
  FROM
    {{ ref('segment_combined_pageviews')}} p ) AS a
LEFT OUTER JOIN
  {{ ref('geo_lookup')}} AS b
ON
  a.classB = b.classB
  AND a.clientIpNum BETWEEN b.startIpNum AND b.endIpNum
ORDER BY
  id ASC
