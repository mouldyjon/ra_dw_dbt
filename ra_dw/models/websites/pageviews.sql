{{
    config(
        materialized='table'
    )
}}
SELECT
  site, anonymous_id, a.ip, context_library_name, context_page_path, context_page_search, context_page_title, context_page_url, context_user_agent, id, _sdc_received_at, path, received_at, search, title, url, timestamp, referrer, country, region, city,  latitude, longitude,
  d.network,
  d.domain,
  d.network_type,
  d.network_category,
 
	CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('EDU', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Education' ELSE CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('COM', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Commercial' ELSE CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('ORG', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Non-Profit' ELSE CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('GOV', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Government' ELSE 'Home User' END
 END
 END
 END
 AS visitor_subcategory,
 CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('EDU', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Business' ELSE CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('COM', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Business' ELSE CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('ORG', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Business' ELSE CASE WHEN network_category LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('GOV', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Business' ELSE 'Personal' END
 END
 END
 END
 AS visitor_category,
 CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Services', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Services' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('About', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'About' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Careers', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Careers' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Contact', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Contact' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Drill', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Podcast' ELSE CASE WHEN path LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('blog', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Blog' ELSE CASE WHEN path = '/' THEN 'Homepage' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Customer', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Customers' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Resoures', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Resources' ELSE 'Others' END
 END
 END
 END
 END
 END
 END
 END
 END
 AS page_subcategory,
 CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Services', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Commercial Page' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('About', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Commercial Page' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Careers', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Commercial Page' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Contact', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Commercial Page' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Drill', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Community Page' ELSE CASE WHEN path LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('blog', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Community Page' ELSE CASE WHEN path = '/' THEN 'Commercial Page' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Customer', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Commercial Page' ELSE CASE WHEN title LIKE CONCAT(CAST('%' AS STRING), CAST(REPLACE(REPLACE(REPLACE('Resoures', '\\', '\\\\'), '%', '\\%'), '_', '\\_') AS STRING), CAST('%' AS STRING)) THEN 'Community Page' ELSE 'Commercial Page' END
 END
 END
 END
 END
 END
 END
 END
 END
 AS page_category
FROM (
  SELECT
    site, anonymous_id, ip, context_library_name, context_page_path, context_page_search, context_page_title, context_page_url, context_user_agent, id, _sdc_received_at, path, received_at, search, title, url, timestamp, referrer,
    NET.IPV4_TO_INT64(NET.IP_FROM_STRING(ip)) AS clientIpNum,
    TRUNC(NET.IPV4_TO_INT64(NET.IP_FROM_STRING(ip))/(256*256)) AS classB
  FROM
    {{ ref('segment_combined_pageviews')}} p ) AS a
LEFT OUTER JOIN
  `ra-development.segment_ra_website.domains` as d
ON a.ip = d.ip
