WITH
  stitch_segment_ra_website AS (
  SELECT
    'rittmananalytics.com' AS site,
    anonymous_id,
    context.ip AS ip,
    context.library.name AS context_library_name,
    context.page.path AS context_page_path,
    context.page.search AS context_page_search,
    context.page.title AS context_page_title,
    context.page.url AS context_page_url,
    context.user_agent AS context_user_agent,
    message_id AS id,
    _sdc_received_at,
    properties.path AS path,
    CAST(received_at AS timestamp) AS received_at,
    context.page.search AS search,
    properties.title AS title,
    properties.url AS url,
    CAST(timestamp AS timestamp) timestamp,
    properties.referrer AS referrer
  FROM
    `ra-development.segment_ra_website.page` ),
  segment_ra_website AS (
  SELECT
    'rittmananalytics.com' AS site,
    anonymous_id,
    context_ip,
    context_library_name,
    context_page_path,
    context_page_search,
    context_page_title,
    context_page_url,
    context_user_agent,
    id,
    loaded_at AS _sdc_received_at,
    path,
    received_at,
    search,
    title,
    url,
    uuid_ts,
    referrer
  FROM
    `ra-development.segment_website_events.pages`),
  segment_drilltodetail_archive AS (
  SELECT
    'drilltodetail.com' AS site,
    anonymous_id,
    context_ip,
    context_library_name,
    context_page_path,
    context_page_search,
    context_page_title,
    context_page_url,
    context_user_agent,
    id,
    loaded_at AS _sdc_received_at,
    path,
    received_at,
    search,
    title,
    url,
    uuid_ts,
    referrer
  FROM
    `ra-development.segment_drilltodetail_website.pages_archive`),
  stitch_segment_dtd_website AS (
  SELECT
    'drilltodetail.com' AS site,
    anonymous_id,
    context.ip AS ip,
    context.library.name AS context_library_name,
    context.page.path AS context_page_path,
    context.page.search AS context_page_search,
    context.page.title AS context_page_title,
    context.page.url AS context_page_url,
    context.user_agent AS context_user_agent,
    message_id AS id,
    _sdc_received_at,
    properties.path AS path,
    CAST(received_at AS timestamp) AS received_at,
    context.page.search AS search,
    properties.title AS title,
    properties.url AS url,
    CAST(timestamp AS timestamp) timestamp,
    properties.referrer AS referrer
  FROM
    `ra-development.segment_drilltodetail_website.page` )
SELECT
  *
FROM
  stitch_segment_ra_website
UNION ALL
SELECT
  *
FROM
  segment_ra_website
UNION ALL
SELECT
  *
FROM
  segment_drilltodetail_archive
UNION ALL
SELECT
  *
FROM
  stitch_segment_dtd_website
