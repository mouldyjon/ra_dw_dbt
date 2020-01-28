WITH campaigns AS (
  SELECT
    *
  FROM
    {{ ref('mailchimp_campaigns') }}
),

select_campaigns AS (
  SELECT
    content_type,
    created_at,
    number_emails_sent,
    campaign_id,
    archive_url,
    list_id,
    list_is_active,
    list_name,
    recipient_count,
    segment_opts,
    segment_text,
    segment_conditions,
    segment_match,
    resendable,
    sent_at,
    has_authenticate,
    has_auto_footer,
    has_auto_tweet,
    is_drag_and_drop,
    has_fb_comments,
    from_name,
    preview_text,
    reply_to,
    subject_line,
    template_id,
    timewarp,
    title,
    to_name,
    status,
    _sdc_batched_at,
    MAX(_sdc_batched_at) over (
      PARTITION BY campaign_id
      ORDER BY
        _sdc_batched_at RANGE BETWEEN unbounded preceding
        AND unbounded following
    ) AS max_sdc_batched_at
  FROM
    campaigns
),

latest_version AS (
  SELECT
    *
  FROM
    select_campaigns
  WHERE
    _sdc_batched_at = max_sdc_batched_at
)


SELECT
  *
FROM
  latest_version
