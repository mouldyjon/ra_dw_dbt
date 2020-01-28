WITH campaigns AS (
  SELECT
    *
  FROM
    {{ ref('email_campaigns_dim') }}
),
list_members AS (
  SELECT
    *
  FROM
    {{ ref('email_list_membership_fact') }}
),
sends AS (
  SELECT
    CONCAT(
      listmembers.email_id,'_',campaigns.campaign_id,'_',campaigns.list_id
    ) AS send_id,
    listmembers.email_id,
    campaigns.campaign_id,
    campaigns._sdc_batched_at,
    campaigns.created_at,
    campaigns.number_emails_sent,
    campaigns.archive_url,
    campaigns.list_id,
    campaigns.list_is_active,
    campaigns.list_name,
    campaigns.recipient_count,
    campaigns.sent_at,
    campaigns.has_authenticate,
    campaigns.has_auto_footer,
    campaigns.has_auto_tweet,
    campaigns.is_drag_and_drop,
    campaigns.has_fb_comments,
    campaigns.from_name,
    campaigns.preview_text,
    campaigns.reply_to,
    campaigns.subject_line,
    campaigns.template_id,
    campaigns.title,
    campaigns.to_name,
    campaigns.status
    --need to add number of clicks, opens, bounces?
    --does this help or actually remove valuable granularity about what was clicked?
  FROM
    campaigns
    INNER JOIN list_members AS listmembers ON campaigns.list_id = listmembers.list_id
  WHERE
    campaigns.sent_at IS NOT NULL
)
SELECT
  *
FROM
  sends
