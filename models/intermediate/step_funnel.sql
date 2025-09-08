-- flatten the combined_events_purchases table into funnel steps
WITH
  page_view AS (
  SELECT
    user_id,
    event_day,
    platform,
    channel,
    ts
  FROM
    {{ ref('combined_events_purchases')}} 
  WHERE
    event_name = 'page_view'),
  quote_start AS (
  SELECT
    user_id,
    event_day,
    platform,
    channel,
    ts
  FROM
    {{ ref('combined_events_purchases')}} 
  WHERE
    event_name = 'quote_start'),
  quote_submit AS (
  SELECT
    user_id,
    event_day,
    platform,
    channel,
    ts
  FROM
    {{ ref('combined_events_purchases')}} 
  WHERE
    event_name = 'quote_submit'),
  purchase AS (
  SELECT
    user_id,
    event_day,
    platform,
    channel,
    ts
  FROM
    {{ ref('combined_events_purchases')}} 
  WHERE
    event_name = 'purchase')
SELECT
  pv.event_day,
  pv.platform,
  pv.channel,
  COUNT(DISTINCT pv.user_id) AS page_view_users,
  COUNT(DISTINCT qs.user_id) AS quote_start_users,
  COUNT(DISTINCT qt.user_id) AS quote_submit_users,
  COUNT(DISTINCT p.user_id) AS purchase_users
FROM
  page_view pv
LEFT JOIN
  quote_start qs
ON
  pv.user_id = qs.user_id
  AND pv.ts < qs.ts
LEFT JOIN
  quote_submit qt
ON
  qs.user_id = qt.user_id
  AND qs.ts < qt.ts
LEFT JOIN
  purchase p
ON
  qt.user_id = p.user_id
  AND qt.ts < p.ts
GROUP BY
  pv.event_day,
  pv.platform,
  pv.channel
