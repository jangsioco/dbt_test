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
  COUNT(DISTINCT p.user_id) AS purchase_users
FROM
  page_view pv
LEFT JOIN
  purchase p
ON
  pv.user_id = p.user_id
  AND pv.ts < p.ts
GROUP BY
  pv.event_day,
  pv.platform,
  pv.channel