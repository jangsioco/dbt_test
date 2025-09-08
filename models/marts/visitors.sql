-- distinct users for each day, platform and channel
SELECT
  event_day,
  platform,
  channel,
  COUNT(DISTINCT user_id) AS distinct_users
FROM
   {{ ref('combined_events_purchases')}} 
GROUP BY
  event_day,
  platform,
  channel
ORDER BY
  event_day,
  platform,
  channel