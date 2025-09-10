-- distinct users for each day, platform and channel
with events_purchases as (
SELECT
  user_id,
  DATE_TRUNC(CAST(purchase_ts AS date), DAY) AS event_day,
  platform,
  channel
FROM {{ ref('purchases') }}
WHERE (CAST(purchase_ts AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var("days_past") }} DAY))
UNION ALL
SELECT
  user_id,
  DATE_TRUNC(CAST(event_ts AS date), DAY) AS event_day,
  platform,
  channel
FROM {{ ref('events') }}
WHERE (CAST(event_ts AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var("days_past") }} DAY))
)


SELECT
  event_day,
  platform,
  channel,
  COUNT(DISTINCT user_id) AS distinct_users
FROM
  events_purchases
GROUP BY
  event_day,
  platform,
  channel
ORDER BY
  event_day,
  platform,
  channel