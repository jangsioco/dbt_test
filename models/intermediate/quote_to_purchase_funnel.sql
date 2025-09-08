-- flatten combined_events_purchases to compute time of first quote_start occurence to purchase

WITH
  rank_ts AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id, event_day, platform, channel ORDER BY ts) AS rn
  FROM
    {{ ref('combined_events_purchases')}} 
  WHERE
    event_name = 'quote_start' ),
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
    event_name = 'purchase'),
  quote_start AS (
SELECT
  user_id,
  event_day,
  platform,
  channel,
  ts
FROM
  rank_ts
WHERE
  rn = 1)

SELECT
  p.user_id,
  p.event_day,  
  p.platform,
  p.channel,
  TIMESTAMP_DIFF(cast(p.ts as timestamp), cast(qs.ts as timestamp), MINUTE) AS quote_to_purchase_minutes,
FROM
  quote_start qs
INNER JOIN
  purchase p
ON
  qs.user_id = p.user_id
  AND qs.event_day = p.event_day
  AND qs.platform = p.platform 
  AND qs.channel = p.channel  
  AND qs.ts < p.ts
