SELECT
  event_day,
  platform,
  channel,
  PERCENTILE_DISC(quote_to_purchase_minutes,0.5) OVER(PARTITION BY event_day, platform, channel) AS median_time
FROM
  {{ ref('quote_to_purchase_funnel') }}  