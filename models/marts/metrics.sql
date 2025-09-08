SELECT
  v.event_day,
  v.platform,
  v.channel,
  v.distinct_users AS visitors,
  sc.page_views,
  sc.quote_starts,
  sc.quote_submits,
  sc.purchases,
  scr.quote_start_rate,
  scr.quote_submit_rate,
  scr.purchase_rate,
  vpr.view_to_purchase_rate,
  t.median_time
FROM
  {{ ref('visitors')}} v
LEFT JOIN
  {{ ref('step_counts')}} sc
ON
  v.event_day = sc.event_day
  AND v.platform = sc.platform
  AND v.channel = sc.channel
LEFT JOIN
  {{ ref('step_conversion_rate')}}  scr
ON
  v.event_day = scr.event_day
  AND v.platform = scr.platform
  AND v.channel = scr.channel
LEFT JOIN
  {{ ref('view_to_purchase_rate')}}  vpr
ON
  v.event_day = vpr.event_day
  AND v.platform = vpr.platform
  AND v.channel = vpr.channel
LEFT JOIN
  {{ ref('quote_to_purchase_time')}} t
ON
  v.event_day = t.event_day
  AND v.platform = t.platform
  AND v.channel = t.channel