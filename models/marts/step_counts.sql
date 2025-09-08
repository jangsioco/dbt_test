-- distinct users hitting each step for each day, platform and channel
SELECT 
  event_day,
  platform,
  channel,
  page_view_users AS page_views,
  quote_start_users AS quote_starts,
  quote_submit_users AS quote_submits,
  purchase_users AS purchases
FROM
   {{ ref('step_funnel')}} 




