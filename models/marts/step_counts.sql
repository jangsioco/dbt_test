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

/*SELECT
  event_day,
  platform,
  channel,
  COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN user_id END) AS page_views,
  COUNT(DISTINCT CASE WHEN event_name = 'quote_start' THEN user_id END) AS quote_starts,
  COUNT(DISTINCT CASE WHEN event_name = 'quote_submit' THEN user_id END) AS quote_submits,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) AS purchases
FROM
   {{ ref('step_funnel')}} 
GROUP BY
  event_day,
  platform,
  channel
*/


