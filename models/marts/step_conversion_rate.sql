-- compute conversion rates between steps in the funnel

SELECT
  event_day,
  platform,
  channel,
  1 AS page_view_rate,
  ROUND(COALESCE(SAFE_DIVIDE(quote_start_users, page_view_users),0),2) AS quote_start_rate,
  ROUND(COALESCE(SAFE_DIVIDE(quote_submit_users, quote_start_users),0),2) AS quote_submit_rate,
  ROUND(COALESCE(SAFE_DIVIDE(purchase_users, quote_submit_users),0),2) AS purchase_rate,
FROM
  {{ ref('step_funnel') }} 