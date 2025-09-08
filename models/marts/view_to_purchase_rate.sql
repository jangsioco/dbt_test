-- compute conversion rate between page_view to purchase in the funnel

SELECT
  event_day,
  platform,
  channel,
  ROUND(COALESCE(SAFE_DIVIDE(purchase_users, page_view_users),0),2) AS view_to_purchase_rate,
FROM
  {{ ref('view_to_purchase_funnel') }} 
