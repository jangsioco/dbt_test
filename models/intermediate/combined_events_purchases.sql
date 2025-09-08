


WITH
  combined_user_data AS (
  SELECT
  *,
  case event_name when 'page_view' then 1
    when 'quote_start' then 2
    when 'quote_submit' then 3
    else 4 end as step_number,
  DATE_TRUNC(CAST(ts AS date), DAY) AS event_day
  FROM
    {{ ref('stg_events_purchases') }} 

-- apply filter for 30 days and relevant event names
/*  WHERE (CAST(purchase_ts AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    OR CAST(event_ts AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))

    AND event_name in ('page_view','quote_start','quote_submit','purchase')
*/
  )

SELECT
  *
FROM
  combined_user_data