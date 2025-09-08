-- assign event numbers and event days to each event and purchase

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


  )

SELECT
  *
FROM
  combined_user_data