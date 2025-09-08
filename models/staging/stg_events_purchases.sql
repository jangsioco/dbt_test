
{{ config(
  materialized='table',
  on_schema_change='fail'
) }}

WITH
  combined_user_data AS (
  SELECT
    p.user_id,
    p.product,
    p.purchase_ts as ts,
    p.channel,
    p.platform,
    p.app_version,
    p.city,
    p.country,
    'NA' as campaign,
    'purchase' as event_name
  FROM {{ ref('purchases') }} p
  WHERE (CAST(p.purchase_ts AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    UNION ALL
    SELECT
    e.user_id,
    e.product_hint AS product,
    e.event_ts as ts,
    e.channel,
    e.platform,
    e.app_version,
    e.city,
    e.country,
    e.campaign,
    e.event_name
  FROM
    {{ ref('events') }} e
  WHERE (CAST(e.event_ts AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    AND e.event_name in ('page_view','quote_start','quote_submit')

  )

SELECT
  *
FROM
  combined_user_data
