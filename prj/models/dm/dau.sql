{{ config(order_by='event_date, week_dt',
engine='MergeTree()',
 materialized='table',
 schema='dm',
  tags=['dm', 'dau', 'total_provider']) }}

SELECT *, dau / wau AS sticky
FROM (SELECT DISTINCT event_date,
       toStartOfWeek(event_date) as week_dt,
       uniqExact(appmetrica_device_id) over(partition by event_date) AS dau,
       uniqExact(appmetrica_device_id) over(partition by week_dt) AS wau
FROM {{ ref('provider') }}
GROUP BY event_date, appmetrica_device_id)