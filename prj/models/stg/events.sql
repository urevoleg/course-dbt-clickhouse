{{ config(order_by='os_name, device_type, device_manufacturer, city, device_model, event_name, event_date, appmetrica_device_id',
engine='MergeTree()',
materialized='table',
schema='stg',
query_settings={'max_threads': 5, 'max_insert_threads': 5},
settings={'index_granularity': 8192, 'allow_nullable_key': True},
tags=['stg_events', 'total_provider']) }}

SELECT *
FROM {{ source('stg', 'events') }}
WHERE event_date = '{{ var('dm_date') }}'
