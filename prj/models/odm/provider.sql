{{ config(order_by='os_name, connection_type, operator_name, device_manufacturer, device_model, city, event_name, event_date, appmetrica_device_id',
engine='MergeTree()',
 materialized='incremental',
 incremental_strategy='append',
 schema='odm',
 query_settings={'max_threads': 5, 'max_insert_threads': 5},
 settings={'index_granularity': 8192, 'allow_nullable_key': True},
  tags=['odm', 'provider', 'total_provider']) }}

SELECT
    event_date,
    appmetrica_device_id,
    argMin(if(os_name = '', NULL, os_name), event_datetime) AS os_name,
    argMin(if(device_model = '', NULL, device_model) , event_datetime) AS device_model,
    argMin(if(device_manufacturer = '', NULL, device_manufacturer) , event_datetime) AS device_manufacturer,
    argMin(if(event_name = '', NULL, event_name), event_datetime) AS event_name,
    argMin(if(connection_type = '', NULL, connection_type) , event_datetime) AS connection_type,
    argMin(if(operator_name = '', NULL, operator_name) , event_datetime) AS operator_name,
    argMin(if(city = '', NULL, city) , event_datetime) AS city,
    arrayDistinct(groupArray(app_version_name)) AS app_version_name_ar,
    arrayDistinct(groupArray(IF(NOT JSONExtractString(event_json, 'magnit_id') IN ('unknown', 'null'), JSONExtractString(event_json, 'magnit_id'), NULL))) AS magnit_ids,
    arrayDistinct(groupArray(IF(NOT JSONExtractString(event_json, 'customer_id') IN ('unknown', 'null'), JSONExtractString(event_json, 'customer_id'), NULL))) AS customer_ids,
    count(1) AS event_cnt
FROM {{ ref('events') }}
WHERE event_date = '{{ var('dm_date') }}'
GROUP BY event_date, appmetrica_device_id
