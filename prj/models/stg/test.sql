{{ config(order_by='id',
engine='MergeTree()',
materialized='distributed',
sharding_key='cityHas64h(id)',
partition_by='toYYYYMMDD(_insert_datetime)',
schema='stg',
query_settings={'max_threads': 5, 'max_insert_threads': 5},
settings={'index_granularity': 8192, 'allow_nullable_key': True},
tags=['stg', 'test']) }}

SELECT arrayJoin(range(10)) as id, now() as _insert_datetime
