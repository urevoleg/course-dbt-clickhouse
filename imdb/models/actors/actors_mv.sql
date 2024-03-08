{{ config(order_by='(gender)',
engine='MergeTree()',
materialized='materialized_view',
  tags=['mv']) }}

select gender, count(1) as cnt
from {{ source('imdb', 'actors') }}
group by gender