{% macro distributed_replace_partition(target_relation, source_relation, partition) %}
  {% set query %}
    ALTER TABLE {{ target_relation }}_shard
        ON CLUSTER '{cluster}'
    REPLACE PARTITION '{{ partition }}' FROM {{ source_relation }}_shard
  {% endset %}

  {{ log("Executing query: " ~ query, true) }}

  {% do run_query(query) %}

{% endmacro %}
