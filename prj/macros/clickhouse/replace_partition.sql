{% macro replace_partition(target_relation, source_relation, partition) %}
  {% set query %}
    ALTER TABLE {{ target_relation }}
    REPLACE PARTITION '{{ partition }}' FROM {{ source_relation }}
  {% endset %}

  {{ log("Executing query: " ~ query, true) }}

  {% do run_query(query) %}

{% endmacro %}
