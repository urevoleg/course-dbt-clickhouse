-- replace partition macro
{% macro drop_partition(relation, table_name, partition) %}
  {% set query %}
    ALTER TABLE {{ relation }}
    DROP PARTITION '{{ partition }}'
  {% endset %}

  {{ log("Executing query: " ~ query, true) }}

  {% do run_query(query) %}

{% endmacro %}
