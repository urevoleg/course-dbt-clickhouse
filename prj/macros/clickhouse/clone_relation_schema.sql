{% macro clone_relation_schema(target_relation, source_relation) %}
  {% set query %}
    CREATE TABLE {{ target_relation }} AS {{ source_relation }};
  {% endset %}

  {{ log("Executing query: " ~ query, true) }}

  {% do run_query(query) %}

{% endmacro %}
