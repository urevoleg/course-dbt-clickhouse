{% macro distributed_clone_relation_schema(target_relation, source_relation, target_schema, sharding_key) %}
  {% set query %}
    CREATE TABLE {{ target_relation }}
        ON CLUSTER '{cluster}'
        AS {{ source_relation }}
    ENGINE = Distributed('{cluster}', '{{target_schema}}', '{{target_relation.table}}_shard', {{ sharding_key }});
  {% endset %}

  {{ log("Executing query for Distributed: " ~ query, true) }}

  {% do run_query(query) %}


  {% set query %}
    CREATE TABLE {{ target_relation }}_shard
        ON CLUSTER '{cluster}'
        AS {{ source_relation }}_shard;
  {% endset %}

  {{ log("Executing query for shard: " ~ query, true) }}

  {% do run_query(query) %}

{% endmacro %}
