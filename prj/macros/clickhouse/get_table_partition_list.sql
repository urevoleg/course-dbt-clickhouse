-- get partition list on cluster for table
{% macro get_table_partition_list(schema_name, table_name) %}
  {% set query %}
        SELECT `partition` FROM system.parts
        WHERE table = '{{ table_name }}'
        AND database = '{{ schema_name }}'
        GROUP BY 1
  {% endset %}

  {{ log("Executing query: " ~ query, true) }}

  {% set partition_list = run_query(query).columns[0].values() %}

  {{ log("Partition_list: " ~ partition_list, true) }}

  {% do return(partition_list) %}

{%- endmacro %}
