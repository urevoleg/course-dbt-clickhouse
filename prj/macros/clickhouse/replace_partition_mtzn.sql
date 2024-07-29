{%- materialization replace_partition_mtzn, default -%}
  {%- set schema_name = config.require('schema_name') -%}
  {%- set table_name = config.get('table_name') -%}
  {%- set src_schema_name = config.require('src_schema_name') -%}
  {%- set src_table_name = config.get('src_table_name') -%}
  {%- set order_by = config.get('order_by') -%}
  {%- set partition_by = config.get('partition_by') -%}
  {%- set engine = config.get('engine') -%}

  {%- set target_relation = this -%}
  {%- set source_relation = api.Relation.create(schema=src_schema_name, identifier=src_table_name) -%}
  {%- set relation_exists = load_relation(target_relation) -%}
  {%- if not relation_exists -%}
    {%- do clone_relation_schema(target_relation, source_relation) -%}
  {%- endif -%}

  -- disable transaction
  {%- do run_hooks(pre_hooks, inside_transaction=False) -%}

  {%- if execute -%}
    {%- set partition_list = get_table_partition_list(src_schema_name, src_table_name) -%}
  {%- endif -%}

  {%- for partition in partition_list -%}
    {%- do replace_partition(target_relation, source_relation, partition) -%}
    {%- do drop_partition(source_relation, partition) -%}
  {%- endfor -%}

  {%- call statement('main', fetch_result=False) -%}
    select 'finished materialization: {{ target_relation }};'
  {%- endcall -%}

  {%- do run_hooks(pre_hooks, inside_transaction=False) -%}

  {%- do return({'relations': [target_relation]}) -%}

{%- endmaterialization -%}
