{%- materialization distributed_replace_partition_mtzn, default -%}
{# все названия таблиц указываются как Distributed, без шардового постфикса #}
  {%- set sharding_key = config.require('sharding_key') -%}
{#  {%- set table_name = config.get('table_name') -%}#}
  {%- set src_schema_name = config.require('src_schema_name') -%}
  {%- set src_table_name = config.get('src_table_name') -%}
  {%- set order_by = config.get('order_by') -%}
  {%- set partition_by = config.get('partition_by') -%}
  {%- set engine = config.get('engine') -%}

  {%- set target_relation = this -%}
  {%- set source_relation = api.Relation.create(schema=src_schema_name, identifier=src_table_name) -%}
  {%- set relation_exists = load_relation(target_relation) -%}
  {%- if not relation_exists -%}
    {%- do distributed_clone_relation_schema(target_relation, source_relation, this.schema, sharding_key) -%}
  {%- endif -%}

  -- disable transaction
  {%- do run_hooks(pre_hooks, inside_transaction=False) -%}

  {%- if execute -%}
    {%- set partition_list = distributed_get_table_partition_list(src_schema_name, src_table_name) -%}
  {%- endif -%}

  {%- for partition in partition_list -%}
    {%- do distributed_replace_partition(target_relation, source_relation, partition) -%}
{#    {%- do drop_partition(source_relation, partition) -%}#}
  {%- endfor -%}

  {%- call statement('main', fetch_result=False) -%}
    select 'finished materialization: {{ target_relation }};'
  {%- endcall -%}

  {%- do run_hooks(pre_hooks, inside_transaction=False) -%}

  {%- do return({'relations': [target_relation]}) -%}

{%- endmaterialization -%}
