# course-dbt-clickhouse

Для именования шардовых таблиц необходимо изменить файл `adapters/clickhouse/credentials.py`:

```
# 35 line
local_suffix: str = 'shard' #TODO FIXME
```