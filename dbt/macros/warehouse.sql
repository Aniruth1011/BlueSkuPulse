{% macro incremental_config(unique_key, partition_field, cluster_fields) %}
  {% if target.type == 'bigquery' %}
    {{ config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key=unique_key,
      partition_by={'field': partition_field, 'data_type': 'date', 'granularity': 'day'},
      cluster_by=cluster_fields,
      require_partition_filter=false,
      on_schema_change='append_new_columns'
    ) }}
  {% else %}
    {{ config(
      materialized='incremental',
      unique_key=unique_key,
      on_schema_change='append_new_columns'
    ) }}
  {% endif %}
{% endmacro %}

{% macro domain_from_url(column_name) %}
  {% if target.type == 'bigquery' %}
    lower(regexp_replace(net.host({{ column_name }}), r'^www\.', ''))
  {% else %}
    lower(regexp_extract({{ column_name }}, '^https?://(?:www\.)?([^/:?#]+)', 1))
  {% endif %}
{% endmacro %}

{% macro hour_bucket(column_name) %}
  {% if target.type == 'bigquery' %}
    timestamp_trunc({{ column_name }}, hour)
  {% else %}
    date_trunc('hour', {{ column_name }})
  {% endif %}
{% endmacro %}

{% macro five_minute_bucket(column_name) %}
  {% if target.type == 'bigquery' %}
    timestamp_seconds(div(unix_seconds({{ column_name }}), 300) * 300)
  {% else %}
    to_timestamp(floor(epoch({{ column_name }}) / 300) * 300)
  {% endif %}
{% endmacro %}

{% macro minute_bucket(column_name, minutes) %}
  {% if target.type == 'bigquery' %}
    timestamp_seconds(div(unix_seconds({{ column_name }}), {{ minutes }} * 60) * {{ minutes }} * 60)
  {% else %}
    to_timestamp(floor(epoch({{ column_name }}) / ({{ minutes }} * 60)) * ({{ minutes }} * 60))
  {% endif %}
{% endmacro %}

{% macro topic_tokens(column_name) %}
  {% if target.type == 'bigquery' %}
    regexp_extract_all(lower(coalesce({{ column_name }}, '')), r'#[a-z0-9_]{2,64}')
  {% else %}
    regexp_extract_all(lower(coalesce({{ column_name }}, '')), '#[a-z0-9_]{2,64}')
  {% endif %}
{% endmacro %}

{% macro duration_seconds(start_column, end_column) %}
  {% if target.type == 'bigquery' %}
    timestamp_diff({{ end_column }}, {{ start_column }}, second)
  {% else %}
    date_diff('second', {{ start_column }}, {{ end_column }})
  {% endif %}
{% endmacro %}

{% macro select_without_rank(columns) %}
  {{ columns | join(', ') }}
{% endmacro %}

{% macro incremental_cutoff(relation, column_name='ingested_at') %}
  {% if target.type == 'bigquery' %}
    (
      select timestamp_sub(
        coalesce(max({{ column_name }}), timestamp('1970-01-01')),
        interval 2 day
      ) from {{ relation }}
    )
  {% else %}
    (
      select coalesce(max({{ column_name }}), timestamp '1970-01-01') - interval '2 days'
      from {{ relation }}
    )
  {% endif %}
{% endmacro %}

{% macro maximum_allowed_event_time() %}
  {% if target.type == 'bigquery' %}
    timestamp_add(current_timestamp(), interval 1 day)
  {% else %}
    current_timestamp + interval '1 day'
  {% endif %}
{% endmacro %}
