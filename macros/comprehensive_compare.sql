{#
  Comprehensive dataset comparison macro - V10

  Two completely independent comparison paths:

  PATH 1 — Unique keys (keys appearing exactly once in BOTH source AND target)
    → Clean 1:1 row comparison, zero ambiguity
    → Produces: MISSING_IN_TARGET, MISSING_IN_SOURCE, VALUE_MISMATCH

  PATH 2 — Duplicate keys (keys appearing >1 in EITHER source OR target)
    → Reports all duplicate rows as-is; no content comparison attempted
    → Produces: DUPLICATE_IN_SOURCE, DUPLICATE_IN_TARGET
    → _INFO suffix when fail_on_duplicates=False
    → fail_on_duplicates controls whether these rows cause test failure
#}

{% macro comprehensive_compare(
    model,
    test_query,
    key_columns,
    compare_columns=None,
    fail_on_duplicates=True,
    target_name=None
) %}

  {% set key_cols = key_columns if key_columns is iterable and key_columns is not string else [key_columns] %}

  {% if key_cols | length == 0 %}
    {% do exceptions.raise_compiler_error('comprehensive_compare: key_columns is empty') %}
  {% endif %}

  {% set resolved_target_name = target_name if target_name else 'test_query' %}
  {% set order_by_keys = key_cols | join(', ') %}

  -- Resolve compare_columns dynamically if not provided
  {% if not compare_columns %}
    {% set compare_columns = adapter.get_columns_in_relation(model)
                            | map(attribute='name')
                            | reject('in', key_cols)
                            | list %}
  {% endif %}

  {% set has_compare_cols = compare_columns | length > 0 %}

  with

  -- ============================================================
  -- BASE LAYER: Raw source and target with row-level data hash
  -- ============================================================

  source_raw as (
    select
      {{ _comprehensive_compare_key_hash_v9(key_cols) }} as row_key_hash,
      {% for key in key_cols %}
      {{ key }},
      {% endfor %}
      {% for col in compare_columns %}
      {{ col }},
      {% endfor %}
      {% if has_compare_cols %}
        hash(concat_ws('|', {% for c in compare_columns %}
             coalesce(to_varchar({{ c }}), 'NULL'){% if not loop.last %}, {% endif %}{% endfor %})) as row_data_hash
      {% else %}
        null as row_data_hash
      {% endif %}
    from {{ model }}
  ),

  target_raw as (
    select
      {{ _comprehensive_compare_key_hash_v9(key_cols) }} as row_key_hash,
      {% for key in key_cols %}
      {{ key }},
      {% endfor %}
      {% for col in compare_columns %}
      {{ col }},
      {% endfor %}
      {% if has_compare_cols %}
        hash(concat_ws('|', {% for c in compare_columns %}
             coalesce(to_varchar({{ c }}), 'NULL'){% if not loop.last %}, {% endif %}{% endfor %})) as row_data_hash
      {% else %}
        null as row_data_hash
      {% endif %}
    from {{ test_query }}
  ),

  -- ============================================================
  -- KEY CLASSIFICATION: Count occurrences per key in each side
  -- ============================================================

  source_key_counts as (
    select
      row_key_hash,
      count(*) as key_count
    from source_raw
    group by row_key_hash
  ),

  target_key_counts as (
    select
      row_key_hash,
      count(*) as key_count
    from target_raw
    group by row_key_hash
  ),

  -- A key is "unique" only if it appears exactly once in BOTH sides
  -- Keys missing from one side are handled by PATH 1 missing logic
  unique_keys as (
    select s.row_key_hash
    from source_key_counts s
    inner join target_key_counts t
      on s.row_key_hash = t.row_key_hash
    where s.key_count = 1
      and t.key_count = 1
  ),

  -- ============================================================
  -- PATH 1: UNIQUE KEY COMPARISON
  -- Clean 1:1 comparison — fully trustworthy results
  -- ============================================================

  unique_source as (
    select s.*
    from source_raw s
    inner join unique_keys u on s.row_key_hash = u.row_key_hash
  ),

  unique_target as (
    select t.*
    from target_raw t
    inner join unique_keys u on t.row_key_hash = u.row_key_hash
  ),

  -- Keys present in source (unique) but completely absent from target
  p1_missing_in_target as (
    select
      s.row_key_hash,
      {% for key in key_cols %}
      s.{{ key }},
      {% endfor %}
      'MISSING_IN_TARGET' as anomaly_type,
      'Unique key exists in source but not in target' as anomaly_description,
      s.row_data_hash as source_hash,
      null::varchar as target_hash,
      {% for col in compare_columns %}
      s.{{ col }} as source_{{ col }},
      null as target_{{ col }},
      {% endfor %}
      null as source_key_count,
      null as target_key_count,
      null as matched_row_count
    from source_raw s
    left join source_key_counts skc on s.row_key_hash = skc.row_key_hash
    left join target_key_counts tkc on s.row_key_hash = tkc.row_key_hash
    where skc.key_count = 1       -- unique in source
      and tkc.row_key_hash is null -- absent from target entirely
  ),

  -- Keys present in target (unique) but completely absent from source
  p1_missing_in_source as (
    select
      t.row_key_hash,
      {% for key in key_cols %}
      t.{{ key }},
      {% endfor %}
      'MISSING_IN_SOURCE' as anomaly_type,
      'Unique key exists in target but not in source' as anomaly_description,
      null::varchar as source_hash,
      t.row_data_hash as target_hash,
      {% for col in compare_columns %}
      null as source_{{ col }},
      t.{{ col }} as target_{{ col }},
      {% endfor %}
      null as source_key_count,
      null as target_key_count,
      null as matched_row_count
    from target_raw t
    left join target_key_counts tkc on t.row_key_hash = tkc.row_key_hash
    left join source_key_counts skc on t.row_key_hash = skc.row_key_hash
    where tkc.key_count = 1       -- unique in target
      and skc.row_key_hash is null -- absent from source entirely
  ),

  -- 1:1 value mismatch on unique keys present in both sides
  p1_value_mismatches as (
    select
      s.row_key_hash,
      {% for key in key_cols %}
      s.{{ key }},
      {% endfor %}
      'VALUE_MISMATCH' as anomaly_type,
      'Unique key record values differ between source and target' as anomaly_description,
      s.row_data_hash as source_hash,
      t.row_data_hash as target_hash,
      {% for col in compare_columns %}
      s.{{ col }} as source_{{ col }},
      t.{{ col }} as target_{{ col }},
      {% endfor %}
      null as source_key_count,
      null as target_key_count,
      null as matched_row_count
    from unique_source s
    inner join unique_target t
      on s.row_key_hash = t.row_key_hash
    where s.row_data_hash != t.row_data_hash
  ),

  -- ============================================================
  -- PATH 2: DUPLICATE KEY REPORTING (simplified)
  -- No content comparison — all duplicate rows reported as-is for review
  -- ============================================================

  p2_duplicate_in_source as (
    select
      s.row_key_hash,
      {% for key in key_cols %}
      s.{{ key }},
      {% endfor %}
      {% if fail_on_duplicates %}'DUPLICATE_IN_SOURCE'{% else %}'DUPLICATE_IN_SOURCE_INFO'{% endif %} as anomaly_type,
      'Key appears ' || to_varchar(skc.key_count) || ' times in source; all rows reported for review' as anomaly_description,
      s.row_data_hash as source_hash,
      null::varchar   as target_hash,
      {% for col in compare_columns %}
      s.{{ col }} as source_{{ col }},
      null        as target_{{ col }},
      {% endfor %}
      skc.key_count as source_key_count,
      null          as target_key_count,
      null          as matched_row_count
    from source_raw s
    inner join source_key_counts skc on s.row_key_hash = skc.row_key_hash
    where skc.key_count > 1
  ),

  p2_duplicate_in_target as (
    select
      t.row_key_hash,
      {% for key in key_cols %}
      t.{{ key }},
      {% endfor %}
      {% if fail_on_duplicates %}'DUPLICATE_IN_TARGET'{% else %}'DUPLICATE_IN_TARGET_INFO'{% endif %} as anomaly_type,
      'Key appears ' || to_varchar(tkc.key_count) || ' times in target; all rows reported for review' as anomaly_description,
      null::varchar   as source_hash,
      t.row_data_hash as target_hash,
      {% for col in compare_columns %}
      null        as source_{{ col }},
      t.{{ col }} as target_{{ col }},
      {% endfor %}
      null          as source_key_count,
      tkc.key_count as target_key_count,
      null          as matched_row_count
    from target_raw t
    inner join target_key_counts tkc on t.row_key_hash = tkc.row_key_hash
    where tkc.key_count > 1
  ),

  -- ============================================================
  -- FINAL: Combine both paths
  -- Path 1 = clean, unambiguous unique-key comparison
  -- Path 2 = duplicate rows reported as-is for human review
  -- ============================================================

  all_anomalies as (
    select * from p1_missing_in_target
    union all
    select * from p1_missing_in_source
    union all
    select * from p1_value_mismatches
    union all
    select * from p2_duplicate_in_source
    union all
    select * from p2_duplicate_in_target
  )

  select
    row_key_hash,
    {% for key in key_cols %}
    {{ key }},
    {% endfor %}
    anomaly_type,
    anomaly_description,
    source_hash,
    target_hash,
    {% for col in compare_columns %}
    source_{{ col }},
    target_{{ col }},
    {% endfor %}
    source_key_count,
    target_key_count,
    matched_row_count,
    current_timestamp()                                       as detected_at,
    '{{ model }}'                                             as source_model,
    '{{ resolved_target_name }}'                              as target_model,
    count(*) over ()                                          as total_anomalies_count,
    count(*) over (partition by anomaly_type)                 as anomaly_type_count

  from all_anomalies

{% endmacro %}


-- Helper: always returns hash() for type consistency across 1-key and n-key paths
{% macro _comprehensive_compare_key_hash_v9(key_columns) %}
  {% if key_columns | length == 1 %}
    hash(coalesce(to_varchar({{ key_columns[0] }}), '___NULL_KEY___'))
  {% else %}
    hash(concat_ws('|', {% for key in key_columns %}
         coalesce(to_varchar({{ key }}), '___NULL_KEY___'){% if not loop.last %}, {% endif %}{% endfor %}))
  {% endif %}
{% endmacro %}
