{#
  Comprehensive dataset comparison macro - V9
  
  Key improvement over V8:
  Two completely independent comparison paths:

  PATH 1 — Unique keys (keys appearing exactly once in BOTH source AND target)
    → Clean 1:1 row comparison, zero ambiguity
    → Produces: MISSING_IN_TARGET, MISSING_IN_SOURCE, VALUE_MISMATCH

  PATH 2 — Duplicate keys (keys appearing >1 in EITHER source OR target)
    → Set-based comparison: does any source row hash match any target row hash?
    → Produces: DUPLICATE_NO_MATCH, DUPLICATE_PARTIAL_MATCH, DUPLICATE_FULL_MATCH
    → Always also tagged with duplicate counts for visibility
    → fail_on_duplicates controls whether these rows cause test failure

  This eliminates false mismatches from arbitrary row picking (V7/V8 bug).
#}

{% macro comprehensive_compare(
    model,
    test_query,
    key_columns,
    compare_columns=None,
    fail_on_duplicates=True,
    max_duplicate_report=100,
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

  -- A key is "duplicate" if it appears >1 in EITHER side
  -- Also captures keys that exist in one side only with duplicates
  duplicate_keys as (
    select row_key_hash from source_key_counts where key_count > 1
    union
    select row_key_hash from target_key_counts where key_count > 1
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
  -- PATH 2: DUPLICATE KEY COMPARISON
  -- Set-based: does any source row hash match any target row hash?
  -- Reported separately, never mixed with Path 1 results
  -- NOTE: LIMIT inside CTE is Snowflake-specific
  -- ============================================================

  dup_source as (
    select s.*,
           skc.key_count as source_key_count
    from source_raw s
    inner join source_key_counts skc on s.row_key_hash = skc.row_key_hash
    inner join duplicate_keys d on s.row_key_hash = d.row_key_hash
    limit {{ max_duplicate_report }}
  ),

  dup_target as (
    select t.*,
           tkc.key_count as target_key_count
    from target_raw t
    inner join target_key_counts tkc on t.row_key_hash = tkc.row_key_hash
    inner join duplicate_keys d on t.row_key_hash = d.row_key_hash
    limit {{ max_duplicate_report }}
  ),

  -- Cross-join source and target rows on key + data hash to find content matches
  dup_hash_matches as (
    select
      s.row_key_hash,
      count(*) as matched_row_count
    from dup_source s
    inner join dup_target t
      on s.row_key_hash = t.row_key_hash
      and s.row_data_hash = t.row_data_hash   -- content match
    group by s.row_key_hash
  ),

  -- One representative source row per duplicate key (for reporting)
  dup_source_representative as (
    select *
    from dup_source
    qualify row_number() over (
      partition by row_key_hash
      order by {{ order_by_keys }}
    ) = 1
  ),

  -- One representative target row per duplicate key (for reporting)
  dup_target_representative as (
    select *
    from dup_target
    qualify row_number() over (
      partition by row_key_hash
      order by {{ order_by_keys }}
    ) = 1
  ),

  -- Classify each duplicate key group
  -- DUPLICATE_FULL_MATCH    : every source row has a matching target row (counts equal, all hashes match)
  -- DUPLICATE_PARTIAL_MATCH : some rows match, some don't
  -- DUPLICATE_NO_MATCH      : no content match found at all
  p2_duplicate_results as (
    select
      sr.row_key_hash,
      {% for key in key_cols %}
      sr.{{ key }},
      {% endfor %}
      case
        when hm.matched_row_count is null then
          {% if fail_on_duplicates %}'DUPLICATE_NO_MATCH'
          {% else %}'DUPLICATE_NO_MATCH_INFO'{% endif %}
        when hm.matched_row_count = sr.source_key_count
          and hm.matched_row_count = tr.target_key_count then
          'DUPLICATE_FULL_MATCH'
        else
          {% if fail_on_duplicates %}'DUPLICATE_PARTIAL_MATCH'
          {% else %}'DUPLICATE_PARTIAL_MATCH_INFO'{% endif %}
      end as anomaly_type,
      case
        when hm.matched_row_count is null then
          'Duplicate key group: no content matches found between source and target'
        when hm.matched_row_count = sr.source_key_count
          and hm.matched_row_count = tr.target_key_count then
          'Duplicate key group: all rows match between source and target'
        else
          'Duplicate key group: ' || to_varchar(coalesce(hm.matched_row_count, 0))
          || ' of ' || to_varchar(sr.source_key_count) || ' source rows matched'
      end as anomaly_description,
      sr.row_data_hash as source_hash,
      tr.row_data_hash as target_hash,
      {% for col in compare_columns %}
      sr.{{ col }} as source_{{ col }},
      tr.{{ col }} as target_{{ col }},
      {% endfor %}
      sr.source_key_count,
      tr.target_key_count,
      coalesce(hm.matched_row_count, 0) as matched_row_count
    from dup_source_representative sr
    left join dup_target_representative tr
      on sr.row_key_hash = tr.row_key_hash
    left join dup_hash_matches hm
      on sr.row_key_hash = hm.row_key_hash
  ),

  -- ============================================================
  -- FINAL: Combine both paths
  -- Path 1 = clean, unambiguous
  -- Path 2 = explicitly labelled duplicate analysis
  -- ============================================================

  all_anomalies as (
    select * from p1_missing_in_target
    union all
    select * from p1_missing_in_source
    union all
    select * from p1_value_mismatches
    union all
    select * from p2_duplicate_results
    -- Optional: exclude FULL_MATCH rows if you only want failures
    -- where anomaly_type != 'DUPLICATE_FULL_MATCH'
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
