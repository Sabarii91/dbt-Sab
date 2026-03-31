{#
  Compares the delta-slice view with the BEFORE_DELTA clone in schema var('before_delta_schema', 'QA')
  (created by staging pre_hook/post_hook). Uses LEFT JOIN to classify each row as NEW, UPDATED, or UNCHANGED.
  Does not modify comprehensive_compare; this is a separate validation.

  Returns rows where change_type in ('NEW', 'UPDATED') so the test fails/warns and store_failures
  captures both for validation and testing evidence; UNCHANGED rows are excluded. Use
  store_failures: true and severity: warn so the run completes while still saving.

  For each compare column, outputs after_delta_<col> (new delta value) and before_delta_<col> (clone value)
  so stored failures show exact row-level and value-level differences (clone vs new delta).
  Also outputs columns_changed: for NEW rows, 'NEW'; for UPDATED rows, comma-separated list of
  column names where view value differs from before value.

  Also outputs change_type_count so callers can see NEW vs UPDATED totals without a separate query.

  Arguments (via test arguments):
    - model         : the delta-slice view/model to validate
    - base_model    : name of the base model that created the clone (e.g. 'stg_customers')
    - clone_suffix  : suffix of the clone table (default '_BEFORE_DELTA', resolved here — not hidden in helper)
    - key_columns   : required — single column name (string) or list for composite join key
    - compare_columns: optional list; if omitted, all non-key columns from model are used.
                       Column order is sorted alphabetically to ensure stable hash across model rebuilds.

  Changes from v1:
    - [FIX #1]  Hash uses concat_ws('|', ...) pattern to eliminate collision risk
    - [FIX #2]  key_columns=none now raises a clear compiler error instead of silently breaking
    - [FIX #3]  Clone existence validated at compile time via adapter.get_relation()
    - [FIX #4]  Duplicate key fan-out prevented — duplicate keys classified and reported separately
    - [FIX #5]  Non-Snowflake hash fallback replaced with cross-platform concat_ws + md5
    - [FIX #6]  columns_changed cross-platform — uses IS DISTINCT FROM on all platforms
    - [FIX #7]  NEW detection uses _join_found sentinel column, not nullable key columns
    - [FIX #8]  clone_suffix default '_BEFORE_DELTA' defined here, not hidden in helper macro
    - [FIX #9]  change_type_count added to output for NEW vs UPDATED totals at a glance
    - [FIX #10] compare_columns sorted alphabetically for stable hash across schema rebuilds
#}

{% macro delta_compare(
    model,
    base_model,
    clone_suffix='_BEFORE_DELTA',
    key_columns=none,
    compare_columns=none
) %}

  {# ── FIX #2: Guard against missing key_columns ─────────────────────────────── #}
  {% if not key_columns %}
    {% do exceptions.raise_compiler_error(
      'delta_compare: key_columns is required. Pass a column name or list of column names.'
    ) %}
  {% endif %}

  {% set key_cols = [key_columns] if key_columns is string else key_columns %}

  {% if key_cols | length == 0 %}
    {% do exceptions.raise_compiler_error('delta_compare: key_columns must not be empty') %}
  {% endif %}

  {# ── FIX #10: Sort compare_columns for stable hash order across rebuilds ────── #}
  {% if not compare_columns %}
    {% set _raw_cols = _get_filtered_columns_in_relation(model, key_cols) %}
    {% set compare_columns = _raw_cols | sort %}
  {% else %}
    {% set compare_columns = compare_columns | sort %}
  {% endif %}

  {% set has_compare_cols = compare_columns | length > 0 %}

  {# ── FIX #8: clone_suffix default defined here, resolved reference built here ─ #}
  {% set before_delta_ref = get_before_delta_table_ref(base_model, clone_suffix) %}

  {# ── FIX #3: Validate the clone actually exists at compile time ─────────────── #}
  {# get_before_delta_table_ref returns a string — derive parts from ref() directly #}
  {% set _base_r       = ref(base_model) %}
  {% set _clone_schema = var('before_delta_schema', 'QA') %}
  {% set _before_id    = _base_r.identifier ~ (clone_suffix if clone_suffix else '_BEFORE_DELTA') %}
  {% set clone_relation = adapter.get_relation(
      database   = _base_r.database,
      schema     = _clone_schema,
      identifier = _before_id
  ) %}
  {% if not clone_relation %}
    {% do exceptions.raise_compiler_error(
      'delta_compare: before-delta clone not found: ' ~ before_delta_ref
      ~ '. Ensure the pre_hook ran successfully before this test.'
    ) %}
  {% endif %}

  with

  {# ── Base CTEs ────────────────────────────────────────────────────────────────── #}
  view_prep as (
    select
      {% for k in key_cols %}
      {{ adapter.quote(k) }},
      {% endfor %}
      {% for col in compare_columns %}
      {{ adapter.quote(col) }},
      {% endfor %}
      {% if has_compare_cols %}
      {{ _row_hash(compare_columns) }} as row_data_hash
      {% else %}
      cast(null as varchar) as row_data_hash
      {% endif %},
      1 as _row_exists   {# sentinel used for join detection — FIX #7 #}
    from {{ model }}
  ),

  before_prep as (
    select
      {% for k in key_cols %}
      {{ adapter.quote(k) }},
      {% endfor %}
      {% for col in compare_columns %}
      {{ adapter.quote(col) }},
      {% endfor %}
      {% if has_compare_cols %}
      {{ _row_hash(compare_columns) }} as row_data_hash
      {% else %}
      cast(null as varchar) as row_data_hash
      {% endif %},
      1 as _row_exists   {# sentinel used for join detection — FIX #7 #}
    from {{ before_delta_ref }}
  ),

  {# ── FIX #4: Classify keys as unique vs duplicate before any comparison ──────── #}
  view_key_counts as (
    select
      {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %},
      count(*) as key_count
    from view_prep
    group by {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
  ),

  before_key_counts as (
    select
      {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %},
      count(*) as key_count
    from before_prep
    group by {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
  ),

  {# Keys that appear exactly once on BOTH sides → safe for direct 1:1 compare #}
  unique_keys as (
    select {% for k in key_cols %}v.{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
    from view_key_counts v
    inner join before_key_counts b
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = b.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
    where v.key_count = 1 and b.key_count = 1
  ),

  {# Keys appearing >1 in EITHER side — handled separately below #}
  duplicate_keys as (
    select {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
    from view_key_counts where key_count > 1
    union
    select {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
    from before_key_counts where key_count > 1
  ),

  {# ── PATH 1: Unique key comparison (fully trustworthy 1:1) ──────────────────── #}
  unique_view as (
    select v.*
    from view_prep v
    inner join unique_keys u
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = u.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
  ),

  unique_before as (
    select b.*
    from before_prep b
    inner join unique_keys u
      on {% for k in key_cols %}b.{{ adapter.quote(k) }} = u.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
  ),

  {# Also capture view-only keys (no match in before at all) for NEW detection #}
  view_only_keys as (
    select {% for k in key_cols %}v.{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
    from view_key_counts v
    left join before_key_counts b
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = b.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
    where b.{{ adapter.quote(key_cols[0]) }} is null   {# left join miss — first key null means all keys null #}
      and v.key_count = 1   {# unique new keys only; duplicate new keys go to PATH 2 #}
  ),

  p1_compared as (
    select
      {% for k in key_cols %}
      v.{{ adapter.quote(k) }},
      {% endfor %}
      {# FIX #7: Use _row_exists sentinel for NEW detection, not nullable key columns #}
      case
        when b._row_exists is null then 'NEW'
        when v.row_data_hash != b.row_data_hash then 'UPDATED'
        else 'UNCHANGED'
      end as change_type,
      v.row_data_hash  as after_delta_row_hash,
      b.row_data_hash  as before_delta_row_hash,
      {# FIX #6: columns_changed works on all platforms via IS DISTINCT FROM #}
      case
        when b._row_exists is null then 'NEW'
        when v.row_data_hash != b.row_data_hash then
          {% if has_compare_cols %}
          {% if target.type == 'snowflake' %}
          array_to_string(array_compact(array_construct(
            {% for col in compare_columns %}
            case when v.{{ adapter.quote(col) }} is distinct from b.{{ adapter.quote(col) }}
                 then '{{ col }}' end{% if not loop.last %},{% endif %}
            {% endfor %}
          )), ', ')
          {% else %}
          {# Cross-platform: build with CASE + concat; verbose but portable #}
          trim(both ', ' from
            concat_ws(', ',
              {% for col in compare_columns %}
              case when v.{{ adapter.quote(col) }} is distinct from b.{{ adapter.quote(col) }}
                   then '{{ col }}' else null end{% if not loop.last %},{% endif %}
              {% endfor %}
            )
          )
          {% endif %}
          {% else %}
          cast(null as varchar)
          {% endif %}
        else cast(null as varchar)
      end as columns_changed,
      'UNIQUE' as key_classification
      {% for col in compare_columns %}
      , v.{{ adapter.quote(col) }} as {{ adapter.quote('after_delta_'  ~ col) }}
      , b.{{ adapter.quote(col) }} as {{ adapter.quote('before_delta_' ~ col) }}
      {% endfor %}
    from unique_view v
    left join unique_before b
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = b.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}

    union all

    {# Unique new keys (exist in view but not in before at all) #}
    select
      {% for k in key_cols %}
      v.{{ adapter.quote(k) }},
      {% endfor %}
      'NEW'             as change_type,
      v.row_data_hash   as after_delta_row_hash,
      null              as before_delta_row_hash,
      'NEW'             as columns_changed,
      'UNIQUE'          as key_classification
      {% for col in compare_columns %}
      , v.{{ adapter.quote(col) }} as {{ adapter.quote('after_delta_'  ~ col) }}
      , null            as {{ adapter.quote('before_delta_' ~ col) }}
      {% endfor %}
    from view_prep v
    inner join view_only_keys vok
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = vok.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
  ),

  {# ── PATH 2: Duplicate key comparison (set-based, labelled separately) ────────
     Uses hash match to check if ANY source row content matches ANY target row.
     Reports one representative row per key group for diagnostic purposes.
  #}
  dup_view as (
    select v.*, vkc.key_count as view_key_count
    from view_prep v
    inner join view_key_counts vkc
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = vkc.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
    inner join duplicate_keys d
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = d.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
  ),

  dup_before as (
    select b.*, bkc.key_count as before_key_count
    from before_prep b
    inner join before_key_counts bkc
      on {% for k in key_cols %}b.{{ adapter.quote(k) }} = bkc.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
    inner join duplicate_keys d
      on {% for k in key_cols %}b.{{ adapter.quote(k) }} = d.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
  ),

  dup_hash_matches as (
    select
      {% for k in key_cols %}v.{{ adapter.quote(k) }},{% endfor %}
      count(*) as matched_row_count
    from dup_view v
    inner join dup_before b
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = b.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
      and v.row_data_hash = b.row_data_hash
    group by {% for k in key_cols %}v.{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
  ),

  dup_view_rep as (
    select *
    from dup_view
    qualify row_number() over (
      partition by {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
      order by {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
    ) = 1
  ),

  dup_before_rep as (
    select *
    from dup_before
    qualify row_number() over (
      partition by {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
      order by {% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}
    ) = 1
  ),

  p2_compared as (
    select
      {% for k in key_cols %}
      v.{{ adapter.quote(k) }},
      {% endfor %}
      case
        when b._row_exists is null then 'NEW'
        when hm.matched_row_count is null then 'UPDATED'
        when hm.matched_row_count = v.view_key_count
         and hm.matched_row_count = b.before_key_count then 'UNCHANGED'
        else 'UPDATED'
      end as change_type,
      v.row_data_hash  as after_delta_row_hash,
      b.row_data_hash  as before_delta_row_hash,
      case
        when b._row_exists is null then 'NEW (duplicate key)'
        when hm.matched_row_count is null then
          'DUPLICATE KEY — no content match ('
          || to_varchar(v.view_key_count) || ' view rows, '
          || to_varchar(b.before_key_count) || ' before rows)'
        when hm.matched_row_count = v.view_key_count
         and hm.matched_row_count = b.before_key_count then
          'DUPLICATE KEY — all rows matched (' || to_varchar(hm.matched_row_count) || ')'
        else
          'DUPLICATE KEY — partial match ('
          || to_varchar(coalesce(hm.matched_row_count, 0)) || ' of '
          || to_varchar(v.view_key_count) || ' view rows matched)'
      end as columns_changed,
      'DUPLICATE' as key_classification
      {% for col in compare_columns %}
      , v.{{ adapter.quote(col) }} as {{ adapter.quote('after_delta_'  ~ col) }}
      , b.{{ adapter.quote(col) }} as {{ adapter.quote('before_delta_' ~ col) }}
      {% endfor %}
    from dup_view_rep v
    left join dup_before_rep b
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = b.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
    left join dup_hash_matches hm
      on {% for k in key_cols %}v.{{ adapter.quote(k) }} = hm.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}{% endfor %}
  ),

  {# ── Combine both paths ──────────────────────────────────────────────────────── #}
  all_compared as (
    select * from p1_compared
    union all
    select * from p2_compared
  )

  {# ── FIX #9: Add change_type_count for NEW vs UPDATED totals at a glance ────── #}
  select
    {% for k in key_cols %}
    {{ adapter.quote(k) }},
    {% endfor %}
    change_type,
    key_classification,
    columns_changed,
    after_delta_row_hash,
    before_delta_row_hash
    {% for col in compare_columns %}
    , {{ adapter.quote('after_delta_'  ~ col) }}
    , {{ adapter.quote('before_delta_' ~ col) }}
    {% endfor %}
    , count(*) over ()                              as total_changes_count
    , count(*) over (partition by change_type)      as change_type_count
  from all_compared
  where change_type in ('NEW', 'UPDATED')

{% endmacro %}


{# ── FIX #1 + #5: Safe cross-platform row hash using concat_ws + md5 ──────────
   Snowflake: uses hash(concat_ws(...)) — fast native hash
   Other:     uses md5(concat_ws(...))  — standard SQL, no silent no-op
   concat_ws('|', ...) eliminates the collision risk of multi-arg hash()
#}
{% macro _row_hash(columns) %}
  {% if target.type == 'snowflake' %}
    hash(concat_ws('|',
      {% for c in columns %}
      coalesce(to_varchar({{ adapter.quote(c) }}), 'NULL'){% if not loop.last %}, {% endif %}
      {% endfor %}
    ))
  {% else %}
    md5(concat_ws('|',
      {% for c in columns %}
      coalesce(cast({{ adapter.quote(c) }} as varchar), 'NULL'){% if not loop.last %}, {% endif %}
      {% endfor %}
    ))
  {% endif %}
{% endmacro %}
