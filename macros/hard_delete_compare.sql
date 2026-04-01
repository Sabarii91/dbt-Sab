{#
  Hard delete detection macro.

  Identifies records that existed in the BEFORE_DELTA clone but no longer exist
  in the current target table — i.e. were hard-deleted during the delta load.

  Comparison: BEFORE_DELTA clone  LEFT JOIN  current target ON key columns
              WHERE target key IS NULL  →  HARD_DELETED

  This macro is completely independent. It does not modify delta_compare,
  comprehensive_compare, or any other existing macro.

  Arguments:
    - model          : the current target model (after delta load), e.g. ref('stg_customers')
    - base_model     : name of the base model whose clone to read, e.g. 'stg_customers'
    - clone_suffix   : suffix of the BEFORE_DELTA clone (default '_BEFORE_DELTA')
    - key_columns    : required — single column name or list for composite join key
    - compare_columns: optional list of columns to include for before/after visibility.
                       If omitted, all non-key columns from the clone are used (sorted alphabetically).
#}

{% macro hard_delete_compare(
    model,
    base_model,
    clone_suffix='_BEFORE_DELTA',
    key_columns=none,
    compare_columns=none
) %}

  {# ── Guard: key_columns required ──────────────────────────────────────────── #}
  {% if not key_columns %}
    {% do exceptions.raise_compiler_error(
      'hard_delete_compare: key_columns is required. Pass a column name or list of column names.'
    ) %}
  {% endif %}

  {% set key_cols = [key_columns] if key_columns is string else key_columns %}

  {% if key_cols | length == 0 %}
    {% do exceptions.raise_compiler_error('hard_delete_compare: key_columns must not be empty') %}
  {% endif %}

  {# ── Resolve before_delta reference ───────────────────────────────────────── #}
  {% set before_delta_ref = get_before_delta_table_ref(base_model, clone_suffix) %}

  {# ── Validate clone exists at compile time ────────────────────────────────── #}
  {# get_before_delta_table_ref returns a string — derive parts from ref() directly #}
  {% set _base_r       = ref(base_model) %}
  {% set _clone_schema = var('before_delta_schema', 'QA') %}
  {% set _before_id    = _base_r.identifier ~ clone_suffix %}
  {% set clone_relation = adapter.get_relation(
      database   = _base_r.database,
      schema     = _clone_schema,
      identifier = _before_id
  ) %}
  {% if not clone_relation %}
    {% do exceptions.raise_compiler_error(
      'hard_delete_compare: before-delta clone not found: ' ~ before_delta_ref
      ~ '. Ensure the pre_hook ran successfully before this test.'
    ) %}
  {% endif %}

  {# ── Resolve compare_columns — sorted for stable output ───────────────────── #}
  {% if not compare_columns %}
    {% set compare_columns = _get_filtered_columns_in_relation(clone_relation, key_cols) | sort %}
  {% else %}
    {% set compare_columns = compare_columns | sort %}
  {% endif %}

  {% set has_compare_cols = compare_columns | length > 0 %}

  with

  -- ============================================================
  -- BEFORE DELTA: snapshot of target taken before the delta load
  -- ============================================================

  before_delta as (
    select
      {% for k in key_cols %}
      {{ adapter.quote(k) }},
      {% endfor %}
      {% for col in compare_columns %}
      {{ adapter.quote(col) }},
      {% endfor %}
      1 as _row_exists
    from {{ before_delta_ref }}
  ),

  -- ============================================================
  -- CURRENT TARGET: state of target after the delta load
  -- Only key columns needed — we only check existence
  -- ============================================================

  target_current as (
    select
      {% for k in key_cols %}
      {{ adapter.quote(k) }},
      {% endfor %}
      1 as _row_exists
    from {{ model }}
  ),

  -- ============================================================
  -- HARD DELETES: in clone but not in current target
  -- ============================================================

  hard_deletes as (
    select
      {% for k in key_cols %}
      b.{{ adapter.quote(k) }},
      {% endfor %}
      'HARD_DELETED' as change_type
      {% for col in compare_columns %}
      , b.{{ adapter.quote(col) }} as {{ adapter.quote('before_delta_' ~ col) }}
      , null                       as {{ adapter.quote('after_delta_'  ~ col) }}
      {% endfor %}
    from before_delta b
    left join target_current t
      on {% for k in key_cols %}
         b.{{ adapter.quote(k) }} = t.{{ adapter.quote(k) }}{% if not loop.last %} and {% endif %}
         {% endfor %}
    where t._row_exists is null
  )

  select
    {% for k in key_cols %}
    {{ adapter.quote(k) }},
    {% endfor %}
    change_type
    {% for col in compare_columns %}
    , {{ adapter.quote('before_delta_' ~ col) }}
    , {{ adapter.quote('after_delta_'  ~ col) }}
    {% endfor %}
    , count(*) over () as total_deletes_count

  from hard_deletes

{% endmacro %}
