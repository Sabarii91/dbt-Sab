{#
  Fully qualified name of the pre-delta clone table (e.g. DB.QA.STG_CUSTOMERS_BEFORE_DELTA).
  Schema defaults to var('before_delta_schema', 'QA') — must match pre_hook/post_hook on staging models.
#}
{% macro get_before_delta_table_ref(base_model, clone_suffix=none) %}
  {% set _suffix = clone_suffix if clone_suffix is defined and clone_suffix else '_BEFORE_DELTA' %}
  {% set _r = ref(base_model) %}
  {% set _before_id = _r.identifier ~ _suffix %}
  {% set _clone_schema = var('before_delta_schema', 'QA') %}
  {{ (_r.database ~ '.' if _r.database else '') ~ _clone_schema ~ '.' ~ _before_id }}
{% endmacro %}
