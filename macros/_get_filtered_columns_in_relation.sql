{#
  Replaces dbt_utils.get_filtered_columns_in_relation so we don't require the dbt_utils package.
  Returns list of column names from the relation, excluding those in the except list (case-insensitive).
#}
{% macro _get_filtered_columns_in_relation(relation, except_columns) %}
  {% set all_cols = adapter.get_columns_in_relation(relation) %}
  {% set except_upper = except_columns | map('upper') | list %}
  {% set ns = namespace(list=[]) %}
  {% for col in all_cols %}
    {% if col.name | upper not in except_upper %}
      {% set ns.list = ns.list + [col.name] %}
    {% endif %}
  {% endfor %}
  {{ return(ns.list) }}
{% endmacro %}
