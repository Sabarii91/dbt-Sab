{% macro _get_filtered_columns_in_relation(relation, except_columns=[]) %}

  {# Safety: ensure except_columns is always a list #}
  {% if except_columns is none %}
      {% set except_columns = [] %}
  {% endif %}

  {# Normalize exclusion list to uppercase for comparison #}
  {% set except_upper = except_columns | map('upper') | list %}

  {# Get columns safely #}
  {% set all_cols = adapter.get_columns_in_relation(relation) %}

  {# Initialize namespace #}
  {% set ns = namespace(filtered_cols=[]) %}

  {% for col in all_cols %}
      {% if col.name | upper not in except_upper %}
          {% set ns.filtered_cols = ns.filtered_cols + [col.name] %}
      {% endif %}
  {% endfor %}

  {{ return(ns.filtered_cols) }}

{% endmacro %}