{% macro _comprehensive_compare_key_hash(key_cols) %}
  {% if target.type == 'snowflake' %}
    cast(hash({% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %}, {% endif %}{% endfor %}) as {{ type_string() }})
  {% elif target.type == 'oracle' %}
    {% if key_cols | length > 1 %}
      standard_hash({% for k in key_cols %}{{ adapter.quote(k) }}{% if not loop.last %} || '|' || {% endif %}{% endfor %}, 'MD5')
    {% else %}
      standard_hash({{ adapter.quote(key_cols[0]) }}, 'MD5')
    {% endif %}
  {% else %}
    {{ dbt.hash(dbt.concat(key_cols)) }}
  {% endif %}
{% endmacro %}