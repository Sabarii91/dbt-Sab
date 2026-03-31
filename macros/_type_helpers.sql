{% macro type_string() %}
  {% if target.type == 'snowflake' %}
    varchar
  {% elif target.type == 'oracle' %}
    varchar2
  {% else %}
    varchar
  {% endif %}
{% endmacro %}
