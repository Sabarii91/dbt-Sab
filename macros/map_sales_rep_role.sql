{% macro map_sales_rep_role(column_name) %}

case
    when trim(upper({{ column_name }})) = 'SALES ASSOCIATE' then 'SA'
    when trim(upper({{ column_name }})) = 'ACCOUNT MANAGER' then 'AM'
    when trim(upper({{ column_name }})) = 'INSIDE SALES' then 'IS'
    else null
end

{% endmacro %}