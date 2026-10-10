{% macro array_length(expression) -%}
  {% if target.type == 'snowflake' %}array_size({{ expression }})
  {% else %}len({{ expression }}){% endif %}
{%- endmacro %}
