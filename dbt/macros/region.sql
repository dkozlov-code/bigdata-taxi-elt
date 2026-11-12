{% macro region(latitude, longitude) -%}
    concat(floor({{ latitude }} / 0.01)::integer, ':',
           floor({{ longitude }} / 0.012)::integer)
{%- endmacro %}
