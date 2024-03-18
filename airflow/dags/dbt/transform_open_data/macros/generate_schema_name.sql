{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if not target.name.startswith('testing_') and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}