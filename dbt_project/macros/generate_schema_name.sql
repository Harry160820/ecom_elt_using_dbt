{#
  By default dbt names schemas <target_schema>_<custom_schema> (e.g. main_staging).
  We override that so a model configured with `+schema: staging` lands in `staging`.
  
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}