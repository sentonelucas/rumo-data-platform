-- macros/generate_schema_name.sql
-- Sobrescreve o comportamento padrão do dbt para geração de schema.
-- Em produção: usa o schema definido no dbt_project.yml (não prefixado).
-- Em dev/CI: usa schema prefixado pelo usuário para isolamento.
--
-- Exemplo:
--   prod target + schema: marts   → schema: marts
--   dev  target + schema: marts   → schema: dbt_lucas_marts

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema | trim }}
        {%- endif -%}

    {%- else -%}
        {%- if custom_schema_name is not none -%}
            {{ default_schema | trim }}_{{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
