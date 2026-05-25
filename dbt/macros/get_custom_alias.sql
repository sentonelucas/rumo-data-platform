-- macros/get_custom_alias.sql
-- Garante que o alias da tabela em prod é sempre o nome do model (sem prefixo de schema).
-- Em dev, mantém o comportamento padrão do dbt para evitar conflito de nomes.

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is not none -%}
        {{ custom_alias_name | trim }}
    {%- elif node.version is not none -%}
        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}

{%- endmacro %}
