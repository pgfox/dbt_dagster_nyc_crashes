{% macro replace_street_suffix(column_expr, seed_alias) %}
    case
        when {{ seed_alias }}.replacement is not null
        then left({{ column_expr }}, length({{ column_expr }}) - length({{ seed_alias }}.abbreviation))
             || {{ seed_alias }}.replacement
        else {{ column_expr }}
    end
{% endmacro %}